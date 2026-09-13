use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU16,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_s3::{
    presigning::PresigningConfig,
    types::{CompletedMultipartUpload, CompletedPart},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{Stream, stream};

use crate::{
    Bucket, Error, IntoBucketName, IntoContentType, IntoObjectKey, ObjectUploadOptions,
    ValidationError, types,
};

// https://developers.cloudflare.com/r2/platform/limits/
const MAX_MULTIPART_OBJECT_SIZE: u64 = types::MAX_MULTIPART_OBJECT_SIZE;
const MAX_PARTS: u16 = 10_000;

/// A validated multipart part number in the range `1..=10_000`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartNumber(NonZeroU16);

impl PartNumber {
    /// Returns the numeric part number.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl TryFrom<u16> for PartNumber {
    type Error = Error;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        let provided = value;
        let value = NonZeroU16::new(value).ok_or(ValidationError::PartNumberOutOfRange {
            provided,
            min: 1,
            max: MAX_PARTS,
        })?;
        if value.get() > MAX_PARTS {
            return Err(ValidationError::PartNumberOutOfRange {
                provided,
                min: 1,
                max: MAX_PARTS,
            }
            .into());
        }
        Ok(Self(value))
    }
}

/// A successfully uploaded multipart part.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadedPart {
    part_number: PartNumber,
    etag: String,
}

/// A validated Base64-encoded 128-bit MD5 digest for one multipart part.
///
/// R2 checks this value against the request body when it is included in a
/// presigned `UploadPart` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartMd5(String);

impl PartMd5 {
    /// Returns the canonical Base64 representation expected by `Content-MD5`.
    #[must_use]
    pub fn as_base64(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PartMd5 {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let decoded = STANDARD.decode(&value).map_err(|_| Error::InvalidInput {
            field: "content_md5",
            reason: "must be canonical Base64 for a 128-bit MD5 digest",
        })?;
        if decoded.len() != 16 || STANDARD.encode(decoded) != value {
            return Err(Error::InvalidInput {
                field: "content_md5",
                reason: "must be canonical Base64 for a 128-bit MD5 digest",
            });
        }
        Ok(Self(value))
    }
}

impl TryFrom<&str> for PartMd5 {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_owned())
    }
}

/// Untrusted transport data reported by a direct multipart uploader.
///
/// Convert this value with [`MultipartPartReceipt::try_into_uploaded_part`]
/// before using it in a completion manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct MultipartPartReceipt {
    part_number: u16,
    etag: String,
}

impl MultipartPartReceipt {
    /// Creates a wire receipt from primitive values.
    #[must_use]
    pub fn new(part_number: u16, etag: impl Into<String>) -> Self {
        Self {
            part_number,
            etag: etag.into(),
        }
    }

    /// Returns the unvalidated numeric part number.
    #[must_use]
    pub const fn part_number(&self) -> u16 {
        self.part_number
    }

    /// Returns the exact ETag reported by R2.
    #[must_use]
    pub fn etag(&self) -> &str {
        &self.etag
    }

    /// Validates this untrusted receipt for use in a completion manifest.
    pub fn try_into_uploaded_part(self) -> Result<UploadedPart, Error> {
        UploadedPart::new(PartNumber::try_from(self.part_number)?, self.etag)
    }
}

impl UploadedPart {
    /// Creates a completion entry from the exact ETag returned by R2.
    pub fn new(part_number: PartNumber, etag: impl Into<String>) -> Result<Self, Error> {
        let etag = etag.into();
        if etag.is_empty() {
            return Err(Error::InvalidInput {
                field: "etag",
                reason: "must not be empty",
            });
        }
        Ok(Self { part_number, etag })
    }

    /// Returns the part number.
    #[must_use]
    pub const fn part_number(&self) -> PartNumber {
        self.part_number
    }

    /// Returns the exact R2 ETag, including quotes when R2 supplied them.
    #[must_use]
    pub fn etag(&self) -> &str {
        &self.etag
    }
}

/// A canonical, duplicate-free completion manifest sorted by part number.
#[derive(Clone, Debug)]
pub struct CompletionManifest(Vec<UploadedPart>);

impl CompletionManifest {
    /// Validates and canonicalizes uploaded parts.
    pub fn try_from_parts(parts: impl IntoIterator<Item = UploadedPart>) -> Result<Self, Error> {
        let mut canonical = BTreeMap::new();
        for part in parts {
            let number = part.part_number();
            if canonical.insert(number, part).is_some() {
                return Err(Error::InvalidInput {
                    field: "parts",
                    reason: "contains a duplicate part number",
                });
            }
        }
        if canonical.is_empty() {
            return Err(Error::InvalidInput {
                field: "parts",
                reason: "must contain at least one uploaded part",
            });
        }
        Ok(Self(canonical.into_values().collect()))
    }

    /// Validates untrusted uploader receipts and builds a canonical manifest.
    pub fn try_from_receipts(
        receipts: impl IntoIterator<Item = MultipartPartReceipt>,
    ) -> Result<Self, Error> {
        receipts
            .into_iter()
            .map(MultipartPartReceipt::try_into_uploaded_part)
            .collect::<Result<Vec<_>, _>>()
            .and_then(Self::try_from_parts)
    }

    /// Iterates over canonical parts in ascending part-number order.
    pub fn parts(&self) -> impl ExactSizeIterator<Item = &UploadedPart> {
        self.0.iter()
    }
}

/// A presigned URL treated as a bearer credential.
#[derive(Clone)]
pub struct SecretUrl(String);

impl SecretUrl {
    /// Deliberately exposes the bearer URL for transmission to an uploader.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Deliberately consumes and exposes the bearer URL.
    #[must_use]
    pub fn into_exposed_string(self) -> String {
        self.0
    }
}

impl fmt::Debug for SecretUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretUrl([REDACTED PRESIGNED URL])")
    }
}

/// An HTTP request signed for temporary direct access to R2.
#[derive(Clone)]
pub struct PresignedRequest {
    method: String,
    url: SecretUrl,
    required_headers: Vec<(String, String)>,
    expires_at: SystemTime,
}

impl PresignedRequest {
    pub(crate) fn from_sdk(
        signed: aws_sdk_s3::presigning::PresignedRequest,
        expires_in: Duration,
    ) -> Result<Self, Error> {
        let required_headers = signed
            .headers()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        Ok(Self {
            method: signed.method().to_owned(),
            url: SecretUrl(signed.uri().to_string()),
            required_headers,
            expires_at: SystemTime::now() + expires_in,
        })
    }

    /// Returns the HTTP method the uploader must use.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Returns the redacted-by-default bearer URL wrapper.
    #[must_use]
    pub fn url(&self) -> &SecretUrl {
        &self.url
    }

    /// Returns headers that must be replayed exactly by the uploader.
    pub fn required_headers(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.required_headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Returns the approximate expiration instant.
    #[must_use]
    pub const fn expires_at(&self) -> SystemTime {
        self.expires_at
    }

    /// Deliberately exposes all request components for an HTTP client.
    #[must_use]
    pub fn into_exposed_parts(self) -> (String, String, Vec<(String, String)>) {
        (
            self.method,
            self.url.into_exposed_string(),
            self.required_headers,
        )
    }

    /// Returns the signed URL as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.url.expose()
    }

    /// Consumes this value and returns the signed URL as an owned string.
    #[must_use]
    pub fn into_url_string(self) -> String {
        self.url.into_exposed_string()
    }
}

impl fmt::Debug for PresignedRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names: Vec<&str> = self
            .required_headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        f.debug_struct("PresignedRequest")
            .field("method", &self.method)
            .field("url", &"[REDACTED PRESIGNED URL]")
            .field("required_header_names", &header_names)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// A presigned upload request for one known multipart part.
#[derive(Clone, Debug)]
pub struct PresignedUploadPart {
    part_number: PartNumber,
    content_length: u64,
    content_md5: Option<PartMd5>,
    request: PresignedRequest,
}

impl PresignedUploadPart {
    /// Returns the part number.
    #[must_use]
    pub const fn part_number(&self) -> PartNumber {
        self.part_number
    }

    /// Returns the exact number of bytes expected for this part.
    #[must_use]
    pub const fn content_length(&self) -> u64 {
        self.content_length
    }

    /// Returns the body checksum enforced by R2, when one was signed.
    #[must_use]
    pub fn content_md5(&self) -> Option<&PartMd5> {
        self.content_md5.as_ref()
    }

    /// Returns the signed request.
    #[must_use]
    pub fn request(&self) -> &PresignedRequest {
        &self.request
    }

    /// Consumes the part wrapper and returns the signed request.
    #[must_use]
    pub fn into_request(self) -> PresignedRequest {
        self.request
    }

    /// Deliberately exposes the bearer request as a serializable protocol DTO.
    ///
    /// The resulting value still redacts its URL and header values from
    /// `Debug`, but serialization exposes them for transport to an uploader.
    pub fn into_protocol_request(self) -> Result<MultipartUploadPartRequest, Error> {
        let expires_at_unix_seconds = self
            .request
            .expires_at()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Presign)?
            .as_secs();
        let (method, url, required_headers) = self.request.into_exposed_parts();
        Ok(MultipartUploadPartRequest {
            part_number: self.part_number.get(),
            content_length: self.content_length,
            content_md5: self.content_md5.map(|value| value.0),
            method,
            url,
            required_headers,
            expires_at_unix_seconds,
        })
    }
}

/// Serializable protocol DTO sent from a trusted signer to an uploader.
///
/// This value contains a bearer URL. Serialization is therefore an explicit
/// secret-exposure boundary even though `Debug` remains redacted. Its primitive
/// fields are a transport representation of an already validated and signed
/// request; deserializing this DTO does not establish a new trusted validation
/// boundary.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct MultipartUploadPartRequest {
    part_number: u16,
    content_length: u64,
    content_md5: Option<String>,
    method: String,
    url: String,
    required_headers: Vec<(String, String)>,
    expires_at_unix_seconds: u64,
}

impl MultipartUploadPartRequest {
    /// Returns the part number this request may upload.
    #[must_use]
    pub const fn part_number(&self) -> u16 {
        self.part_number
    }

    /// Returns the exact required request-body length.
    #[must_use]
    pub const fn content_length(&self) -> u64 {
        self.content_length
    }

    /// Returns the signed Base64 `Content-MD5`, when enabled.
    #[must_use]
    pub fn content_md5(&self) -> Option<&str> {
        self.content_md5.as_deref()
    }

    /// Returns the signed HTTP method.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Deliberately exposes the bearer URL to the uploader.
    #[must_use]
    pub fn expose_url(&self) -> &str {
        &self.url
    }

    /// Returns headers that the uploader must replay exactly.
    pub fn required_headers(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.required_headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Returns the approximate Unix expiration timestamp in seconds.
    #[must_use]
    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }
}

impl fmt::Debug for MultipartUploadPartRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names: Vec<&str> = self
            .required_headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        f.debug_struct("MultipartUploadPartRequest")
            .field("part_number", &self.part_number)
            .field("content_length", &self.content_length)
            .field("content_md5", &self.content_md5)
            .field("method", &self.method)
            .field("url", &"[REDACTED PRESIGNED URL]")
            .field("required_header_names", &header_names)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// Persistable state needed to resume a presigned multipart upload.
#[derive(Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MultipartSessionSnapshot {
    bucket: String,
    key: String,
    upload_id: String,
    file_size: u64,
    part_size: u64,
}

/// Versioned persistence DTO for resuming a multipart session.
///
/// This value contains an upload ID. Serialization deliberately exposes that
/// credential to the selected persistence layer, while `Debug` redacts it.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct MultipartSessionRecord {
    version: u8,
    bucket: String,
    key: String,
    upload_id: String,
    file_size: u64,
    part_size: u64,
}

impl MultipartSessionRecord {
    /// Returns the persistence schema version.
    #[must_use]
    pub const fn version(&self) -> u8 {
        self.version
    }

    /// Returns the bucket stored in this untrusted record.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the key stored in this untrusted record.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Deliberately exposes the persisted upload ID.
    #[must_use]
    pub fn expose_upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Returns the planned object size.
    #[must_use]
    pub const fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Returns the planned part size.
    #[must_use]
    pub const fn part_size(&self) -> u64 {
        self.part_size
    }
}

impl fmt::Debug for MultipartSessionRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartSessionRecord")
            .field("version", &self.version)
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("upload_id", &"[REDACTED]")
            .field("file_size", &self.file_size)
            .field("part_size", &self.part_size)
            .finish()
    }
}

impl MultipartSessionSnapshot {
    /// Restores validated multipart session state previously returned by [`PresignedMultipart::snapshot`].
    pub fn restore(
        bucket: impl IntoBucketName,
        key: impl IntoObjectKey,
        upload_id: impl Into<String>,
        file_size: u64,
        part_size: u64,
    ) -> Result<Self, Error> {
        let bucket = bucket.into_bucket_name()?;
        let key = key.into_object_key()?;
        MultipartPlan::new(file_size, part_size)?;
        let upload_id = upload_id.into();
        if upload_id.is_empty() {
            return Err(Error::InvalidInput {
                field: "upload_id",
                reason: "must not be empty",
            });
        }
        Ok(Self {
            bucket: bucket.into_inner(),
            key: key.into_inner(),
            upload_id,
            file_size,
            part_size,
        })
    }

    /// Validates a deserialized persistence record before it becomes session state.
    pub fn from_persistence_record(record: MultipartSessionRecord) -> Result<Self, Error> {
        if record.version != 1 {
            return Err(Error::InvalidInput {
                field: "version",
                reason: "unsupported multipart session record version",
            });
        }
        Self::restore(
            record.bucket,
            record.key,
            record.upload_id,
            record.file_size,
            record.part_size,
        )
    }

    /// Deliberately exposes resumable state as a versioned persistence DTO.
    #[must_use]
    pub fn into_persistence_record(self) -> MultipartSessionRecord {
        MultipartSessionRecord {
            version: 1,
            bucket: self.bucket,
            key: self.key,
            upload_id: self.upload_id,
            file_size: self.file_size,
            part_size: self.part_size,
        }
    }

    /// Deliberately exposes the upload ID for persistence.
    #[must_use]
    pub fn expose_upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Returns the bucket owning this upload.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the destination object key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the complete object size in bytes.
    #[must_use]
    pub const fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Returns the uniform non-final part size in bytes.
    #[must_use]
    pub const fn part_size(&self) -> u64 {
        self.part_size
    }
}

impl fmt::Debug for MultipartSessionSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartSessionSnapshot")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("upload_id", &"[REDACTED]")
            .field("file_size", &self.file_size)
            .field("part_size", &self.part_size)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MultipartPlan {
    file_size: u64,
    part_size: u64,
    part_count: u16,
}

impl MultipartPlan {
    pub(crate) fn new(file_size: u64, part_size: u64) -> Result<Self, Error> {
        if file_size == 0 {
            return Err(ValidationError::MultipartFileSizeZero.into());
        }
        if file_size > MAX_MULTIPART_OBJECT_SIZE {
            return Err(ValidationError::MultipartObjectTooLarge {
                provided: file_size,
                max: MAX_MULTIPART_OBJECT_SIZE,
            }
            .into());
        }
        types::validate_part_size(part_size)?;
        let part_count = file_size.div_ceil(part_size);
        if part_count > u64::from(MAX_PARTS) {
            return Err(ValidationError::TooManyParts {
                required: part_count,
                max: MAX_PARTS,
            }
            .into());
        }
        Ok(Self {
            file_size,
            part_size,
            part_count: part_count as u16,
        })
    }

    pub(crate) fn part_length(&self, number: PartNumber) -> Result<u64, Error> {
        if number.get() > self.part_count {
            return Err(Error::InvalidInput {
                field: "part_number",
                reason: "exceeds this upload's planned part count",
            });
        }
        let start = u64::from(number.get() - 1) * self.part_size;
        Ok((self.file_size - start).min(self.part_size))
    }
}

/// Server-observed state of a live multipart upload.
#[derive(Clone, Debug)]
pub struct MultipartReconciliation {
    uploaded_parts: Vec<UploadedPart>,
    missing_parts: Vec<PartNumber>,
}

impl MultipartReconciliation {
    /// Returns uploaded parts in ascending part-number order.
    pub fn uploaded_parts(&self) -> impl ExactSizeIterator<Item = &UploadedPart> {
        self.uploaded_parts.iter()
    }

    /// Consumes the reconciliation and returns uploaded parts in ascending
    /// part-number order.
    #[must_use]
    pub fn into_uploaded_parts(self) -> Vec<UploadedPart> {
        self.uploaded_parts
    }

    /// Returns planned parts that R2 has not received yet.
    pub fn missing_parts(&self) -> impl ExactSizeIterator<Item = PartNumber> + '_ {
        self.missing_parts.iter().copied()
    }

    /// Returns whether R2 has every planned part with the expected size.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_parts.is_empty()
    }

    /// Builds a canonical completion manifest when every part is present.
    pub fn into_completion_manifest(self) -> Result<CompletionManifest, Error> {
        if !self.missing_parts.is_empty() {
            return Err(Error::InvalidInput {
                field: "remote_parts",
                reason: "multipart upload is missing planned parts",
            });
        }
        CompletionManifest::try_from_parts(self.uploaded_parts)
    }
}

/// Builder for a new presigned multipart upload session.
#[derive(Clone, Debug)]
pub struct PresignedMultipartBuilder {
    bucket: Bucket,
    key: String,
    file_size: Option<u64>,
    part_size: Option<u64>,
    options: ObjectUploadOptions,
}

impl PresignedMultipartBuilder {
    /// Sets typed metadata stored on the completed object.
    #[must_use]
    pub fn upload_options(mut self, options: ObjectUploadOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets the completed object's MIME media type.
    #[must_use]
    pub fn content_type(mut self, value: impl IntoContentType) -> Self {
        self.options = self.options.with_content_type(value);
        self
    }

    /// Sets the completed object's typed HTTP cache policy.
    #[must_use]
    pub fn cache_control(mut self, value: headers::CacheControl) -> Self {
        self.options = self.options.with_cache_control(value);
        self
    }

    /// Sets the completed object's content disposition.
    #[must_use]
    pub fn content_disposition(mut self, value: impl Into<String>) -> Self {
        self.options = self.options.with_content_disposition(value);
        self
    }

    /// Sets the completed object's content encoding.
    #[must_use]
    pub fn content_encoding(mut self, value: impl Into<String>) -> Self {
        self.options = self.options.with_content_encoding(value);
        self
    }

    /// Sets the completed object's content language.
    #[must_use]
    pub fn content_language(mut self, value: impl Into<String>) -> Self {
        self.options = self.options.with_content_language(value);
        self
    }

    /// Sets the completed object's HTTP expiration time.
    #[must_use]
    pub fn expires(mut self, value: SystemTime) -> Self {
        self.options = self.options.with_expires(value);
        self
    }

    /// Adds user-defined metadata to the completed object.
    #[must_use]
    pub fn custom_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options = self.options.with_custom_metadata(key, value);
        self
    }

    /// Sets the complete object size in bytes.
    ///
    /// This is required because r2kit plans every part before creating the
    /// remote upload: it determines the final part length, enforces R2's object
    /// and 10,000-part limits, signs exact content lengths, and later verifies
    /// the remote parts. For browser uploads, send the browser's `File.size`
    /// to the trusted server and treat it as untrusted input; r2kit validates
    /// the value before network I/O.
    ///
    /// Managed local-file uploads do not require this setting because
    /// [`crate::ManagedMultipartBuilder::upload_file`] reads file metadata.
    #[must_use]
    pub const fn file_size(mut self, bytes: u64) -> Self {
        self.file_size = Some(bytes);
        self
    }

    /// Sets the uniform multipart part size in bytes.
    #[must_use]
    pub const fn part_size(mut self, bytes: u64) -> Self {
        self.part_size = Some(bytes);
        self
    }

    /// Sets the uniform multipart part size in mebibytes (MiB).
    ///
    /// This is equivalent to [`Self::part_size`] with a binary-unit conversion
    /// and avoids repeating `1024 * 1024` at call sites. Values that cannot be
    /// represented as bytes are rejected by [`Self::create`].
    #[must_use]
    pub const fn part_size_mib(mut self, mebibytes: u64) -> Self {
        self.part_size = Some(types::mebibytes(mebibytes));
        self
    }

    /// Creates the remote multipart upload after local validation succeeds.
    pub async fn create(self) -> Result<PresignedMultipart, Error> {
        self.options.validate()?;
        let plan = MultipartPlan::new(
            self.file_size.ok_or(Error::InvalidInput {
                field: "file_size",
                reason: "is required",
            })?,
            self.part_size.ok_or(Error::InvalidInput {
                field: "part_size",
                reason: "is required",
            })?,
        )?;
        self.options.validate()?;
        if self.options.checksum().is_some() {
            return Err(Error::InvalidInput {
                field: "checksum",
                reason: "checksum verification is not supported on multipart upload sessions; verify individual parts using MD5 or SHA",
            });
        }
        if self.options.if_match().is_some() || self.options.if_none_match().is_some() {
            return Err(Error::InvalidInput {
                field: "if_match",
                reason: "conditional match headers are not supported on multipart upload sessions",
            });
        }
        let req = self
            .bucket
            .client
            .as_sdk()
            .create_multipart_upload()
            .bucket(self.bucket.name.as_str())
            .key(&self.key);
        let req = self
            .options
            .apply_to(req)
            .set_metadata(self.options.custom_metadata_values());
        let output = req
            .send()
            .await
            .map_err(|error| Error::remote("CreateMultipartUpload", &error))?;
        let upload_id = output.upload_id().ok_or(Error::Service {
            operation: "CreateMultipartUpload",
        })?;

        Ok(PresignedMultipart {
            bucket: self.bucket,
            key: self.key,
            upload_id: upload_id.to_owned(),
            plan,
        })
    }
}

/// An active R2 multipart upload that can presign parts, complete, or abort.
#[derive(Clone)]
pub struct PresignedMultipart {
    bucket: Bucket,
    key: String,
    upload_id: String,
    plan: MultipartPlan,
}

impl PresignedMultipart {
    /// Returns the upload ID for this multipart session.
    #[must_use]
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Returns the number of planned parts.
    #[must_use]
    pub const fn part_count(&self) -> u16 {
        self.plan.part_count
    }

    /// Returns the expected byte length for a part.
    pub fn part_length(&self, number: PartNumber) -> Result<u64, Error> {
        self.plan.part_length(number)
    }

    /// Creates a temporary signed PUT request for one part.
    pub async fn presign_part(
        &self,
        number: PartNumber,
        expires_in: Duration,
    ) -> Result<PresignedUploadPart, Error> {
        self.presign_part_inner(number, None, expires_in).await
    }

    /// Creates a signed PUT request that makes R2 verify `Content-MD5`.
    ///
    /// The uploader must replay the returned `content-md5` header exactly.
    pub async fn presign_part_with_md5(
        &self,
        number: PartNumber,
        content_md5: PartMd5,
        expires_in: Duration,
    ) -> Result<PresignedUploadPart, Error> {
        self.presign_part_inner(number, Some(content_md5), expires_in)
            .await
    }

    async fn presign_part_inner(
        &self,
        number: PartNumber,
        content_md5: Option<PartMd5>,
        expires_in: Duration,
    ) -> Result<PresignedUploadPart, Error> {
        let content_length = self.plan.part_length(number)?;
        types::validate_expiry(expires_in)?;
        let config = PresigningConfig::expires_in(expires_in).map_err(|_| Error::Presign)?;
        let request = self
            .bucket
            .client
            .as_sdk()
            .upload_part()
            .bucket(self.bucket.name.as_str())
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(i32::from(number.get()));
        let request = match &content_md5 {
            Some(value) => request.content_md5(value.as_base64()),
            None => request,
        };
        let signed = request
            .presigned(config)
            .await
            .map_err(|_| Error::Presign)?;

        Ok(PresignedUploadPart {
            part_number: number,
            content_length,
            content_md5,
            request: PresignedRequest::from_sdk(signed, expires_in)?,
        })
    }

    /// Reconciles persisted client state with the parts currently stored by R2.
    ///
    /// Every remote part is checked for a valid number, a unique entry, and
    /// the exact size required by the original upload plan.
    pub async fn reconcile(&self) -> Result<MultipartReconciliation, Error> {
        let mut marker = None;
        let mut uploaded: Vec<Option<UploadedPart>> = std::iter::repeat_with(|| None)
            .take(usize::from(self.plan.part_count))
            .collect();
        loop {
            let output = self
                .bucket
                .client
                .as_sdk()
                .list_parts()
                .bucket(self.bucket.name.as_str())
                .key(&self.key)
                .upload_id(&self.upload_id)
                .max_parts(1_000)
                .set_part_number_marker(marker)
                .send()
                .await
                .map_err(|error| Error::remote("ListParts", &error))?;
            for remote in output.parts() {
                let raw_number = remote.part_number().ok_or(Error::Service {
                    operation: "ListParts",
                })?;
                let number = u16::try_from(raw_number)
                    .ok()
                    .and_then(|value| PartNumber::try_from(value).ok())
                    .ok_or(Error::InvalidInput {
                        field: "remote_parts",
                        reason: "contains an invalid part number",
                    })?;
                let expected_size =
                    self.plan
                        .part_length(number)
                        .map_err(|_| Error::InvalidInput {
                            field: "remote_parts",
                            reason: "contains a part outside the upload plan",
                        })?;
                if remote.size().and_then(|size| u64::try_from(size).ok()) != Some(expected_size) {
                    return Err(Error::InvalidInput {
                        field: "remote_parts",
                        reason: "contains a part with an unexpected size",
                    });
                }
                let etag = remote.e_tag().ok_or(Error::Service {
                    operation: "ListParts",
                })?;
                let part = UploadedPart::new(number, etag)?;
                let slot = &mut uploaded[usize::from(number.get() - 1)];
                if slot.replace(part).is_some() {
                    return Err(Error::InvalidInput {
                        field: "remote_parts",
                        reason: "contains a duplicate part number",
                    });
                }
            }
            if output.is_truncated() != Some(true) {
                break;
            }
            marker = output.next_part_number_marker;
            if marker.is_none() {
                return Err(Error::Service {
                    operation: "ListParts",
                });
            }
        }

        let mut uploaded_parts = Vec::with_capacity(uploaded.len());
        let mut missing_parts = Vec::new();
        for (index, part) in uploaded.into_iter().enumerate() {
            match part {
                Some(part) => uploaded_parts.push(part),
                None => missing_parts.push(
                    PartNumber::try_from(index as u16 + 1).expect("planned part numbers are valid"),
                ),
            }
        }
        Ok(MultipartReconciliation {
            uploaded_parts,
            missing_parts,
        })
    }

    /// Verifies client receipts against R2 before completing the upload.
    ///
    /// This extra `ListParts` round trip rejects stale, missing, incorrectly
    /// sized, or mismatched part receipts before `CompleteMultipartUpload`.
    pub async fn complete_verified(
        &self,
        manifest: CompletionManifest,
    ) -> Result<CompletedObject, Error> {
        let reconciliation = self.reconcile().await?;
        if !reconciliation.is_complete()
            || reconciliation.uploaded_parts.as_slice() != manifest.0.as_slice()
        {
            return Err(Error::InvalidInput {
                field: "parts",
                reason: "does not match the parts currently stored by R2",
            });
        }
        self.complete(manifest).await
    }

    /// Completes the upload with the exact ETags returned by every uploaded part.
    pub async fn complete(&self, manifest: CompletionManifest) -> Result<CompletedObject, Error> {
        if manifest.0.len() != usize::from(self.plan.part_count)
            || manifest
                .0
                .iter()
                .enumerate()
                .any(|(index, part)| usize::from(part.part_number().get()) != index + 1)
        {
            return Err(Error::InvalidInput {
                field: "parts",
                reason: "must include every planned part exactly once",
            });
        }

        let parts = manifest
            .0
            .into_iter()
            .map(|part| {
                CompletedPart::builder()
                    .part_number(i32::from(part.part_number.get()))
                    .e_tag(part.etag)
                    .build()
            })
            .collect();
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        let output = self
            .bucket
            .client
            .as_sdk()
            .complete_multipart_upload()
            .bucket(self.bucket.name.as_str())
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map_err(|error| Error::remote("CompleteMultipartUpload", &error))?;

        Ok(CompletedObject {
            etag: output.e_tag().map(ToOwned::to_owned),
        })
    }

    /// Aborts the remote multipart upload.
    pub async fn abort(&self) -> Result<(), Error> {
        self.bucket
            .client
            .as_sdk()
            .abort_multipart_upload()
            .bucket(self.bucket.name.as_str())
            .key(&self.key)
            .upload_id(&self.upload_id)
            .send()
            .await
            .map_err(|error| Error::remote("AbortMultipartUpload", &error))?;
        Ok(())
    }

    /// Captures resumable session state. Its debug output redacts the upload ID.
    #[must_use]
    pub fn snapshot(&self) -> MultipartSessionSnapshot {
        MultipartSessionSnapshot {
            bucket: self.bucket.name.to_string(),
            key: self.key.clone(),
            upload_id: self.upload_id.clone(),
            file_size: self.plan.file_size,
            part_size: self.plan.part_size,
        }
    }
}

impl fmt::Debug for PresignedMultipart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PresignedMultipart")
            .field("bucket", &self.bucket.name)
            .field("key", &self.key)
            .field("upload_id", &"[REDACTED]")
            .field("plan", &self.plan)
            .finish()
    }
}

/// An initiated presigned multipart upload plan.
///
/// Wraps an active multipart session and provides convenient access to session state,
/// upload ID, part counts, and individual part presigning methods.
#[derive(Clone)]
pub struct PresignedMultipartPlan {
    session: PresignedMultipart,
}

impl PresignedMultipartPlan {
    /// Creates a new presigned multipart plan from an active session.
    #[must_use]
    pub fn from_session(session: PresignedMultipart) -> Self {
        Self { session }
    }

    /// Returns a reference to the active multipart session.
    #[must_use]
    pub fn session(&self) -> &PresignedMultipart {
        &self.session
    }

    /// Consumes the plan and returns the underlying multipart session.
    #[must_use]
    pub fn into_session(self) -> PresignedMultipart {
        self.session
    }

    /// Returns the upload ID for this multipart session.
    #[must_use]
    pub fn upload_id(&self) -> &str {
        &self.session.upload_id
    }

    /// Returns the number of planned parts.
    #[must_use]
    pub const fn part_count(&self) -> u16 {
        self.session.part_count()
    }

    /// Returns the planned part size in bytes.
    #[must_use]
    pub const fn part_size(&self) -> u64 {
        self.session.plan.part_size
    }

    /// Returns the total expected file size in bytes.
    #[must_use]
    pub const fn file_size(&self) -> u64 {
        self.session.plan.file_size
    }

    /// Creates a temporary signed PUT request for one part.
    pub async fn presign_part(
        &self,
        number: PartNumber,
        expires_in: Duration,
    ) -> Result<PresignedUploadPart, Error> {
        self.session.presign_part(number, expires_in).await
    }

    /// Creates a signed PUT request with `Content-MD5` verification for one part.
    pub async fn presign_part_with_md5(
        &self,
        number: PartNumber,
        content_md5: PartMd5,
        expires_in: Duration,
    ) -> Result<PresignedUploadPart, Error> {
        self.session
            .presign_part_with_md5(number, content_md5, expires_in)
            .await
    }
}

impl fmt::Debug for PresignedMultipartPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PresignedMultipartPlan")
            .field("upload_id", &"[REDACTED]")
            .field("part_count", &self.session.part_count())
            .field("part_size", &self.session.plan.part_size)
            .field("file_size", &self.session.plan.file_size)
            .finish()
    }
}

/// A presigned upload coordination plan.
///
/// Automatically chooses between a single presigned PUT request for small objects
/// and an initiated multipart upload session for objects meeting or exceeding the threshold.
#[derive(Clone, Debug)]
pub enum PresignedUploadPlan {
    /// Single PUT request for objects smaller than the upload threshold.
    Single(crate::object::PresignedPutObject),
    /// Multipart upload session for objects meeting or exceeding the upload threshold.
    Multipart(PresignedMultipartPlan),
}

impl PresignedUploadPlan {
    /// Returns `true` if this upload plan requires multipart coordination.
    #[must_use]
    pub fn is_multipart(&self) -> bool {
        matches!(self, Self::Multipart(_))
    }

    /// Returns `true` if this upload plan is a single PUT request.
    #[must_use]
    pub fn is_single(&self) -> bool {
        matches!(self, Self::Single(_))
    }

    /// Returns a reference to the single PUT object if this plan is [`Self::Single`].
    #[must_use]
    pub fn as_single(&self) -> Option<&crate::object::PresignedPutObject> {
        match self {
            Self::Single(put) => Some(put),
            Self::Multipart(_) => None,
        }
    }

    /// Returns a reference to the multipart plan if this plan is [`Self::Multipart`].
    #[must_use]
    pub fn as_multipart(&self) -> Option<&PresignedMultipartPlan> {
        match self {
            Self::Single(_) => None,
            Self::Multipart(plan) => Some(plan),
        }
    }

    /// Consumes the plan, returning the single PUT object if applicable.
    #[must_use]
    pub fn single(self) -> Option<crate::object::PresignedPutObject> {
        match self {
            Self::Single(put) => Some(put),
            Self::Multipart(_) => None,
        }
    }

    /// Consumes the plan, returning the multipart plan if applicable.
    #[must_use]
    pub fn multipart(self) -> Option<PresignedMultipartPlan> {
        match self {
            Self::Single(_) => None,
            Self::Multipart(plan) => Some(plan),
        }
    }
}

/// Result metadata for a completed multipart object.
#[derive(Clone, Debug)]
pub struct CompletedObject {
    etag: Option<String>,
}

impl CompletedObject {
    /// Returns R2's multipart ETag. It is not the MD5 of the complete object.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
}

/// Summary of an in-progress multipart upload returned by a bucket listing.
#[derive(Clone, Eq, PartialEq)]
pub struct MultipartUploadSummary {
    key: String,
    upload_id: String,
    initiated: Option<SystemTime>,
}

impl MultipartUploadSummary {
    /// Returns the target object key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the opaque upload ID assigned by R2.
    #[must_use]
    pub fn expose_upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Returns the time when the multipart upload was initiated, if reported.
    #[must_use]
    pub const fn initiated(&self) -> Option<SystemTime> {
        self.initiated
    }
}

impl fmt::Debug for MultipartUploadSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultipartUploadSummary")
            .field("key", &self.key)
            .field("upload_id", &"[REDACTED]")
            .field("initiated", &self.initiated)
            .finish()
    }
}

/// One bounded page of in-progress multipart uploads.
#[derive(Clone)]
pub struct MultipartUploadPage {
    uploads: Vec<MultipartUploadSummary>,
    common_prefixes: Vec<String>,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
}

impl fmt::Debug for MultipartUploadPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted_upload_id_marker = self.next_upload_id_marker.as_ref().map(|_| "[REDACTED]");
        f.debug_struct("MultipartUploadPage")
            .field("uploads", &self.uploads)
            .field("common_prefixes", &self.common_prefixes)
            .field("next_key_marker", &self.next_key_marker)
            .field("next_upload_id_marker", &redacted_upload_id_marker)
            .finish()
    }
}

impl MultipartUploadPage {
    /// Returns the in-progress uploads found in this page.
    #[must_use]
    pub fn uploads(&self) -> &[MultipartUploadSummary] {
        &self.uploads
    }

    /// Returns common prefixes rolled up by the delimiter.
    #[must_use]
    pub fn common_prefixes(&self) -> &[String] {
        &self.common_prefixes
    }

    /// Returns the key marker needed to request the next page.
    #[must_use]
    pub fn next_key_marker(&self) -> Option<&str> {
        self.next_key_marker.as_deref()
    }

    /// Returns the upload ID marker needed to request the next page.
    #[must_use]
    pub fn next_upload_id_marker(&self) -> Option<&str> {
        self.next_upload_id_marker.as_deref()
    }
}

/// Builder for listing in-progress multipart uploads in an R2 bucket.
#[derive(Clone)]
pub struct ListMultipartUploadsBuilder {
    bucket: Bucket,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: u16,
    key_marker: Option<String>,
    upload_id_marker: Option<String>,
}

impl fmt::Debug for ListMultipartUploadsBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted_upload_id_marker = self.upload_id_marker.as_ref().map(|_| "[REDACTED]");
        f.debug_struct("ListMultipartUploadsBuilder")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("delimiter", &self.delimiter)
            .field("limit", &self.limit)
            .field("key_marker", &self.key_marker)
            .field("upload_id_marker", &redacted_upload_id_marker)
            .finish()
    }
}

impl ListMultipartUploadsBuilder {
    pub(crate) fn new(bucket: Bucket) -> Self {
        Self {
            bucket,
            prefix: None,
            delimiter: None,
            limit: 1_000,
            key_marker: None,
            upload_id_marker: None,
        }
    }

    /// Restricts results to keys beginning with this prefix.
    #[must_use]
    pub fn prefix(mut self, value: impl Into<String>) -> Self {
        self.prefix = Some(value.into());
        self
    }

    /// Groups keys by this delimiter and returns rolled-up common prefixes.
    #[must_use]
    pub fn delimiter(mut self, value: impl Into<String>) -> Self {
        self.delimiter = Some(value.into());
        self
    }

    /// Sets the maximum number of uploads returned, from 1 through 1,000.
    #[must_use]
    pub const fn limit(mut self, value: u16) -> Self {
        self.limit = value;
        self
    }

    /// Sets the key marker for pagination.
    #[must_use]
    pub fn key_marker(mut self, value: impl Into<String>) -> Self {
        self.key_marker = Some(value.into());
        self
    }

    /// Sets the upload ID marker for pagination.
    #[must_use]
    pub fn upload_id_marker(mut self, value: impl Into<String>) -> Self {
        self.upload_id_marker = Some(value.into());
        self
    }

    /// Validates the request and fetches one page of in-progress multipart uploads.
    pub async fn send(self) -> Result<MultipartUploadPage, Error> {
        if self.limit == 0 || self.limit > 1_000 {
            return Err(ValidationError::ListLimitOutOfRange {
                provided: self.limit,
                min: 1,
                max: 1_000,
            }
            .into());
        }
        if let Some(prefix) = self.prefix.as_deref() {
            types::validate_prefix(prefix)?;
        }
        if self.delimiter.as_ref().is_some_and(String::is_empty) {
            return Err(Error::InvalidInput {
                field: "delimiter",
                reason: "must not be empty",
            });
        }
        if self
            .delimiter
            .as_ref()
            .is_some_and(|delimiter| delimiter.len() > types::MAX_KEY_BYTES)
        {
            return Err(Error::InvalidInput {
                field: "delimiter",
                reason: "must not exceed 1,024 UTF-8 bytes",
            });
        }
        if self.key_marker.as_ref().is_some_and(String::is_empty) {
            return Err(Error::InvalidInput {
                field: "key_marker",
                reason: "must not be empty",
            });
        }
        if self.upload_id_marker.as_ref().is_some_and(String::is_empty) {
            return Err(Error::InvalidInput {
                field: "upload_id_marker",
                reason: "must not be empty",
            });
        }

        let output = self
            .bucket
            .client
            .as_sdk()
            .list_multipart_uploads()
            .bucket(self.bucket.name.as_str())
            .set_prefix(self.prefix)
            .set_delimiter(self.delimiter)
            .max_uploads(i32::from(self.limit))
            .set_key_marker(self.key_marker)
            .set_upload_id_marker(self.upload_id_marker)
            .send()
            .await
            .map_err(|error| Error::remote("ListMultipartUploads", &error))?;

        let uploads = output
            .uploads()
            .iter()
            .map(|upload| {
                let key = upload.key().ok_or(Error::Service {
                    operation: "ListMultipartUploads",
                })?;
                let upload_id = upload.upload_id().ok_or(Error::Service {
                    operation: "ListMultipartUploads",
                })?;
                let initiated = upload
                    .initiated()
                    .cloned()
                    .map(SystemTime::try_from)
                    .transpose()
                    .map_err(|_| Error::Service {
                        operation: "ListMultipartUploads",
                    })?;
                Ok(MultipartUploadSummary {
                    key: key.to_owned(),
                    upload_id: upload_id.to_owned(),
                    initiated,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let common_prefixes = output
            .common_prefixes()
            .iter()
            .map(|prefix| {
                prefix
                    .prefix()
                    .map(ToOwned::to_owned)
                    .ok_or(Error::Service {
                        operation: "ListMultipartUploads",
                    })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let (next_key_marker, next_upload_id_marker) = if output.is_truncated() == Some(true) {
            let key_marker = output.next_key_marker.filter(|s| !s.is_empty());
            let upload_id_marker = output.next_upload_id_marker.filter(|s| !s.is_empty());
            if key_marker.is_none() && upload_id_marker.is_none() {
                return Err(Error::Service {
                    operation: "ListMultipartUploads",
                });
            }
            (key_marker, upload_id_marker)
        } else {
            (None, None)
        };

        Ok(MultipartUploadPage {
            uploads,
            common_prefixes,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    /// Streams every page of in-progress multipart uploads until R2 reports completion.
    pub fn into_pages(self) -> impl Stream<Item = Result<MultipartUploadPage, Error>> + Send {
        stream::try_unfold(Some(self), |state| async move {
            let Some(builder) = state else {
                return Ok(None);
            };
            let prev_key = builder.key_marker.clone();
            let prev_upload_id = builder.upload_id_marker.clone();
            let next_builder = builder.clone();
            let page = builder.send().await?;
            let next_key = page.next_key_marker.clone();
            let next_upload_id = page.next_upload_id_marker.clone();

            if (next_key.is_some() || next_upload_id.is_some())
                && next_key == prev_key
                && next_upload_id == prev_upload_id
            {
                return Err(Error::Service {
                    operation: "ListMultipartUploads",
                });
            }

            let state = if next_key.is_some() || next_upload_id.is_some() {
                let mut b = next_builder;
                b.key_marker = next_key;
                b.upload_id_marker = next_upload_id;
                Some(b)
            } else {
                None
            };

            Ok(Some((page, state)))
        })
    }
}

impl Bucket {
    /// Lists in-progress multipart uploads in this bucket.
    #[must_use]
    pub fn list_multipart_uploads(&self) -> ListMultipartUploadsBuilder {
        ListMultipartUploadsBuilder::new(self.clone())
    }

    /// Starts configuring a presigned multipart upload for an object key.
    pub fn presigned_multipart(
        &self,
        key: impl IntoObjectKey,
    ) -> Result<PresignedMultipartBuilder, Error> {
        let key = key.into_object_key()?;
        Ok(PresignedMultipartBuilder {
            bucket: self.clone(),
            key: key.into_inner(),
            file_size: None,
            part_size: None,
            options: ObjectUploadOptions::default(),
        })
    }

    /// Restores a previously captured multipart upload session.
    pub fn resume_presigned_multipart(
        &self,
        snapshot: MultipartSessionSnapshot,
    ) -> Result<PresignedMultipart, Error> {
        if snapshot.bucket != self.name.as_str() {
            return Err(Error::InvalidInput {
                field: "bucket",
                reason: "snapshot belongs to another bucket",
            });
        }
        let plan = MultipartPlan::new(snapshot.file_size, snapshot.part_size)?;
        Ok(PresignedMultipart {
            bucket: self.clone(),
            key: snapshot.key,
            upload_id: snapshot.upload_id,
            plan,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_exact_and_short_final_parts() {
        let plan = MultipartPlan::new(11 * 1024 * 1024, 5 * 1024 * 1024).unwrap();
        assert_eq!(plan.part_count, 3);
        assert_eq!(
            plan.part_length(PartNumber::try_from(1).unwrap()).unwrap(),
            5 * 1024 * 1024
        );
        assert_eq!(
            plan.part_length(PartNumber::try_from(3).unwrap()).unwrap(),
            1024 * 1024
        );
    }

    #[test]
    fn rejects_too_many_parts() {
        let min_part_size = 5 * 1024 * 1024;
        let result = MultipartPlan::new(10_001 * min_part_size, min_part_size);
        assert!(matches!(
            result,
            Err(Error::Validation(ValidationError::TooManyParts {
                required: 10_001,
                max: 10_000
            }))
        ));
    }

    #[test]
    fn rejects_an_object_over_r2s_effective_limit() {
        let max_part_size = types::MAX_MULTIPART_PART_SIZE;
        let result = MultipartPlan::new(MAX_MULTIPART_OBJECT_SIZE + 1, max_part_size);
        assert!(matches!(
            result,
            Err(Error::Validation(
                ValidationError::MultipartObjectTooLarge {
                    provided,
                    max: MAX_MULTIPART_OBJECT_SIZE
                }
            )) if provided == MAX_MULTIPART_OBJECT_SIZE + 1
        ));
    }

    #[test]
    fn rejects_subsecond_presign_expiry() {
        assert!(matches!(
            types::validate_expiry(Duration::from_millis(999)),
            Err(Error::Validation(
                ValidationError::PresignExpiryOutOfRange { provided, .. }
            )) if provided == Duration::from_millis(999)
        ));
    }

    #[test]
    fn canonicalizes_manifest_and_rejects_duplicates() {
        let one = PartNumber::try_from(1).unwrap();
        let two = PartNumber::try_from(2).unwrap();
        let manifest = CompletionManifest::try_from_parts([
            UploadedPart::new(two, "two").unwrap(),
            UploadedPart::new(one, "one").unwrap(),
        ])
        .unwrap();
        assert_eq!(manifest.0[0].part_number(), one);

        let duplicate = CompletionManifest::try_from_parts([
            UploadedPart::new(one, "one").unwrap(),
            UploadedPart::new(one, "again").unwrap(),
        ]);
        assert!(duplicate.is_err());
    }

    #[test]
    fn redacts_secret_wrappers() {
        let min_part_size = 5 * 1024 * 1024;
        let url = SecretUrl("https://example.invalid/?X-Amz-Signature=secret".into());
        assert!(!format!("{url:?}").contains("X-Amz-Signature"));
        let snapshot = MultipartSessionSnapshot::restore(
            "r2kit",
            "key",
            "secret-upload-id",
            min_part_size,
            min_part_size,
        )
        .unwrap();
        assert!(!format!("{snapshot:?}").contains("secret-upload-id"));

        let page = MultipartUploadPage {
            uploads: Vec::new(),
            common_prefixes: Vec::new(),
            next_key_marker: Some("next-key".into()),
            next_upload_id_marker: Some("sensitive-page-upload-id-777".into()),
        };
        let page_debug = format!("{page:?}");
        assert!(
            !page_debug.contains("sensitive-page-upload-id-777"),
            "MultipartUploadPage leaked upload_id in Debug: {page_debug}"
        );
    }
}
