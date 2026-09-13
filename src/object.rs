use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    time::{Duration, SystemTime},
};

use aws_sdk_s3::{
    presigning::PresigningConfig,
    primitives::{ByteStream, DateTime},
    types::{Delete, ObjectIdentifier},
};
use aws_smithy_types::date_time::Format as DateTimeFormat;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{Stream, StreamExt, stream};
use headers::Header;
use mime::Mime;
use oxilangtag::LanguageTag;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

use crate::{
    Bucket, BucketName, Error, IntoBucketName, IntoContentType, IntoObjectKey, ObjectKey,
    PresignedRequest, ValidationError,
    multipart::{PresignedMultipartPlan, PresignedUploadPlan},
    types,
};

macro_rules! map_object_error {
    ($operation:expr, $error:expr) => {{
        if $error
            .raw_response()
            .is_some_and(|response| response.status().as_u16() == 404)
        {
            Error::NotFound
        } else if $error
            .raw_response()
            .is_some_and(|response| response.status().as_u16() == 304)
        {
            Error::NotModified
        } else if $error
            .raw_response()
            .is_some_and(|response| response.status().as_u16() == 412)
        {
            Error::PreconditionFailed
        } else {
            Error::remote($operation, &$error)
        }
    }};
}

const MAX_SINGLE_PUT_SIZE: u64 = types::MAX_UPLOAD_SIZE;
const MAX_LIST_KEYS: u16 = 1_000;
const MAX_DELETE_KEYS: usize = 1_000;
const MAX_OBJECT_METADATA_BYTES: usize = 8_192;
const COPY_SOURCE_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b'?')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Checksum algorithm for upload integrity verification.
///
/// R2 validates the provided checksum server-side and returns `BadDigest`
/// (HTTP 400) when the uploaded content does not match.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChecksumAlgorithm {
    /// CRC-32 checksum.
    Crc32,
    /// CRC-32C (Castagnoli) checksum.
    Crc32c,
    /// SHA-1 digest.
    Sha1,
    /// SHA-256 digest.
    Sha256,
}

impl ChecksumAlgorithm {
    /// Returns the header name used by S3/R2 for this algorithm.
    #[must_use]
    pub const fn header_name(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
        }
    }

    /// Expected byte length of the raw binary digest.
    #[must_use]
    pub const fn digest_length(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
        }
    }
}

#[cfg(feature = "checksum")]
pub(crate) fn compute_checksum(bytes: &[u8], algorithm: ChecksumAlgorithm) -> String {
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(bytes);
            let crc = hasher.finalize();
            STANDARD.encode(crc.to_be_bytes())
        }
        ChecksumAlgorithm::Crc32c => {
            let crc = crc32c::crc32c(bytes);
            STANDARD.encode(crc.to_be_bytes())
        }
        ChecksumAlgorithm::Sha1 => {
            use sha1::Digest;
            let mut hasher = sha1::Sha1::new();
            hasher.update(bytes);
            let digest = hasher.finalize();
            STANDARD.encode(digest)
        }
        ChecksumAlgorithm::Sha256 => {
            use sha2::Digest;
            let mut hasher = sha2::Sha256::new();
            hasher.update(bytes);
            let digest = hasher.finalize();
            STANDARD.encode(digest)
        }
    }
}

/// Typed system metadata applied when an object is created.
///
/// These values are stored by R2 on the completed object. For a presigned
/// single PUT, every returned required header must be replayed exactly by the
/// uploader. Multipart metadata is applied once when the session is created,
/// not on individual part requests.
#[derive(Clone, Debug, Default)]
pub struct ObjectUploadOptions {
    content_type: Option<Result<Mime, Error>>,
    cache_control: Option<headers::CacheControl>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    expires: Option<SystemTime>,
    custom: BTreeMap<String, String>,
    if_match: Option<String>,
    if_none_match: Option<String>,
    checksum: Option<ChecksumAlgorithm>,
    checksum_value: Option<(ChecksumAlgorithm, String)>,
}

impl ObjectUploadOptions {
    /// Creates empty upload metadata (equivalent to `Default::default()`).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the configured media type.
    #[must_use]
    pub fn content_type(&self) -> Option<&Mime> {
        match &self.content_type {
            Some(Ok(mime)) => Some(mime),
            _ => None,
        }
    }

    /// Returns the configured cache policy.
    #[must_use]
    pub const fn cache_control(&self) -> Option<&headers::CacheControl> {
        self.cache_control.as_ref()
    }

    /// Returns the configured content disposition.
    #[must_use]
    pub fn content_disposition(&self) -> Option<&str> {
        self.content_disposition.as_deref()
    }

    /// Returns the configured content encoding.
    #[must_use]
    pub fn content_encoding(&self) -> Option<&str> {
        self.content_encoding.as_deref()
    }

    /// Returns the configured content language.
    #[must_use]
    pub fn content_language(&self) -> Option<&str> {
        self.content_language.as_deref()
    }

    /// Returns the configured HTTP expiration time.
    #[must_use]
    pub const fn expires(&self) -> Option<SystemTime> {
        self.expires
    }

    /// Returns the configured `If-Match` condition.
    #[must_use]
    pub fn if_match(&self) -> Option<&str> {
        self.if_match.as_deref()
    }

    /// Returns the configured `If-None-Match` condition.
    #[must_use]
    pub fn if_none_match(&self) -> Option<&str> {
        self.if_none_match.as_deref()
    }

    /// Returns the configured checksum algorithm.
    #[must_use]
    pub const fn checksum(&self) -> Option<ChecksumAlgorithm> {
        self.checksum
    }

    /// Returns the precomputed checksum algorithm and Base64-encoded value.
    #[must_use]
    pub fn checksum_value(&self) -> Option<(ChecksumAlgorithm, &str)> {
        self.checksum_value
            .as_ref()
            .map(|(algo, val)| (*algo, val.as_str()))
    }

    /// Returns user-defined metadata without the `x-amz-meta-` prefix.
    #[must_use]
    pub const fn custom_metadata(&self) -> &BTreeMap<String, String> {
        &self.custom
    }

    /// Returns whether no system metadata was configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.content_type.is_none()
            && self.cache_control.is_none()
            && self.content_disposition.is_none()
            && self.content_encoding.is_none()
            && self.content_language.is_none()
            && self.expires.is_none()
            && self.custom.is_empty()
            && self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.checksum.is_none()
            && self.checksum_value.is_none()
    }

    /// Returns a copy configured with this MIME media type.
    #[must_use]
    pub fn with_content_type(mut self, value: impl IntoContentType) -> Self {
        self.content_type = Some(value.into_content_type());
        self
    }

    /// Returns a copy configured with this typed HTTP cache policy.
    #[must_use]
    pub fn with_cache_control(mut self, value: headers::CacheControl) -> Self {
        self.cache_control = Some(value);
        self
    }

    /// Returns a copy configured with this content disposition.
    #[must_use]
    pub fn with_content_disposition(mut self, value: impl Into<String>) -> Self {
        self.content_disposition = Some(value.into());
        self
    }

    /// Returns a copy configured with this content encoding.
    #[must_use]
    pub fn with_content_encoding(mut self, value: impl Into<String>) -> Self {
        self.content_encoding = Some(value.into());
        self
    }

    /// Returns a copy configured with this content language.
    #[must_use]
    pub fn with_content_language(mut self, value: impl Into<String>) -> Self {
        self.content_language = Some(value.into());
        self
    }

    /// Returns a copy configured with this HTTP expiration time.
    #[must_use]
    pub fn with_expires(mut self, value: SystemTime) -> Self {
        self.expires = Some(value);
        self
    }

    /// Returns a copy containing this user-defined metadata entry.
    ///
    /// The key is supplied without the `x-amz-meta-` prefix. Keys are
    /// canonicalized to lowercase when the options are validated.
    #[must_use]
    pub fn with_custom_metadata(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }

    /// Returns a copy configured with this `If-Match` condition.
    #[must_use]
    pub fn with_if_match(mut self, value: impl Into<String>) -> Self {
        self.if_match = Some(value.into());
        self
    }

    /// Returns a copy configured with this `If-None-Match` condition.
    #[must_use]
    pub fn with_if_none_match(mut self, value: impl Into<String>) -> Self {
        self.if_none_match = Some(value.into());
        self
    }

    /// Returns a copy configured with this checksum algorithm for auto-computation.
    #[must_use]
    pub fn with_checksum(mut self, value: ChecksumAlgorithm) -> Self {
        self.checksum = Some(value);
        self
    }

    /// Returns a copy configured with this precomputed checksum.
    #[must_use]
    pub fn with_checksum_value(
        mut self,
        algorithm: ChecksumAlgorithm,
        base64_value: impl Into<String>,
    ) -> Self {
        self.checksum_value = Some((algorithm, base64_value.into()));
        self
    }

    pub(crate) fn apply_to<T: SetObjectMetadata>(&self, req: T) -> T {
        req.set_content_type(self.content_type().map(ToString::to_string))
            .set_cache_control(self.cache_control.as_ref().map(encode_header))
            .set_content_disposition(self.content_disposition.clone())
            .set_content_encoding(self.content_encoding.clone())
            .set_content_language(self.content_language.clone())
            .set_expires(self.expires.map(DateTime::from))
    }

    pub(crate) fn if_match_value(&self) -> Option<String> {
        self.if_match.clone()
    }

    pub(crate) fn if_none_match_value(&self) -> Option<String> {
        self.if_none_match.clone()
    }

    pub(crate) fn custom_metadata_values(&self) -> Option<HashMap<String, String>> {
        (!self.custom.is_empty()).then(|| {
            self.custom
                .iter()
                .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
                .collect()
        })
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if let Some(res) = &self.content_type {
            res.as_ref().map_err(Clone::clone)?;
        }
        for (field, value) in [
            ("content_disposition", self.content_disposition.as_deref()),
            ("content_encoding", self.content_encoding.as_deref()),
            ("if_match", self.if_match.as_deref()),
            ("if_none_match", self.if_none_match.as_deref()),
        ] {
            if let Some(value) = value {
                validate_metadata_header(field, value)?;
            }
        }
        if let Some(value) = self.content_language.as_deref() {
            validate_content_language(value)?;
        }
        if let Some((algorithm, base64_val)) = &self.checksum_value {
            let decoded = STANDARD
                .decode(base64_val)
                .map_err(|_| Error::InvalidInput {
                    field: "checksum",
                    reason: "must be canonical Base64",
                })?;
            if decoded.len() != algorithm.digest_length()
                || STANDARD.encode(&decoded) != *base64_val
            {
                return Err(Error::InvalidInput {
                    field: "checksum",
                    reason: "checksum digest length or encoding does not match algorithm",
                });
            }
        }

        let mut total_bytes = self
            .content_type()
            .map_or(0, |value| "content-type".len() + value.to_string().len())
            + self.cache_control.as_ref().map_or(0, |value| {
                "cache-control".len() + encode_header(value).len()
            })
            + self
                .content_disposition
                .as_ref()
                .map_or(0, |value| "content-disposition".len() + value.len())
            + self
                .content_encoding
                .as_ref()
                .map_or(0, |value| "content-encoding".len() + value.len())
            + self
                .content_language
                .as_ref()
                .map_or(0, |value| "content-language".len() + value.len());
        let mut normalized = BTreeMap::new();
        for (key, value) in &self.custom {
            validate_metadata_key(key)?;
            validate_metadata_header("custom_metadata", value)?;
            let key = key.to_ascii_lowercase();
            if normalized.insert(key.clone(), ()).is_some() {
                return Err(Error::InvalidInput {
                    field: "custom_metadata",
                    reason: "contains duplicate keys after ASCII case normalization",
                });
            }
            total_bytes = total_bytes
                .saturating_add("x-amz-meta-".len())
                .saturating_add(key.len())
                .saturating_add(value.len());
        }
        if total_bytes > MAX_OBJECT_METADATA_BYTES {
            return Err(Error::InvalidInput {
                field: "upload_options",
                reason: "metadata exceeds R2's 8,192-byte object metadata limit",
            });
        }
        Ok(())
    }
}

pub(crate) trait SetObjectMetadata {
    fn set_content_type(self, value: Option<String>) -> Self;
    fn set_cache_control(self, value: Option<String>) -> Self;
    fn set_content_disposition(self, value: Option<String>) -> Self;
    fn set_content_encoding(self, value: Option<String>) -> Self;
    fn set_content_language(self, value: Option<String>) -> Self;
    fn set_expires(self, value: Option<DateTime>) -> Self;
}

macro_rules! impl_set_object_metadata {
    ($builder:ty) => {
        impl SetObjectMetadata for $builder {
            fn set_content_type(self, value: Option<String>) -> Self {
                self.set_content_type(value)
            }
            fn set_cache_control(self, value: Option<String>) -> Self {
                self.set_cache_control(value)
            }
            fn set_content_disposition(self, value: Option<String>) -> Self {
                self.set_content_disposition(value)
            }
            fn set_content_encoding(self, value: Option<String>) -> Self {
                self.set_content_encoding(value)
            }
            fn set_content_language(self, value: Option<String>) -> Self {
                self.set_content_language(value)
            }
            fn set_expires(self, value: Option<DateTime>) -> Self {
                self.set_expires(value)
            }
        }
    };
}

impl_set_object_metadata!(aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder);
impl_set_object_metadata!(aws_sdk_s3::operation::copy_object::builders::CopyObjectFluentBuilder);
impl_set_object_metadata!(
    aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder
);

fn validate_metadata_header(field: &'static str, value: &str) -> Result<(), Error> {
    if value.is_empty() {
        return Err(Error::InvalidInput {
            field,
            reason: "must not be empty",
        });
    }
    if !value
        .bytes()
        .all(|byte| matches!(byte, b'\t' | 0x20..=0x7e))
    {
        return Err(Error::InvalidInput {
            field,
            reason: "must contain only visible ASCII or horizontal tabs",
        });
    }
    Ok(())
}

fn validate_content_language(value: &str) -> Result<(), Error> {
    validate_metadata_header("content_language", value)?;
    if value.split(',').any(|tag| {
        let tag = tag.trim();
        tag.is_empty() || LanguageTag::parse(tag).is_err()
    }) {
        return Err(Error::InvalidInput {
            field: "content_language",
            reason: "must contain one or more well-formed BCP 47 language tags",
        });
    }
    Ok(())
}

fn validate_metadata_key(key: &str) -> Result<(), Error> {
    if key.is_empty() {
        return Err(Error::InvalidInput {
            field: "custom_metadata",
            reason: "keys must not be empty",
        });
    }
    if key.to_ascii_lowercase().starts_with("x-amz-meta-") {
        return Err(Error::InvalidInput {
            field: "custom_metadata",
            reason: "keys must omit the x-amz-meta- prefix",
        });
    }
    if !key
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::InvalidInput {
            field: "custom_metadata",
            reason: "keys must use ASCII letters, digits, hyphens, underscores, or periods",
        });
    }
    Ok(())
}

fn encode_header(value: &impl Header) -> String {
    let mut encoded = Vec::with_capacity(1);
    value.encode(&mut encoded);
    encoded
        .into_iter()
        .next()
        .expect("typed header must encode one value")
        .to_str()
        .expect("typed cache policy must be visible ASCII")
        .to_owned()
}

/// Metadata common to downloaded and inspected objects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    size: u64,
    etag: Option<String>,
    content_type: Option<String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    expires: Option<SystemTime>,
    last_modified: Option<SystemTime>,
    custom: BTreeMap<String, String>,
}

impl ObjectMetadata {
    /// Returns the object size in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns R2's opaque entity tag when present.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Returns the object's media type when present.
    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// Returns the object's cache policy when present.
    #[must_use]
    pub fn cache_control(&self) -> Option<&str> {
        self.cache_control.as_deref()
    }

    /// Returns the response content disposition when present.
    #[must_use]
    pub fn content_disposition(&self) -> Option<&str> {
        self.content_disposition.as_deref()
    }

    /// Returns the response content encoding when present.
    #[must_use]
    pub fn content_encoding(&self) -> Option<&str> {
        self.content_encoding.as_deref()
    }

    /// Returns the response content language when present.
    #[must_use]
    pub fn content_language(&self) -> Option<&str> {
        self.content_language.as_deref()
    }

    /// Returns the HTTP expiration time when present.
    #[must_use]
    pub const fn expires(&self) -> Option<SystemTime> {
        self.expires
    }

    /// Returns the object's last modification time when present.
    #[must_use]
    pub const fn last_modified(&self) -> Option<SystemTime> {
        self.last_modified
    }

    /// Returns user-defined object metadata.
    #[must_use]
    pub const fn custom(&self) -> &BTreeMap<String, String> {
        &self.custom
    }
}

/// A streaming object download and its response metadata.
pub struct DownloadedObject {
    metadata: ObjectMetadata,
    body: ByteStream,
}

impl DownloadedObject {
    /// Returns metadata reported with the download.
    #[must_use]
    pub const fn metadata(&self) -> &ObjectMetadata {
        &self.metadata
    }

    /// Returns a shared reference to the streaming body.
    #[must_use]
    pub const fn body(&self) -> &ByteStream {
        &self.body
    }

    /// Consumes the response and returns its streaming body.
    #[must_use]
    pub fn into_body(self) -> ByteStream {
        self.body
    }
}

impl fmt::Debug for DownloadedObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownloadedObject")
            .field("metadata", &self.metadata)
            .field("body", &"ByteStream(..)")
            .finish()
    }
}

/// A fully downloaded object containing its metadata and body in memory.
#[derive(Clone)]
pub struct ObjectBytes {
    /// The object's metadata.
    pub metadata: ObjectMetadata,
    /// The object's body bytes.
    pub bytes: bytes::Bytes,
}

impl fmt::Debug for ObjectBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectBytes")
            .field("metadata", &self.metadata)
            .field("bytes", &format!("{} bytes", self.bytes.len()))
            .finish()
    }
}

/// Result metadata for a successful single-request upload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutObjectResult {
    etag: Option<String>,
}

/// Result metadata for a server-side object copy.
#[derive(Clone, Debug)]
pub struct CopyObjectResult {
    etag: Option<String>,
    last_modified: Option<SystemTime>,
}

impl CopyObjectResult {
    /// Returns the ETag of the copied object, when R2 supplied one.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Returns the creation time of the copied object, when R2 supplied one.
    #[must_use]
    pub const fn last_modified(&self) -> Option<SystemTime> {
        self.last_modified
    }
}

impl PutObjectResult {
    /// Returns R2's opaque entity tag when present.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
}

/// A presigned single-request upload with an exact expected body length.
#[derive(Clone, Debug)]
pub struct PresignedPutObject {
    content_length: u64,
    request: PresignedRequest,
}

impl PresignedPutObject {
    /// Returns the exact body length the uploader must send.
    #[must_use]
    pub const fn content_length(&self) -> u64 {
        self.content_length
    }

    /// Returns the signed PUT request.
    #[must_use]
    pub const fn request(&self) -> &PresignedRequest {
        &self.request
    }

    /// Consumes this value and returns the signed PUT request.
    #[must_use]
    pub fn into_request(self) -> PresignedRequest {
        self.request
    }

    /// Returns the signed URL as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.request.as_str()
    }

    /// Consumes this value and returns the signed URL as an owned string.
    #[must_use]
    pub fn into_url_string(self) -> String {
        self.request.into_url_string()
    }
}

/// Alias for an object summary returned by a bucket listing.
pub type ObjectItem = ObjectSummary;

/// One object returned by a bucket listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectSummary {
    key: String,
    size: u64,
    etag: Option<String>,
    last_modified: Option<SystemTime>,
}

impl ObjectSummary {
    /// Returns the object key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the object size in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns R2's opaque entity tag when present.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Returns the object's last modification time when present.
    #[must_use]
    pub const fn last_modified(&self) -> Option<SystemTime> {
        self.last_modified
    }
}

/// A single page from an R2 object listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPage {
    objects: Vec<ObjectSummary>,
    common_prefixes: Vec<String>,
    next_continuation_token: Option<String>,
}

/// One per-key failure returned by an R2 multi-object delete request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteObjectFailure {
    key: String,
    code: Option<String>,
    message: Option<String>,
}

impl DeleteObjectFailure {
    /// Returns the key R2 did not delete.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns R2's machine-readable error code when present.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Returns R2's error message when present.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Aggregate result of one or more `DeleteObjects` requests.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeleteObjectsResult {
    deleted_keys: Vec<String>,
    failures: Vec<DeleteObjectFailure>,
    request_count: usize,
}

impl DeleteObjectsResult {
    /// Returns keys R2 reported as deleted.
    #[must_use]
    pub fn deleted_keys(&self) -> &[String] {
        &self.deleted_keys
    }

    /// Returns per-key failures reported inside successful HTTP responses.
    #[must_use]
    pub fn failures(&self) -> &[DeleteObjectFailure] {
        &self.failures
    }

    /// Returns the number of completed `DeleteObjects` requests.
    #[must_use]
    pub const fn request_count(&self) -> usize {
        self.request_count
    }

    /// Returns whether every requested key was reported as deleted.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }
}

/// A request-level batch delete error with results from earlier batches.
#[derive(Debug)]
pub struct BatchDeleteError {
    error: Error,
    partial: DeleteObjectsResult,
}

impl BatchDeleteError {
    /// Returns the sanitized request failure.
    #[must_use]
    pub const fn error(&self) -> &Error {
        &self.error
    }

    /// Returns results accumulated before the failed request.
    #[must_use]
    pub const fn partial_result(&self) -> &DeleteObjectsResult {
        &self.partial
    }
}

impl fmt::Display for BatchDeleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "batch delete failed: {}", self.error)
    }
}

impl std::error::Error for BatchDeleteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl ObjectPage {
    /// Returns objects in this page.
    #[must_use]
    pub fn objects(&self) -> &[ObjectSummary] {
        &self.objects
    }

    /// Consumes the page and returns its objects.
    #[must_use]
    pub fn into_objects(self) -> Vec<ObjectSummary> {
        self.objects
    }

    /// Returns rolled-up prefixes when a delimiter was requested.
    #[must_use]
    pub fn common_prefixes(&self) -> &[String] {
        &self.common_prefixes
    }

    /// Returns the opaque token needed to request the next page.
    #[must_use]
    pub fn next_continuation_token(&self) -> Option<&str> {
        self.next_continuation_token.as_deref()
    }
}

/// Builder for one bounded page of an R2 object listing.
#[derive(Clone, Debug)]
pub struct ListObjectsBuilder {
    bucket: Bucket,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: u16,
    continuation_token: Option<String>,
}

impl ListObjectsBuilder {
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

    /// Sets the maximum number of entries returned, from 1 through 1,000.
    #[must_use]
    pub const fn limit(mut self, value: u16) -> Self {
        self.limit = value;
        self
    }

    /// Continues from an opaque token returned by a previous page.
    #[must_use]
    pub fn continuation_token(mut self, value: impl Into<String>) -> Self {
        self.continuation_token = Some(value.into());
        self
    }

    /// Validates the request and fetches one page.
    pub async fn send(self) -> Result<ObjectPage, Error> {
        if self.limit == 0 || self.limit > MAX_LIST_KEYS {
            return Err(ValidationError::ListLimitOutOfRange {
                provided: self.limit,
                min: 1,
                max: MAX_LIST_KEYS,
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
        if self
            .continuation_token
            .as_ref()
            .is_some_and(String::is_empty)
        {
            return Err(Error::InvalidInput {
                field: "continuation_token",
                reason: "must not be empty",
            });
        }

        let output = self
            .bucket
            .client
            .as_sdk()
            .list_objects_v2()
            .bucket(self.bucket.name.as_str())
            .set_prefix(self.prefix)
            .set_delimiter(self.delimiter)
            .max_keys(i32::from(self.limit))
            .set_continuation_token(self.continuation_token)
            .send()
            .await
            .map_err(|error| Error::remote("ListObjectsV2", &error))?;

        let objects = output
            .contents()
            .iter()
            .map(|object| {
                let key = object.key().ok_or(Error::Service {
                    operation: "ListObjectsV2",
                })?;
                let size = non_negative_size(object.size(), "ListObjectsV2")?;
                Ok(ObjectSummary {
                    key: key.to_owned(),
                    size,
                    etag: object.e_tag().map(ToOwned::to_owned),
                    last_modified: optional_system_time(
                        object.last_modified().cloned(),
                        "ListObjectsV2",
                    )?,
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
                        operation: "ListObjectsV2",
                    })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let next_continuation_token = if output.is_truncated() == Some(true) {
            match output.next_continuation_token {
                Some(token) if !token.is_empty() => Some(token),
                _ => {
                    return Err(Error::Service {
                        operation: "ListObjectsV2",
                    });
                }
            }
        } else {
            None
        };

        Ok(ObjectPage {
            objects,
            common_prefixes,
            next_continuation_token,
        })
    }

    /// Streams every listing page until R2 reports that the listing is complete.
    ///
    /// The configured page limit applies to each request. Page boundaries and
    /// common prefixes are preserved.
    pub fn into_pages(self) -> impl Stream<Item = Result<ObjectPage, Error>> + Send {
        stream::try_unfold(Some(self), |state| async move {
            let Some(builder) = state else {
                return Ok(None);
            };
            let previous_token = builder.continuation_token.clone();
            let next_builder = builder.clone();
            let page = builder.send().await?;
            let next_token = page.next_continuation_token.clone();
            if next_token.is_some() && next_token == previous_token {
                return Err(Error::Service {
                    operation: "ListObjectsV2",
                });
            }
            let state = next_token.map(|token| next_builder.continuation_token(token));
            Ok(Some((page, state)))
        })
    }

    /// Streams individual object summaries across all pages until R2 reports that
    /// the listing is complete.
    pub fn into_stream(self) -> impl Stream<Item = Result<ObjectItem, Error>> + Send {
        self.into_pages()
            .map(|page_res| match page_res {
                Ok(page) => {
                    let items: Vec<Result<ObjectItem, Error>> =
                        page.into_objects().into_iter().map(Ok).collect();
                    stream::iter(items)
                }
                Err(err) => stream::iter(vec![Err(err)]),
            })
            .flatten()
    }

    /// Streams individual object summaries across all pages until R2 reports that
    /// the listing is complete.
    ///
    /// Alias for [`into_stream`](Self::into_stream).
    #[inline]
    pub fn into_objects(self) -> impl Stream<Item = Result<ObjectItem, Error>> + Send {
        self.into_stream()
    }
}

/// A byte range for partial object downloads.
///
/// R2 supports single byte ranges only. Multi-range requests are not supported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ByteRange {
    /// Download bytes from `start` to `end` inclusive (e.g. `bytes=0-1023`).
    Bounded(u64, u64),
    /// Download from `offset` to end of object (e.g. `bytes=500-`).
    From(u64),
    /// Download the last `n` bytes (e.g. `bytes=-500`).
    Suffix(u64),
}

impl ByteRange {
    /// Formats this byte range as a standard HTTP `Range` header value.
    #[must_use]
    pub fn as_header_value(&self) -> String {
        match self {
            Self::Bounded(start, end) => format!("bytes={start}-{end}"),
            Self::From(offset) => format!("bytes={offset}-"),
            Self::Suffix(length) => format!("bytes=-{length}"),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Bounded(start, end) if start > end => Err(Error::InvalidInput {
                field: "range",
                reason: "start must not exceed end",
            }),
            Self::Suffix(0) => Err(Error::InvalidInput {
                field: "range",
                reason: "suffix length must be greater than zero",
            }),
            _ => Ok(()),
        }
    }
}

/// Strategy for copying metadata during a server-side object copy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataDirective {
    /// Copy the source object's metadata (default).
    #[default]
    Copy,
    /// Replace the source object's metadata with newly specified metadata.
    Replace,
}

impl MetadataDirective {
    pub(crate) const fn as_sdk_directive(self) -> aws_sdk_s3::types::MetadataDirective {
        match self {
            Self::Copy => aws_sdk_s3::types::MetadataDirective::Copy,
            Self::Replace => aws_sdk_s3::types::MetadataDirective::Replace,
        }
    }
}

/// Builder for configuring a single-object download.
#[derive(Clone, Debug)]
pub struct GetObjectBuilder {
    bucket: Bucket,
    key: Result<ObjectKey, Error>,
    range: Option<ByteRange>,
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<SystemTime>,
    if_unmodified_since: Option<SystemTime>,
}

impl GetObjectBuilder {
    pub(crate) fn new(bucket: Bucket, key: impl IntoObjectKey) -> Self {
        Self {
            bucket,
            key: key.into_object_key(),
            range: None,
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
        }
    }

    /// Sets a byte range for partial download.
    #[must_use]
    pub fn range(mut self, value: ByteRange) -> Self {
        self.range = Some(value);
        self
    }

    /// Sets an `If-Match` condition requiring the ETag to match.
    #[must_use]
    pub fn if_match(mut self, value: impl Into<String>) -> Self {
        self.if_match = Some(value.into());
        self
    }

    /// Sets an `If-None-Match` condition requiring the ETag not to match.
    #[must_use]
    pub fn if_none_match(mut self, value: impl Into<String>) -> Self {
        self.if_none_match = Some(value.into());
        self
    }

    /// Sets an `If-Modified-Since` condition.
    #[must_use]
    pub fn if_modified_since(mut self, value: SystemTime) -> Self {
        self.if_modified_since = Some(value);
        self
    }

    /// Sets an `If-Unmodified-Since` condition.
    #[must_use]
    pub fn if_unmodified_since(mut self, value: SystemTime) -> Self {
        self.if_unmodified_since = Some(value);
        self
    }

    /// Sends the GET request to R2.
    pub async fn send(self) -> Result<DownloadedObject, Error> {
        let key = self.key?;
        if let Some(range) = &self.range {
            range.validate()?;
        }
        if let Some(if_match) = self.if_match.as_deref() {
            validate_metadata_header("if_match", if_match)?;
        }
        if let Some(if_none_match) = self.if_none_match.as_deref() {
            validate_metadata_header("if_none_match", if_none_match)?;
        }

        let mut req = self
            .bucket
            .client
            .as_sdk()
            .get_object()
            .bucket(self.bucket.name.as_str())
            .key(key.as_str());

        if let Some(range) = self.range {
            req = req.range(range.as_header_value());
        }
        if let Some(if_match) = self.if_match {
            req = req.if_match(if_match);
        }
        if let Some(if_none_match) = self.if_none_match {
            req = req.if_none_match(if_none_match);
        }
        if let Some(if_modified_since) = self.if_modified_since {
            req = req.if_modified_since(DateTime::from(if_modified_since));
        }
        if let Some(if_unmodified_since) = self.if_unmodified_since {
            req = req.if_unmodified_since(DateTime::from(if_unmodified_since));
        }

        let output = req
            .send()
            .await
            .map_err(|error| map_object_error!("GetObject", error))?;

        let metadata = ObjectMetadata {
            size: non_negative_size(output.content_length, "GetObject")?,
            etag: output.e_tag,
            content_type: output.content_type,
            cache_control: output.cache_control,
            content_disposition: output.content_disposition,
            content_encoding: output.content_encoding,
            content_language: output.content_language,
            expires: optional_http_date(output.expires_string.as_deref(), "GetObject")?,
            last_modified: optional_system_time(output.last_modified, "GetObject")?,
            custom: output.metadata.unwrap_or_default().into_iter().collect(),
        };
        Ok(DownloadedObject {
            metadata,
            body: output.body,
        })
    }
}

/// Builder for fetching object metadata with optional conditions.
#[derive(Clone, Debug)]
pub struct HeadObjectBuilder {
    bucket: Bucket,
    key: Result<ObjectKey, Error>,
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<SystemTime>,
    if_unmodified_since: Option<SystemTime>,
}

impl HeadObjectBuilder {
    pub(crate) fn new(bucket: Bucket, key: impl IntoObjectKey) -> Self {
        Self {
            bucket,
            key: key.into_object_key(),
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
        }
    }

    /// Sets an `If-Match` condition requiring the ETag to match.
    #[must_use]
    pub fn if_match(mut self, value: impl Into<String>) -> Self {
        self.if_match = Some(value.into());
        self
    }

    /// Sets an `If-None-Match` condition requiring the ETag not to match.
    #[must_use]
    pub fn if_none_match(mut self, value: impl Into<String>) -> Self {
        self.if_none_match = Some(value.into());
        self
    }

    /// Sets an `If-Modified-Since` condition.
    #[must_use]
    pub fn if_modified_since(mut self, value: SystemTime) -> Self {
        self.if_modified_since = Some(value);
        self
    }

    /// Sets an `If-Unmodified-Since` condition.
    #[must_use]
    pub fn if_unmodified_since(mut self, value: SystemTime) -> Self {
        self.if_unmodified_since = Some(value);
        self
    }

    /// Sends the HEAD request to R2.
    pub async fn send(self) -> Result<ObjectMetadata, Error> {
        let key = self.key?;
        if let Some(if_match) = self.if_match.as_deref() {
            validate_metadata_header("if_match", if_match)?;
        }
        if let Some(if_none_match) = self.if_none_match.as_deref() {
            validate_metadata_header("if_none_match", if_none_match)?;
        }

        let mut req = self
            .bucket
            .client
            .as_sdk()
            .head_object()
            .bucket(self.bucket.name.as_str())
            .key(key.as_str());

        if let Some(if_match) = self.if_match {
            req = req.if_match(if_match);
        }
        if let Some(if_none_match) = self.if_none_match {
            req = req.if_none_match(if_none_match);
        }
        if let Some(if_modified_since) = self.if_modified_since {
            req = req.if_modified_since(DateTime::from(if_modified_since));
        }
        if let Some(if_unmodified_since) = self.if_unmodified_since {
            req = req.if_unmodified_since(DateTime::from(if_unmodified_since));
        }

        let output = req
            .send()
            .await
            .map_err(|error| map_object_error!("HeadObject", error))?;

        Ok(ObjectMetadata {
            size: non_negative_size(output.content_length, "HeadObject")?,
            etag: output.e_tag,
            content_type: output.content_type,
            cache_control: output.cache_control,
            content_disposition: output.content_disposition,
            content_encoding: output.content_encoding,
            content_language: output.content_language,
            expires: optional_http_date(output.expires_string.as_deref(), "HeadObject")?,
            last_modified: optional_system_time(output.last_modified, "HeadObject")?,
            custom: output.metadata.unwrap_or_default().into_iter().collect(),
        })
    }
}

/// Builder for copying an object server-side.
#[derive(Clone, Debug)]
pub struct CopyObjectBuilder {
    bucket: Bucket,
    source_key: Result<ObjectKey, Error>,
    destination_key: Result<ObjectKey, Error>,
    source_bucket: Option<Result<BucketName, Error>>,
    source_if_match: Option<String>,
    source_if_none_match: Option<String>,
    source_if_modified_since: Option<SystemTime>,
    source_if_unmodified_since: Option<SystemTime>,
    metadata_directive: Option<MetadataDirective>,
    metadata_options: Option<ObjectUploadOptions>,
}

impl CopyObjectBuilder {
    pub(crate) fn new(
        bucket: Bucket,
        source_key: impl IntoObjectKey,
        destination_key: impl IntoObjectKey,
    ) -> Self {
        Self {
            bucket,
            source_key: source_key.into_object_key(),
            destination_key: destination_key.into_object_key(),
            source_bucket: None,
            source_if_match: None,
            source_if_none_match: None,
            source_if_modified_since: None,
            source_if_unmodified_since: None,
            metadata_directive: None,
            metadata_options: None,
        }
    }

    /// Sets a different source bucket within the same account (cross-bucket copy).
    #[must_use]
    pub fn source_bucket(mut self, value: impl IntoBucketName) -> Self {
        self.source_bucket = Some(value.into_bucket_name());
        self
    }

    /// Sets a condition requiring the source object ETag to match.
    #[must_use]
    pub fn source_if_match(mut self, value: impl Into<String>) -> Self {
        self.source_if_match = Some(value.into());
        self
    }

    /// Sets a condition requiring the source object ETag not to match.
    #[must_use]
    pub fn source_if_none_match(mut self, value: impl Into<String>) -> Self {
        self.source_if_none_match = Some(value.into());
        self
    }

    /// Sets a condition requiring the source object to be modified since the time.
    #[must_use]
    pub fn source_if_modified_since(mut self, value: SystemTime) -> Self {
        self.source_if_modified_since = Some(value);
        self
    }

    /// Sets a condition requiring the source object to be unmodified since the time.
    #[must_use]
    pub fn source_if_unmodified_since(mut self, value: SystemTime) -> Self {
        self.source_if_unmodified_since = Some(value);
        self
    }

    /// Sets the metadata copy directive (`COPY` or `REPLACE`).
    #[must_use]
    pub fn metadata_directive(mut self, value: MetadataDirective) -> Self {
        self.metadata_directive = Some(value);
        self
    }

    /// Sets the replacement metadata when `metadata_directive` is `REPLACE`.
    #[must_use]
    pub fn upload_options(mut self, options: ObjectUploadOptions) -> Self {
        self.metadata_options = Some(options);
        self
    }

    /// Sends the CopyObject request to R2.
    pub async fn send(self) -> Result<CopyObjectResult, Error> {
        let source_key = self.source_key?;
        let destination_key = self.destination_key?;
        let source_bucket = match self.source_bucket {
            Some(res) => Some(res?),
            None => None,
        };
        if let Some(source_if_match) = self.source_if_match.as_deref() {
            validate_metadata_header("source_if_match", source_if_match)?;
        }
        if let Some(source_if_none_match) = self.source_if_none_match.as_deref() {
            validate_metadata_header("source_if_none_match", source_if_none_match)?;
        }
        if let Some(options) = &self.metadata_options {
            options.validate()?;
            if self.metadata_directive == Some(MetadataDirective::Copy) {
                return Err(Error::InvalidInput {
                    field: "metadata_directive",
                    reason: "cannot specify upload_options when metadata_directive is Copy",
                });
            }
            if options.checksum().is_some() {
                return Err(Error::InvalidInput {
                    field: "checksum",
                    reason: "checksum verification is not supported on CopyObject operations",
                });
            }
            if options.if_match().is_some() || options.if_none_match().is_some() {
                return Err(Error::InvalidInput {
                    field: "if_match",
                    reason: "conditional match headers in upload_options are not supported for CopyObject; use source_if_match or source_if_none_match",
                });
            }
        }

        let source_bucket_name = source_bucket.as_ref().unwrap_or(&self.bucket.name);

        let copy_source = format!(
            "{}/{}",
            source_bucket_name.as_str(),
            utf8_percent_encode(source_key.as_str(), COPY_SOURCE_ENCODE_SET)
        );

        let mut req = self
            .bucket
            .client
            .as_sdk()
            .copy_object()
            .bucket(self.bucket.name.as_str())
            .key(destination_key.as_str())
            .copy_source(copy_source);

        if let Some(source_if_match) = self.source_if_match {
            req = req.copy_source_if_match(source_if_match);
        }
        if let Some(source_if_none_match) = self.source_if_none_match {
            req = req.copy_source_if_none_match(source_if_none_match);
        }
        if let Some(source_if_modified_since) = self.source_if_modified_since {
            req = req.copy_source_if_modified_since(DateTime::from(source_if_modified_since));
        }
        if let Some(source_if_unmodified_since) = self.source_if_unmodified_since {
            req = req.copy_source_if_unmodified_since(DateTime::from(source_if_unmodified_since));
        }
        let effective_directive = match self.metadata_directive {
            Some(directive) => directive.as_sdk_directive(),
            None => {
                if self.metadata_options.is_some() {
                    aws_sdk_s3::types::MetadataDirective::Replace
                } else {
                    aws_sdk_s3::types::MetadataDirective::Copy
                }
            }
        };
        req = req.metadata_directive(effective_directive);
        if let Some(options) = self.metadata_options {
            req = options
                .apply_to(req)
                .set_metadata(options.custom_metadata_values());
        }

        let output = req
            .send()
            .await
            .map_err(|error| map_object_error!("CopyObject", error))?;

        let result = output.copy_object_result.ok_or(Error::Service {
            operation: "CopyObject",
        })?;
        let last_modified = optional_system_time(result.last_modified, "CopyObject")?;
        Ok(CopyObjectResult {
            etag: result.e_tag,
            last_modified,
        })
    }
}

impl Bucket {
    /// Presigns a DELETE request for an object.
    pub async fn presign_delete(
        &self,
        key: impl IntoObjectKey,
        expires_in: Duration,
    ) -> Result<PresignedRequest, Error> {
        let key = key.into_object_key()?;
        types::validate_expiry(expires_in)?;
        let config = PresigningConfig::expires_in(expires_in).map_err(|_| Error::Presign)?;
        let req = self
            .client
            .as_sdk()
            .delete_object()
            .bucket(self.name.as_str())
            .key(key)
            .presigned(config)
            .await
            .map_err(|_| Error::Presign)?;
        PresignedRequest::from_sdk(req, expires_in)
    }

    /// Returns a builder for configuring a single-object download with range or conditions.
    #[must_use]
    pub fn get_object(&self, key: impl IntoObjectKey) -> GetObjectBuilder {
        GetObjectBuilder::new(self.clone(), key)
    }

    /// Fetches an entire object directly into memory.
    pub async fn get_bytes(&self, key: impl IntoObjectKey) -> Result<ObjectBytes, Error> {
        let object = self.get_object(key).send().await?;
        let metadata = object.metadata;
        let bytes = object
            .body
            .collect()
            .await
            .map_err(|_| Error::Service {
                operation: "GetObject",
            })?
            .into_bytes();
        Ok(ObjectBytes { metadata, bytes })
    }

    /// Returns a builder for fetching object metadata with conditions.
    #[must_use]
    pub fn head_object(&self, key: impl IntoObjectKey) -> HeadObjectBuilder {
        HeadObjectBuilder::new(self.clone(), key)
    }

    /// Returns a builder for copying an object server-side with conditions or across buckets.
    #[must_use]
    pub fn copy_object(
        &self,
        source_key: impl IntoObjectKey,
        destination_key: impl IntoObjectKey,
    ) -> CopyObjectBuilder {
        CopyObjectBuilder::new(self.clone(), source_key, destination_key)
    }

    /// Creates a temporary signed GET request for one object.
    pub async fn presign_get(
        &self,
        key: impl IntoObjectKey,
        expires_in: Duration,
    ) -> Result<PresignedRequest, Error> {
        let key = key.into_object_key()?;
        types::validate_expiry(expires_in)?;
        let config = PresigningConfig::expires_in(expires_in).map_err(|_| Error::Presign)?;
        let signed = self
            .client
            .as_sdk()
            .get_object()
            .bucket(self.name.as_str())
            .key(key)
            .presigned(config)
            .await
            .map_err(|_| Error::Presign)?;
        PresignedRequest::from_sdk(signed, expires_in)
    }

    /// Creates a temporary signed PUT request for one object.
    pub async fn presign_put(
        &self,
        key: impl IntoObjectKey,
        content_length: u64,
        expires_in: Duration,
    ) -> Result<PresignedPutObject, Error> {
        self.presign_put_with_options(
            key,
            content_length,
            expires_in,
            ObjectUploadOptions::default(),
        )
        .await
    }

    /// Creates a temporary signed PUT with typed object metadata.
    ///
    /// Metadata headers included in the returned request are part of its
    /// signature and must be replayed exactly by the uploader.
    pub async fn presign_put_with_options(
        &self,
        key: impl IntoObjectKey,
        content_length: u64,
        expires_in: Duration,
        options: ObjectUploadOptions,
    ) -> Result<PresignedPutObject, Error> {
        let key = key.into_object_key()?;
        types::validate_expiry(expires_in)?;
        options.validate()?;
        if options.checksum().is_some() && options.checksum_value().is_none() {
            return Err(Error::InvalidInput {
                field: "checksum",
                reason: "auto-computing checksums is only supported for in-memory bytes; provide a precomputed digest using with_checksum_value",
            });
        }
        if content_length > MAX_SINGLE_PUT_SIZE {
            return Err(ValidationError::SingleUploadTooLarge {
                provided: content_length,
                max: MAX_SINGLE_PUT_SIZE,
            }
            .into());
        }
        let config = PresigningConfig::expires_in(expires_in).map_err(|_| Error::Presign)?;
        let mut req = self
            .client
            .as_sdk()
            .put_object()
            .bucket(self.name.as_str())
            .key(key)
            .content_length(
                i64::try_from(content_length).expect("validated single-upload length fits in i64"),
            );
        req = options
            .apply_to(req)
            .set_metadata(options.custom_metadata_values())
            .set_if_match(options.if_match_value())
            .set_if_none_match(options.if_none_match_value());

        if let Some((algorithm, value)) = options.checksum_value() {
            match algorithm {
                ChecksumAlgorithm::Crc32 => req = req.checksum_crc32(value),
                ChecksumAlgorithm::Crc32c => req = req.checksum_crc32_c(value),
                ChecksumAlgorithm::Sha1 => req = req.checksum_sha1(value),
                ChecksumAlgorithm::Sha256 => req = req.checksum_sha256(value),
            }
        }

        let signed = req.presigned(config).await.map_err(|_| Error::Presign)?;
        Ok(PresignedPutObject {
            content_length,
            request: PresignedRequest::from_sdk(signed, expires_in)?,
        })
    }

    /// Creates a coordinated presigned upload plan for an object of known size.
    ///
    /// Automatically selects between a single presigned PUT and a presigned multipart
    /// upload session based on the default threshold ([`crate::UploadThreshold::DEFAULT_BYTES`]).
    pub async fn presign_upload(
        &self,
        key: impl IntoObjectKey,
        file_size: u64,
        expires_in: Duration,
    ) -> Result<PresignedUploadPlan, Error> {
        self.presign_upload_with_options(key, file_size, expires_in, ObjectUploadOptions::default())
            .await
    }

    /// Creates a coordinated presigned upload plan with custom upload options.
    ///
    /// If `file_size` is smaller than the upload threshold (or 0 bytes),
    /// generates a single presigned PUT request ([`PresignedUploadPlan::Single`]).
    /// If `file_size` meets or exceeds the threshold, initiates a presigned multipart upload
    /// ([`PresignedUploadPlan::Multipart`]).
    pub async fn presign_upload_with_options(
        &self,
        key: impl IntoObjectKey,
        file_size: u64,
        expires_in: Duration,
        options: ObjectUploadOptions,
    ) -> Result<PresignedUploadPlan, Error> {
        let key = key.into_object_key()?;
        let threshold = crate::types::UploadThreshold::default().get();
        if file_size < threshold {
            let put = self
                .presign_put_with_options(&key, file_size, expires_in, options)
                .await?;
            Ok(PresignedUploadPlan::Single(put))
        } else {
            let session = self
                .presigned_multipart(&key)?
                .file_size(file_size)
                .part_size(crate::types::UploadThreshold::DEFAULT_BYTES)
                .upload_options(options)
                .create()
                .await?;
            Ok(PresignedUploadPlan::Multipart(
                PresignedMultipartPlan::from_session(session),
            ))
        }
    }

    /// Uploads an in-memory object with a single R2 request.
    pub async fn put_bytes(
        &self,
        key: impl IntoObjectKey,
        bytes: impl Into<Vec<u8>>,
    ) -> Result<PutObjectResult, Error> {
        self.put_bytes_with_options(key, bytes, ObjectUploadOptions::default())
            .await
    }

    /// Uploads in-memory bytes with typed object metadata.
    pub async fn put_bytes_with_options(
        &self,
        key: impl IntoObjectKey,
        bytes: impl Into<Vec<u8>>,
        options: ObjectUploadOptions,
    ) -> Result<PutObjectResult, Error> {
        let bytes = bytes.into();
        let content_length = bytes.len() as u64;

        #[cfg(feature = "checksum")]
        let options = if let (Some(algo), None) = (options.checksum(), options.checksum_value()) {
            let digest = compute_checksum(&bytes, algo);
            options.with_checksum_value(algo, digest)
        } else {
            options
        };

        #[cfg(not(feature = "checksum"))]
        let options = if options.checksum().is_some() && options.checksum_value().is_none() {
            return Err(Error::InvalidInput {
                field: "checksum",
                reason: "auto-computing checksums requires the 'checksum' crate feature",
            });
        } else {
            options
        };

        self.put_stream_with_options(key, ByteStream::from(bytes), content_length, options)
            .await
    }

    /// Uploads a streaming body with a declared byte length using one request.
    ///
    /// The declared length must match the stream exactly. This API keeps the
    /// body out of a single application-owned `Vec`; use managed multipart for
    /// large local files that need bounded parallelism and retries.
    ///
    /// ```no_run
    /// use aws_sdk_s3::primitives::ByteStream;
    /// use r2kit::R2Client;
    ///
    /// # async fn upload() -> Result<(), r2kit::Error> {
    /// let bucket = R2Client::from_env()?.bucket("media")?;
    /// let body = ByteStream::from_static(b"streamed body");
    /// bucket.put_stream("incoming/body.bin", body, 13).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put_stream(
        &self,
        key: impl IntoObjectKey,
        body: ByteStream,
        content_length: u64,
    ) -> Result<PutObjectResult, Error> {
        self.put_stream_with_options(key, body, content_length, ObjectUploadOptions::default())
            .await
    }

    /// Uploads a streaming body with typed object metadata.
    pub async fn put_stream_with_options(
        &self,
        key: impl IntoObjectKey,
        body: ByteStream,
        content_length: u64,
        options: ObjectUploadOptions,
    ) -> Result<PutObjectResult, Error> {
        let key = key.into_object_key()?;
        options.validate()?;
        if options.checksum().is_some() && options.checksum_value().is_none() {
            return Err(Error::InvalidInput {
                field: "checksum",
                reason: "auto-computing checksums is only supported for in-memory bytes; provide a precomputed digest using with_checksum_value",
            });
        }
        if content_length > MAX_SINGLE_PUT_SIZE {
            return Err(ValidationError::SingleUploadTooLarge {
                provided: content_length,
                max: MAX_SINGLE_PUT_SIZE,
            }
            .into());
        }
        let mut req = self
            .client
            .as_sdk()
            .put_object()
            .bucket(self.name.as_str())
            .key(key)
            .content_length(
                i64::try_from(content_length).expect("validated single-upload length fits in i64"),
            );
        req = options
            .apply_to(req)
            .set_metadata(options.custom_metadata_values())
            .set_if_match(options.if_match_value())
            .set_if_none_match(options.if_none_match_value());

        if let Some((algorithm, value)) = options.checksum_value() {
            match algorithm {
                ChecksumAlgorithm::Crc32 => req = req.checksum_crc32(value),
                ChecksumAlgorithm::Crc32c => req = req.checksum_crc32_c(value),
                ChecksumAlgorithm::Sha1 => req = req.checksum_sha1(value),
                ChecksumAlgorithm::Sha256 => req = req.checksum_sha256(value),
            }
        }

        let output = req.body(body).send().await.map_err(|error| {
            if error
                .raw_response()
                .is_some_and(|response| response.status().as_u16() == 412)
            {
                Error::PreconditionFailed
            } else {
                Error::remote("PutObject", &error)
            }
        })?;
        Ok(PutObjectResult { etag: output.e_tag })
    }

    /// Downloads an object as a stream.
    pub async fn get(&self, key: impl IntoObjectKey) -> Result<DownloadedObject, Error> {
        self.get_object(key).send().await
    }

    /// Downloads an object directly to a local file using streaming I/O.
    ///
    /// Creates or truncates the destination file. This keeps the body out of
    /// application memory.
    ///
    /// ```no_run
    /// use r2kit::R2Client;
    ///
    /// # async fn run() -> Result<(), r2kit::Error> {
    /// let bucket = R2Client::from_env()?.bucket("media")?;
    /// let metadata = bucket.download_file("video.mp4", "./video.mp4").await?;
    /// println!("downloaded {} bytes", metadata.size());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn download_file(
        &self,
        key: impl IntoObjectKey,
        path: impl AsRef<std::path::Path>,
    ) -> Result<ObjectMetadata, Error> {
        let downloaded = self.get(key).await?;
        let metadata = downloaded.metadata().clone();
        let mut reader = downloaded.into_body().into_async_read();
        let mut file = tokio::fs::File::create(path.as_ref())
            .await
            .map_err(|_| Error::Io {
                operation: "download_file",
            })?;
        if tokio::io::copy(&mut reader, &mut file).await.is_err() {
            let _ = tokio::fs::remove_file(path.as_ref()).await;
            return Err(Error::Io {
                operation: "download_file",
            });
        }
        if file.sync_all().await.is_err() {
            let _ = tokio::fs::remove_file(path.as_ref()).await;
            return Err(Error::Io {
                operation: "download_file",
            });
        }
        Ok(metadata)
    }

    /// Fetches object metadata without downloading its body.
    pub async fn head(&self, key: impl IntoObjectKey) -> Result<ObjectMetadata, Error> {
        self.head_object(key).send().await
    }

    /// Deletes an object. R2 treats deleting a missing key as success.
    pub async fn delete(&self, key: impl IntoObjectKey) -> Result<(), Error> {
        let key = key.into_object_key()?;
        self.client
            .as_sdk()
            .delete_object()
            .bucket(self.name.as_str())
            .key(key)
            .send()
            .await
            .map_err(|error| Error::remote("DeleteObject", &error))?;
        Ok(())
    }

    /// Aborts an in-progress or orphaned multipart upload directly.
    ///
    /// This method calls S3/R2 `AbortMultipartUpload` without requiring a
    /// [`MultipartSessionSnapshot`](crate::MultipartSessionSnapshot) or prior
    /// knowledge of part sizing or file dimensions.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] if `key` or `upload_id` is empty.
    /// Returns [`Error::NotFound`] if the specified upload or object does not exist.
    /// Returns [`Error::Remote`] on other Cloudflare R2 service failures.
    pub async fn abort_multipart_upload(
        &self,
        key: impl IntoObjectKey,
        upload_id: impl Into<String>,
    ) -> Result<(), Error> {
        let key = key.into_object_key()?;
        let upload_id = upload_id.into();
        if upload_id.trim().is_empty() {
            return Err(Error::InvalidInput {
                field: "upload_id",
                reason: "must not be empty",
            });
        }
        self.client
            .as_sdk()
            .abort_multipart_upload()
            .bucket(self.name())
            .key(key.as_str())
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|err| Error::from_sdk("AbortMultipartUpload", &err))?;
        Ok(())
    }

    /// Copies an object within this bucket without downloading its body.
    ///
    /// R2 performs the copy server-side and preserves the source metadata. Both
    /// keys are validated before the request is sent. Use [`Bucket::copy_object`]
    /// for cross-bucket copying, conditional copies, or metadata replacement.
    ///
    /// ```no_run
    /// use r2kit::R2Client;
    ///
    /// # async fn copy() -> Result<(), r2kit::Error> {
    /// let bucket = R2Client::from_env()?.bucket("media")?;
    /// let copied = bucket.copy("original.jpg", "archive/original.jpg").await?;
    /// println!("copied ETag: {:?}", copied.etag());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn copy(
        &self,
        source_key: impl IntoObjectKey,
        destination_key: impl IntoObjectKey,
    ) -> Result<CopyObjectResult, Error> {
        self.copy_object(source_key, destination_key).send().await
    }

    /// Deletes arbitrary many keys in sequential batches of at most 1,000.
    ///
    /// Every key is validated before the first remote mutation. An HTTP-successful
    /// response can still contain per-key failures, which are returned in the
    /// result. If a later request fails, [`BatchDeleteError::partial_result`]
    /// retains outcomes from all earlier batches.
    pub async fn delete_objects<I, K>(
        &self,
        keys: I,
    ) -> Result<DeleteObjectsResult, BatchDeleteError>
    where
        I: IntoIterator<Item = K>,
        K: IntoObjectKey,
    {
        let mut object_keys = Vec::new();
        for item in keys {
            let key = item.into_object_key().map_err(|error| BatchDeleteError {
                error,
                partial: DeleteObjectsResult::default(),
            })?;
            object_keys.push(key.into_inner());
        }
        let keys = object_keys;

        let mut result = DeleteObjectsResult::default();
        for keys in delete_batches(&keys) {
            let objects = keys
                .iter()
                .map(|key| {
                    ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .expect("validated object identifiers contain keys")
                })
                .collect();
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .quiet(false)
                .build()
                .expect("each delete batch contains at least one key");
            let output = self
                .client
                .as_sdk()
                .delete_objects()
                .bucket(self.name.as_str())
                .delete(delete)
                .send()
                .await
                .map_err(|error| BatchDeleteError {
                    error: Error::remote("DeleteObjects", &error),
                    partial: result.clone(),
                })?;
            result.request_count += 1;
            result.deleted_keys.extend(
                output
                    .deleted()
                    .iter()
                    .filter_map(|deleted| deleted.key().map(ToOwned::to_owned)),
            );
            for failure in output.errors() {
                let key = failure.key().ok_or_else(|| BatchDeleteError {
                    error: Error::Service {
                        operation: "DeleteObjects",
                    },
                    partial: result.clone(),
                })?;
                result.failures.push(DeleteObjectFailure {
                    key: key.to_owned(),
                    code: failure.code().map(ToOwned::to_owned),
                    message: failure.message().map(ToOwned::to_owned),
                });
            }
        }
        Ok(result)
    }

    /// Starts a bounded, paginated object listing.
    #[must_use]
    pub fn list(&self) -> ListObjectsBuilder {
        ListObjectsBuilder {
            bucket: self.clone(),
            prefix: None,
            delimiter: None,
            limit: MAX_LIST_KEYS,
            continuation_token: None,
        }
    }
}

fn delete_batches(keys: &[String]) -> impl Iterator<Item = &[String]> {
    keys.chunks(MAX_DELETE_KEYS)
}

fn non_negative_size(value: Option<i64>, operation: &'static str) -> Result<u64, Error> {
    value
        .and_then(|size| u64::try_from(size).ok())
        .ok_or(Error::Service { operation })
}

fn optional_system_time(
    value: Option<DateTime>,
    operation: &'static str,
) -> Result<Option<SystemTime>, Error> {
    value
        .map(SystemTime::try_from)
        .transpose()
        .map_err(|_| Error::Service { operation })
}

fn optional_http_date(
    value: Option<&str>,
    operation: &'static str,
) -> Result<Option<SystemTime>, Error> {
    value
        .map(|value| DateTime::from_str(value, DateTimeFormat::HttpDate))
        .transpose()
        .map_err(|_| Error::Service { operation })?
        .map(SystemTime::try_from)
        .transpose()
        .map_err(|_| Error::Service { operation })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_reported_sizes() {
        assert!(non_negative_size(None, "test").is_err());
        assert!(non_negative_size(Some(-1), "test").is_err());
        assert_eq!(non_negative_size(Some(0), "test").unwrap(), 0);
    }

    #[test]
    fn delete_batches_never_exceed_r2s_request_limit() {
        let keys = (0..2_001)
            .map(|index| index.to_string())
            .collect::<Vec<_>>();
        let lengths = delete_batches(&keys).map(<[_]>::len).collect::<Vec<_>>();

        assert_eq!(lengths, [1_000, 1_000, 1]);
    }

    #[test]
    fn upload_metadata_validation_is_case_insensitive_and_bounded() {
        let duplicate = ObjectUploadOptions::new()
            .with_custom_metadata("a", "b")
            .with_custom_metadata("A", "c");
        assert!(duplicate.validate().is_err());

        let too_large = ObjectUploadOptions::new()
            .with_custom_metadata("large", "x".repeat(MAX_OBJECT_METADATA_BYTES));
        assert!(too_large.validate().is_err());
    }

    #[test]
    fn content_language_accepts_bcp47_lists_and_rejects_malformed_tags() {
        for valid in ["en", "vi", "en-US", "zh-Hant", "x-private", "en, vi"] {
            assert!(validate_content_language(valid).is_ok(), "{valid}");
        }
        for invalid in ["", "en_US", "en--US", "en,", ",vi", "abc ???"] {
            assert!(validate_content_language(invalid).is_err(), "{invalid}");
        }
    }
}
