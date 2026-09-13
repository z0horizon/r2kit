use std::{
    borrow::{Borrow, Cow},
    fmt,
    ops::Deref,
    str::FromStr,
    time::Duration,
};

use crate::{Error, ValidationError};

pub(crate) const MAX_KEY_BYTES: usize = 1_024;
pub(crate) const MIN_BUCKET_NAME_LEN: usize = 3;
pub(crate) const MAX_BUCKET_NAME_LEN: usize = 63;

// R2's platform limit is 5 MiB below 5 GiB for a single request/part.
// https://developers.cloudflare.com/r2/platform/limits/
pub(crate) const MIN_MULTIPART_PART_SIZE: u64 = 5 * 1024 * 1024;
pub(crate) const MAX_UPLOAD_SIZE: u64 = 5 * 1024 * 1024 * 1024 - MIN_MULTIPART_PART_SIZE;
pub(crate) const MAX_MULTIPART_PART_SIZE: u64 = MAX_UPLOAD_SIZE;
pub(crate) const MAX_MULTIPART_OBJECT_SIZE: u64 =
    5 * 1024 * 1024 * 1024 * 1024 - 5 * 1024 * 1024 * 1024;
pub(crate) const MAX_PRESIGN_SECONDS: u64 = 7 * 24 * 60 * 60;

pub(crate) const fn mebibytes(value: u64) -> u64 {
    match value.checked_mul(1024 * 1024) {
        Some(bytes) => bytes,
        None => u64::MAX,
    }
}

pub(crate) fn validate_prefix(prefix: &str) -> Result<(), Error> {
    if prefix.len() > MAX_KEY_BYTES {
        return Err(Error::InvalidInput {
            field: "prefix",
            reason: "must not exceed 1,024 UTF-8 bytes",
        });
    }
    Ok(())
}

pub(crate) fn validate_expiry(expires_in: Duration) -> Result<(), Error> {
    if expires_in < Duration::from_secs(1) || expires_in > Duration::from_secs(MAX_PRESIGN_SECONDS)
    {
        return Err(ValidationError::PresignExpiryOutOfRange {
            provided: expires_in,
            min: Duration::from_secs(1),
            max: Duration::from_secs(MAX_PRESIGN_SECONDS),
        }
        .into());
    }
    Ok(())
}

pub(crate) fn validate_part_size(part_size: u64) -> Result<(), Error> {
    if !(MIN_MULTIPART_PART_SIZE..=MAX_MULTIPART_PART_SIZE).contains(&part_size) {
        return Err(ValidationError::PartSizeOutOfRange {
            provided: part_size,
            min: MIN_MULTIPART_PART_SIZE,
            max: MAX_MULTIPART_PART_SIZE,
        }
        .into());
    }
    Ok(())
}

/// A validated Cloudflare R2 object key.
///
/// An object key must contain between 1 and 1,024 UTF-8 bytes.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Creates and validates a new [`ObjectKey`].
    pub fn new(key: impl Into<String>) -> Result<Self, Error> {
        let key = key.into();
        validate_key_str(&key)?;
        Ok(Self(key))
    }

    /// Returns the object key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the [`ObjectKey`], returning the inner [`String`].
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

fn validate_key_str(key: &str) -> Result<(), Error> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(Error::InvalidInput {
            field: "key",
            reason: "must contain between 1 and 1,024 UTF-8 bytes",
        });
    }
    Ok(())
}

impl Deref for ObjectKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for ObjectKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ObjectKey").field(&self.0).finish()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for ObjectKey {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<String> for ObjectKey {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl FromStr for ObjectKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<ObjectKey> for String {
    fn from(key: ObjectKey) -> Self {
        key.0
    }
}

impl From<&ObjectKey> for String {
    fn from(key: &ObjectKey) -> Self {
        key.0.clone()
    }
}

/// A validated Cloudflare R2 bucket name.
///
/// A bucket name must contain between 3 and 63 bytes, using lowercase ASCII letters,
/// digits, or interior hyphens (cannot start or end with a hyphen).
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
pub struct BucketName(String);

impl BucketName {
    /// Creates and validates a new [`BucketName`].
    pub fn new(name: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        validate_bucket_name_str(&name)?;
        Ok(Self(name))
    }

    /// Returns the bucket name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the [`BucketName`], returning the inner [`String`].
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

fn validate_bucket_name_str(name: &str) -> Result<(), Error> {
    if name.len() < MIN_BUCKET_NAME_LEN || name.len() > MAX_BUCKET_NAME_LEN {
        return Err(Error::InvalidInput {
            field: "bucket",
            reason: "must contain between 3 and 63 bytes",
        });
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || name.starts_with('-')
        || name.ends_with('-')
    {
        return Err(Error::InvalidInput {
            field: "bucket",
            reason: "must use lowercase ASCII letters, digits, or interior hyphens",
        });
    }
    Ok(())
}

impl Deref for BucketName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for BucketName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for BucketName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BucketName").field(&self.0).finish()
    }
}

impl fmt::Display for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for BucketName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<String> for BucketName {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl FromStr for BucketName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<BucketName> for String {
    fn from(name: BucketName) -> Self {
        name.0
    }
}

/// A validated threshold for selecting single PUT vs multipart upload.
///
/// Must be at least [`UploadThreshold::MIN_BYTES`] (5 MiB).
/// Defaults to [`UploadThreshold::DEFAULT_BYTES`] (8 MiB).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u64", into = "u64"))]
pub struct UploadThreshold(u64);

impl UploadThreshold {
    /// Minimum upload threshold in bytes (5 MiB), matching R2's minimum multipart part size.
    pub const MIN_BYTES: u64 = 5 * 1024 * 1024;
    /// Default upload threshold in bytes (8 MiB).
    pub const DEFAULT_BYTES: u64 = 8 * 1024 * 1024;

    /// Creates and validates a new [`UploadThreshold`].
    ///
    /// # Errors
    /// Returns [`ValidationError::PartSizeOutOfRange`] if `bytes` is less than [`Self::MIN_BYTES`]
    /// or exceeds the maximum multipart object limit.
    pub fn new(bytes: u64) -> Result<Self, ValidationError> {
        if !(Self::MIN_BYTES..=MAX_MULTIPART_OBJECT_SIZE).contains(&bytes) {
            return Err(ValidationError::PartSizeOutOfRange {
                provided: bytes,
                min: Self::MIN_BYTES,
                max: MAX_MULTIPART_OBJECT_SIZE,
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the threshold value in bytes.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl Default for UploadThreshold {
    fn default() -> Self {
        Self(Self::DEFAULT_BYTES)
    }
}

impl fmt::Display for UploadThreshold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<u64> for UploadThreshold {
    type Error = ValidationError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<UploadThreshold> for u64 {
    fn from(threshold: UploadThreshold) -> Self {
        threshold.get()
    }
}

impl From<&BucketName> for String {
    fn from(name: &BucketName) -> Self {
        name.0.clone()
    }
}

/// Types that can be converted into a validated [`ObjectKey`].
pub trait IntoObjectKey {
    /// Attempts conversion into a validated [`ObjectKey`].
    fn into_object_key(self) -> Result<ObjectKey, Error>;
}

impl IntoObjectKey for ObjectKey {
    fn into_object_key(self) -> Result<ObjectKey, Error> {
        Ok(self)
    }
}

impl IntoObjectKey for &ObjectKey {
    fn into_object_key(self) -> Result<ObjectKey, Error> {
        Ok(self.clone())
    }
}

impl IntoObjectKey for &str {
    fn into_object_key(self) -> Result<ObjectKey, Error> {
        ObjectKey::new(self)
    }
}

impl IntoObjectKey for String {
    fn into_object_key(self) -> Result<ObjectKey, Error> {
        ObjectKey::new(self)
    }
}

impl IntoObjectKey for &String {
    fn into_object_key(self) -> Result<ObjectKey, Error> {
        ObjectKey::new(self.as_str())
    }
}

impl IntoObjectKey for Cow<'_, str> {
    fn into_object_key(self) -> Result<ObjectKey, Error> {
        ObjectKey::new(self.into_owned())
    }
}

/// Types that can be converted into a validated [`BucketName`].
pub trait IntoBucketName {
    /// Attempts conversion into a validated [`BucketName`].
    fn into_bucket_name(self) -> Result<BucketName, Error>;
}

impl IntoBucketName for BucketName {
    fn into_bucket_name(self) -> Result<BucketName, Error> {
        Ok(self)
    }
}

impl IntoBucketName for &BucketName {
    fn into_bucket_name(self) -> Result<BucketName, Error> {
        Ok(self.clone())
    }
}

impl IntoBucketName for &str {
    fn into_bucket_name(self) -> Result<BucketName, Error> {
        BucketName::new(self)
    }
}

impl IntoBucketName for String {
    fn into_bucket_name(self) -> Result<BucketName, Error> {
        BucketName::new(self)
    }
}

impl IntoBucketName for &String {
    fn into_bucket_name(self) -> Result<BucketName, Error> {
        BucketName::new(self.as_str())
    }
}

impl IntoBucketName for Cow<'_, str> {
    fn into_bucket_name(self) -> Result<BucketName, Error> {
        BucketName::new(self.into_owned())
    }
}

/// Types that can be converted into a validated MIME media type.
pub trait IntoContentType {
    /// Attempts conversion into a validated [`mime::Mime`].
    fn into_content_type(self) -> Result<mime::Mime, Error>;
}

impl IntoContentType for mime::Mime {
    fn into_content_type(self) -> Result<mime::Mime, Error> {
        Ok(self)
    }
}

impl IntoContentType for &mime::Mime {
    fn into_content_type(self) -> Result<mime::Mime, Error> {
        Ok(self.clone())
    }
}

impl IntoContentType for &str {
    fn into_content_type(self) -> Result<mime::Mime, Error> {
        self.parse::<mime::Mime>().map_err(|_| Error::InvalidInput {
            field: "content_type",
            reason: "must be a valid MIME media type",
        })
    }
}

impl IntoContentType for String {
    fn into_content_type(self) -> Result<mime::Mime, Error> {
        self.as_str().into_content_type()
    }
}

impl IntoContentType for &String {
    fn into_content_type(self) -> Result<mime::Mime, Error> {
        self.as_str().into_content_type()
    }
}

impl IntoContentType for Cow<'_, str> {
    fn into_content_type(self) -> Result<mime::Mime, Error> {
        self.as_ref().into_content_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_validates_length_bounds() {
        assert!(ObjectKey::new("").is_err());
        assert!(ObjectKey::new("a").is_ok());
        assert!(ObjectKey::new("a".repeat(1_024)).is_ok());
        assert!(ObjectKey::new("a".repeat(1_025)).is_err());
    }

    #[test]
    fn bucket_name_validates_rfc1035_and_r2_invariants() {
        assert!(BucketName::new("ab").is_err());
        assert!(BucketName::new("abc").is_ok());
        assert!(BucketName::new("my-bucket-123").is_ok());
        assert!(BucketName::new("-starts-with-hyphen").is_err());
        assert!(BucketName::new("ends-with-hyphen-").is_err());
        assert!(BucketName::new("UPPERCASE").is_err());
        assert!(BucketName::new("a".repeat(63)).is_ok());
        assert!(BucketName::new("a".repeat(64)).is_err());
    }

    #[test]
    fn into_traits_convert_smoothly() {
        let key_str = "hello.txt";
        let key = key_str.into_object_key().unwrap();
        assert_eq!(key.as_str(), "hello.txt");
        assert_eq!((&key).into_object_key().unwrap(), key);

        let bucket_str = "my-bucket";
        let b = bucket_str.into_bucket_name().unwrap();
        assert_eq!(b.as_str(), "my-bucket");
        assert_eq!((&b).into_bucket_name().unwrap(), b);
    }
}
