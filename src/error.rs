use std::{fmt, time::Duration};

use aws_sdk_s3::error::SdkError;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;

/// A configuration value required to create an R2 client was absent or invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// A required configuration field was not supplied.
    MissingField(&'static str),
    /// The Cloudflare account ID was not a 32-character hexadecimal identifier.
    InvalidAccountId,
    /// A supplied configuration field was empty.
    EmptyField(&'static str),
    /// An environment variable could not be represented as Unicode.
    InvalidEnvironment(&'static str),
    /// The configured R2 jurisdiction was not recognized.
    InvalidJurisdiction,
    /// A timeout was zero and would make requests fail immediately.
    InvalidTimeout(&'static str),
    /// An attempt limit was zero.
    InvalidAttempts(&'static str),
    /// The per-attempt timeout exceeded the total operation timeout.
    InconsistentTimeouts,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField(field) => write!(f, "missing required R2 configuration: {field}"),
            Self::InvalidAccountId => {
                write!(
                    f,
                    "R2 account ID must be a 32-character hexadecimal identifier"
                )
            }
            Self::EmptyField(field) => write!(f, "R2 configuration must not be empty: {field}"),
            Self::InvalidEnvironment(field) => {
                write!(f, "R2 environment variable is not valid Unicode: {field}")
            }
            Self::InvalidJurisdiction => write!(
                f,
                "R2 jurisdiction must be one of: default, eu, us, fedramp"
            ),
            Self::InvalidTimeout(field) => {
                write!(f, "R2 timeout must be greater than zero: {field}")
            }
            Self::InvalidAttempts(field) => {
                write!(f, "R2 attempt limit must be at least one: {field}")
            }
            Self::InconsistentTimeouts => write!(
                f,
                "R2 operation_attempt_timeout must not exceed operation_timeout"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

/// A machine-readable failure of caller-provided numeric limits.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ValidationError {
    /// A multipart part size was outside R2's supported range.
    PartSizeOutOfRange {
        /// Supplied size in bytes.
        provided: u64,
        /// Minimum size in bytes.
        min: u64,
        /// Maximum size in bytes.
        max: u64,
    },
    /// A multipart upload was configured with an empty object.
    MultipartFileSizeZero,
    /// A multipart object exceeded R2's effective object limit.
    MultipartObjectTooLarge {
        /// Supplied object size in bytes.
        provided: u64,
        /// Maximum object size in bytes.
        max: u64,
    },
    /// A multipart plan required more parts than R2 accepts.
    TooManyParts {
        /// Number of parts required by the plan.
        required: u64,
        /// Maximum accepted number of parts.
        max: u16,
    },
    /// A multipart part number was outside R2's supported range.
    PartNumberOutOfRange {
        /// Supplied part number.
        provided: u16,
        /// Minimum accepted part number.
        min: u16,
        /// Maximum accepted part number.
        max: u16,
    },
    /// Managed-upload concurrency was outside r2kit's safety bound.
    ConcurrencyOutOfRange {
        /// Supplied concurrency.
        provided: usize,
        /// Minimum accepted concurrency.
        min: usize,
        /// Maximum accepted concurrency.
        max: usize,
    },
    /// Managed-upload attempts were outside r2kit's safety bound.
    AttemptsOutOfRange {
        /// Supplied total attempt count.
        provided: u8,
        /// Minimum accepted total attempt count.
        min: u8,
        /// Maximum accepted total attempt count.
        max: u8,
    },
    /// A managed upload could buffer more bytes than its configured budget.
    ManagedMemoryBudgetExceeded {
        /// Maximum bytes required by the configured part size and concurrency.
        required: u64,
        /// Maximum bytes the caller allowed r2kit to buffer for parts.
        max: u64,
    },
    /// A single-request upload exceeded R2's maximum size.
    SingleUploadTooLarge {
        /// Supplied content length in bytes.
        provided: u64,
        /// Maximum content length in bytes.
        max: u64,
    },
    /// A list page limit was outside R2's accepted range.
    ListLimitOutOfRange {
        /// Supplied page limit.
        provided: u16,
        /// Minimum accepted page limit.
        min: u16,
        /// Maximum accepted page limit.
        max: u16,
    },
    /// A presigned request lifetime was outside the supported range.
    PresignExpiryOutOfRange {
        /// Supplied lifetime.
        provided: Duration,
        /// Minimum accepted lifetime.
        min: Duration,
        /// Maximum accepted lifetime.
        max: Duration,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartSizeOutOfRange { provided, min, max } => write!(
                f,
                "multipart part size {provided} bytes is outside the allowed range {min}..={max} bytes"
            ),
            Self::MultipartFileSizeZero => {
                write!(f, "multipart file size must be greater than zero")
            }
            Self::MultipartObjectTooLarge { provided, max } => write!(
                f,
                "multipart object size {provided} bytes exceeds the maximum {max} bytes"
            ),
            Self::TooManyParts { required, max } => write!(
                f,
                "multipart plan requires {required} parts, exceeding the maximum {max}"
            ),
            Self::PartNumberOutOfRange { provided, min, max } => write!(
                f,
                "multipart part number {provided} is outside the allowed range {min}..={max}"
            ),
            Self::ConcurrencyOutOfRange { provided, min, max } => write!(
                f,
                "managed upload concurrency {provided} is outside the allowed range {min}..={max}"
            ),
            Self::AttemptsOutOfRange { provided, min, max } => write!(
                f,
                "managed upload attempts {provided} is outside the allowed range {min}..={max}"
            ),
            Self::ManagedMemoryBudgetExceeded { required, max } => write!(
                f,
                "managed upload may buffer {required} bytes, exceeding the configured memory budget of {max} bytes"
            ),
            Self::SingleUploadTooLarge { provided, max } => write!(
                f,
                "single-request upload size {provided} bytes exceeds the maximum {max} bytes"
            ),
            Self::ListLimitOutOfRange { provided, min, max } => write!(
                f,
                "list limit {provided} is outside the allowed range {min}..={max}"
            ),
            Self::PresignExpiryOutOfRange { provided, min, max } => write!(
                f,
                "presigned request lifetime {provided:?} is outside the allowed range {min:?}..={max:?}"
            ),
        }
    }
}

impl std::error::Error for ValidationError {}

/// Sanitized category of an AWS SDK or Cloudflare R2 request failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServiceErrorKind {
    /// Credentials were missing, invalid, or expired.
    Authentication,
    /// Credentials were valid but did not authorize the operation.
    PermissionDenied,
    /// The requested remote resource was not found.
    NotFound,
    /// The request conflicted with current remote state.
    Conflict,
    /// R2 rate-limited the request.
    RateLimited,
    /// A request or operation timed out.
    Timeout,
    /// The request could not reach R2.
    Network,
    /// R2 returned a transient server failure.
    Unavailable,
    /// R2 returned a response that the SDK could not process.
    InvalidResponse,
    /// The failure did not fit a stable, non-sensitive category.
    Unknown,
}

impl ServiceErrorKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Authentication => "authentication failed",
            Self::PermissionDenied => "permission denied",
            Self::NotFound => "remote resource not found",
            Self::Conflict => "remote state conflict",
            Self::RateLimited => "rate limited",
            Self::Timeout => "request timed out",
            Self::Network => "network failure",
            Self::Unavailable => "service unavailable",
            Self::InvalidResponse => "invalid service response",
            Self::Unknown => "unknown service failure",
        }
    }

    const fn from_status(status: u16) -> Self {
        match status {
            401 => Self::Authentication,
            403 => Self::PermissionDenied,
            404 => Self::NotFound,
            409 => Self::Conflict,
            408 => Self::Timeout,
            429 => Self::RateLimited,
            500..=599 => Self::Unavailable,
            _ => Self::Unknown,
        }
    }

    fn from_code(code: &str) -> Option<Self> {
        match code {
            "Unauthorized" | "ExpiredRequest" | "SignatureDoesNotMatch" => {
                Some(Self::Authentication)
            }
            "AccessDenied" | "NotEntitled" | "ObjectLockedByBucketPolicy" => {
                Some(Self::PermissionDenied)
            }
            "NoSuchBucket" | "NoSuchKey" | "NoSuchUpload" => Some(Self::NotFound),
            "BucketConflict" | "BucketNotEmpty" | "PreconditionFailed" => Some(Self::Conflict),
            "TooManyRequests" | "SlowDown" => Some(Self::RateLimited),
            "InternalError" | "ServiceUnavailable" => Some(Self::Unavailable),
            _ => None,
        }
    }
}

impl fmt::Display for ServiceErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A sanitized remote failure that never exposes an SDK error or signed request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError {
    operation: &'static str,
    kind: ServiceErrorKind,
    status: Option<u16>,
}

impl ServiceError {
    /// Returns the stable AWS S3 operation name.
    #[must_use]
    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    /// Returns the sanitized failure category.
    #[must_use]
    pub const fn kind(&self) -> ServiceErrorKind {
        self.kind
    }

    /// Returns the HTTP response status when R2 supplied one.
    #[must_use]
    pub const fn status(&self) -> Option<u16> {
        self.status
    }

    fn from_sdk<E: ProvideErrorMetadata>(operation: &'static str, error: &SdkError<E>) -> Self {
        let status = error
            .raw_response()
            .map(|response| response.status().as_u16());
        let service_code = match error {
            SdkError::ServiceError(context) => context.err().code(),
            _ => None,
        };
        let kind = service_code
            .and_then(ServiceErrorKind::from_code)
            .or_else(|| status.map(ServiceErrorKind::from_status))
            .unwrap_or(match error {
                SdkError::TimeoutError(_) => ServiceErrorKind::Timeout,
                SdkError::DispatchFailure(_) => ServiceErrorKind::Network,
                SdkError::ResponseError(_) => ServiceErrorKind::InvalidResponse,
                _ => ServiceErrorKind::Unknown,
            });
        Self {
            operation,
            kind,
            status,
        }
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "R2 {} failed: {}", self.operation, self.kind)?;
        if let Some(status) = self.status {
            write!(f, " (HTTP {status})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ServiceError {}

/// An error returned by an `r2kit` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Client configuration was invalid.
    Config(ConfigError),
    /// A caller-provided numeric limit was invalid.
    Validation(ValidationError),
    /// A caller-provided value was invalid.
    InvalidInput {
        /// Name of the invalid field.
        field: &'static str,
        /// Stable, non-sensitive explanation.
        reason: &'static str,
    },
    /// An R2 S3 operation failed. The generated SDK error is intentionally not
    /// formatted because it can contain signed request details.
    Service {
        /// Name of the operation that failed.
        operation: &'static str,
    },
    /// A network or Cloudflare R2 request failed.
    Remote(ServiceError),
    /// The requested R2 object does not exist.
    NotFound,
    /// The conditional request's precondition was not met (HTTP 412).
    PreconditionFailed,
    /// The object has not been modified since the condition (HTTP 304).
    NotModified,
    /// A local file operation failed. Paths are intentionally omitted.
    Io {
        /// Stable name of the file operation that failed.
        operation: &'static str,
    },
    /// A caller explicitly cancelled an in-progress managed upload.
    Cancelled,
    /// Presigning failed before a request was sent.
    Presign,
    /// A signed header could not be represented as text.
    InvalidSignedHeader,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => error.fmt(f),
            Self::Validation(error) => error.fmt(f),
            Self::InvalidInput { field, reason } => write!(f, "invalid {field}: {reason}"),
            Self::Service { operation } => write!(f, "R2 operation failed: {operation}"),
            Self::Remote(error) => error.fmt(f),
            Self::NotFound => write!(f, "R2 object was not found"),
            Self::PreconditionFailed => write!(f, "R2 precondition was not met"),
            Self::NotModified => write!(f, "R2 object was not modified"),
            Self::Io { operation } => write!(f, "local file operation failed: {operation}"),
            Self::Cancelled => write!(f, "managed upload was cancelled"),
            Self::Presign => write!(f, "failed to create a presigned R2 request"),
            Self::InvalidSignedHeader => write!(f, "presigned request contains a non-text header"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Validation(error) => Some(error),
            Self::Remote(error) => Some(error),
            _ => None,
        }
    }
}

impl Error {
    /// Returns `true` if this error represents a missing remote resource (such as HTTP 404
    /// or `NoSuchKey`/`NoSuchUpload`).
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::NotFound => true,
            Self::Remote(se) => se.kind() == ServiceErrorKind::NotFound,
            _ => false,
        }
    }

    pub(crate) fn remote<E: ProvideErrorMetadata>(
        operation: &'static str,
        error: &SdkError<E>,
    ) -> Self {
        let error = ServiceError::from_sdk(operation, error);
        crate::observability::remote_failure(&error);
        Self::Remote(error)
    }

    pub(crate) fn from_sdk<E: ProvideErrorMetadata>(
        operation: &'static str,
        error: &SdkError<E>,
    ) -> Self {
        if error
            .raw_response()
            .is_some_and(|response| response.status().as_u16() == 404)
        {
            Self::NotFound
        } else {
            Self::remote(operation, error)
        }
    }
}

impl From<ConfigError> for Error {
    fn from(value: ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<ValidationError> for Error {
    fn from(value: ValidationError) -> Self {
        Self::Validation(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_http_status_without_exposing_response_details() {
        let cases = [
            (401, ServiceErrorKind::Authentication),
            (403, ServiceErrorKind::PermissionDenied),
            (404, ServiceErrorKind::NotFound),
            (408, ServiceErrorKind::Timeout),
            (409, ServiceErrorKind::Conflict),
            (429, ServiceErrorKind::RateLimited),
            (503, ServiceErrorKind::Unavailable),
            (418, ServiceErrorKind::Unknown),
        ];
        for (status, expected) in cases {
            assert_eq!(ServiceErrorKind::from_status(status), expected);
        }
    }

    #[test]
    fn classifies_documented_r2_error_codes_before_falling_back_to_status() {
        let cases = [
            ("SignatureDoesNotMatch", ServiceErrorKind::Authentication),
            ("AccessDenied", ServiceErrorKind::PermissionDenied),
            ("NoSuchUpload", ServiceErrorKind::NotFound),
            ("PreconditionFailed", ServiceErrorKind::Conflict),
            ("TooManyRequests", ServiceErrorKind::RateLimited),
            ("ServiceUnavailable", ServiceErrorKind::Unavailable),
        ];
        for (code, expected) in cases {
            assert_eq!(ServiceErrorKind::from_code(code), Some(expected));
        }
        assert_eq!(ServiceErrorKind::from_code("FutureR2Code"), None);
    }

    #[test]
    fn classifies_sdk_timeout_without_formatting_its_source() {
        let sdk_error: SdkError<aws_sdk_s3::operation::get_object::GetObjectError> =
            SdkError::timeout_error(std::io::Error::other("sensitive-source"));
        let error = ServiceError::from_sdk("GetObject", &sdk_error);

        assert_eq!(error.kind(), ServiceErrorKind::Timeout);
        assert_eq!(error.status(), None);
        assert!(!format!("{error}").contains("sensitive-source"));
        assert!(!format!("{error:?}").contains("sensitive-source"));
    }

    #[test]
    fn is_not_found_identifies_not_found_variants() {
        assert!(Error::NotFound.is_not_found());

        let service_err = ServiceError {
            operation: "GetObject",
            kind: ServiceErrorKind::NotFound,
            status: Some(404),
        };
        assert!(Error::Remote(service_err).is_not_found());

        let auth_err = ServiceError {
            operation: "GetObject",
            kind: ServiceErrorKind::Authentication,
            status: Some(401),
        };
        assert!(!Error::Remote(auth_err).is_not_found());
        assert!(!Error::PreconditionFailed.is_not_found());
    }
}
