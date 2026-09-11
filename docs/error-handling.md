# Error handling cookbook

`r2kit::Error` separates invalid local input from sanitized remote failures.
Raw SDK errors are deliberately not retained because they can contain signed
request details. Error messages are safe to log, but bucket names, object keys,
upload IDs, and presigned URLs should still be treated as application data.

## Decide whether to retry

Retry only transient remote categories. The AWS SDK already retries ordinary
requests according to `R2Config::sdk_max_attempts`; application retries should
therefore wrap a complete idempotent workflow rather than blindly repeat every
request.

```rust,no_run
use r2kit::{Error, ServiceErrorKind};

fn retryable(error: &Error) -> bool {
    matches!(
        error,
        Error::Remote(remote)
            if matches!(
                remote.kind(),
                ServiceErrorKind::Network
                    | ServiceErrorKind::Timeout
                    | ServiceErrorKind::RateLimited
                    | ServiceErrorKind::Unavailable
            )
    )
}
```

Do not retry authentication, permission, validation, or malformed-response
errors without changing credentials, permissions, input, or configuration.
Managed multipart uploads implement their own bounded retry policy and expose a
session snapshot through `ManagedUploadError` when recovery remains possible.

## Map errors in an HTTP service

Suggested mappings are intentionally conservative:

| r2kit error | HTTP response |
|---|---:|
| `Error::InvalidInput` or `Error::Validation` | `400 Bad Request` |
| `Error::NotFound` or remote `NotFound` | `404 Not Found` |
| `Error::NotModified` | `304 Not Modified` |
| `Error::PreconditionFailed` | `412 Precondition Failed` |
| remote `Authentication` | `502 Bad Gateway` |
| remote `PermissionDenied` | `502 Bad Gateway` |
| remote `RateLimited` | `503 Service Unavailable` |
| remote `Network`, `Timeout`, or `Unavailable` | `503 Service Unavailable` |
| `Error::Cancelled` | `499` or an application-specific cancellation result |
| remaining failures | `500 Internal Server Error` |

Avoid returning upstream authentication details to an untrusted caller. A
credential failure normally describes the server's R2 integration, not the end
user's authorization.

## Preserve partial progress

`BatchDeleteError::partial_result` reports batches completed before a later
request failed. `ManagedUploadError::snapshot` can be persisted and supplied to
`Bucket::resume_managed_multipart`. Inspect these values before converting the
error into a generic application error.
