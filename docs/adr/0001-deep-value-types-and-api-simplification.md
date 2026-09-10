# Deep Value Types and Single-Path Options

We replaced raw string parameters with deep, validated value types (`ObjectKey`, `BucketName`) and unified object creation options under `ObjectUploadOptions` (eliminating `ObjectUploadOptionsBuilder`). This enforces R2 storage invariants offline at the boundary, prevents primitive obsession across the API, and removes redundant builder wrappers without sacrificing ergonomic call sites.

## Status

Accepted

## Considered Options

- **Raw strings everywhere (`impl Into<String>`):** Rejected because validation errors were deferred to runtime AWS SDK requests, keys and bucket names could not be reasoned about safely in domain logic, and validation logic was duplicated across multiple call sites.
- **Dual-builder pattern (`ObjectUploadOptionsBuilder` alongside `ObjectUploadOptions::with_*`):** Rejected because `ObjectUploadOptionsBuilder::build()` performed no invariant enforcement (validation occurs at network boundary via `.validate()`), creating duplicate boilerplate with no distinct lifecycle benefit.
- **Removing conversion traits (`IntoObjectKey`, `IntoBucketName`):** Rejected because requiring callers to write `ObjectKey::new("file.txt")?` at every `bucket.get()` or `bucket.delete()` call site severely harmed daily ergonomics.

## Consequences

- All public bucket and object transfer operations require types implementing `IntoObjectKey` or `IntoBucketName`.
- String slices (`&str`) and owned `String` continue to work seamlessly via trait implementations.
- Object metadata is configured via `ObjectUploadOptions::default().with_*(...)` or fluent convenience methods on multipart builders.
- Newtype unwrapping is standardized on `.into_inner()`.
