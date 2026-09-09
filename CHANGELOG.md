# Changelog

All notable changes to this project will be documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-09-10

### Added

- Range GET with single byte ranges: `ByteRange` enum (`Bounded`, `From`, `Suffix`) and `Bucket::get_object().range(...)`.
- Conditional operations: `If-Match`, `If-None-Match`, `If-Modified-Since`, and `If-Unmodified-Since` support on `GetObjectBuilder`, `HeadObjectBuilder`, `CopyObjectBuilder`, and `ObjectUploadOptions`, with HTTP 412 mapped to `Error::PreconditionFailed` and HTTP 304 mapped to `Error::NotModified`.
- Server-side cross-bucket copy and metadata directives: `CopyObjectBuilder` with same-account `source_bucket`, conditional source checks, and `MetadataDirective` (`Copy` or `Replace`).
- In-progress multipart upload listing: `Bucket::list_multipart_uploads()` returning `MultipartUploadPage` with secret-redacted upload IDs and sendable `.into_pages()` auto-pagination stream.
- Client-side upload checksum verification: `ChecksumAlgorithm` (`Crc32`, `Crc32c`, `Sha1`, `Sha256`) with automatic digest computation under the `checksum` feature flag and precomputed digest validation.
- Direct streaming download helper: `Bucket::download_file()` streaming remote bodies directly into local disk files.
- Account-level bucket management APIs: `R2Client::create_bucket()`, `R2Client::delete_bucket()`, `R2Client::list_buckets()`, and `R2Client::bucket_exists()` with validated 3-63 character bucket names.
- A streaming local-download example and an Axum presigned-upload example.
- Error-handling recipes, centralized transfer limits, and runnable API documentation for all transfer workflows.

## [0.1.0] - 2026-08-25

### Added

- R2-native client and bucket configuration.
- Typed default, EU, US, and FedRAMP jurisdiction endpoints.
- Validated connection, read, operation, and per-attempt timeouts plus SDK
  request-attempt configuration.
- Core object PUT, GET, HEAD, LIST, and DELETE operations.
- Presigned single-object and multipart upload flows.
- Resumable managed multipart file uploads with bounded concurrency, exact
  retries, progress reporting, cancellation, and cleanup.
- Secret-safe protocol and persistence types with optional Serde support.
- Machine-readable numeric validation errors with supplied and accepted bounds.
- Sanitized R2/AWS failure categories without exposing raw SDK errors.
- Explicit read-only bucket-access preflight helpers.
- Optional secret-safe `tracing` events, disabled by default.
- `part_size_mib` conveniences for readable multipart configuration without
  repeated byte-unit arithmetic.
- Typed `Mime` and `CacheControl` object metadata for regular, presigned, and
  managed uploads, including multipart creation and browser-safe signed headers.
- Upload support for content disposition, encoding, language, expiration, and
  user-defined metadata across regular, presigned, and managed workflows.
- Local structural BCP 47 validation for single and comma-separated
  `Content-Language` values.
- Opt-in R2 live coverage for extended metadata round trips, automatic page
  traversal, ordinary batch deletion, and a 1,001-object multi-request delete.
- Auto-paginating object page streams that preserve delimiter and common-prefix
  semantics.
- Multi-object deletion with automatic 1,000-key batching, per-key failures,
  and partial results when a later request fails.
- Managed-upload progress fraction and percentage helpers.
- A configurable 256 MiB default budget for in-flight managed-upload part
  buffers, validated before file or network I/O.
- Capped exponential full-jitter retries with bounded server retry-delay support.
- Source-file mutation detection using size, modification time, and file
  identity where supported.

### Fixed

- Enforced R2's documented 63-character bucket-name maximum.
- Enforced R2's effective per-request upload maximum of 5 MiB below 5 GiB.

[Unreleased]: https://github.com/zer0horizon/r2kit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/zer0horizon/r2kit/releases/tag/v0.1.0
