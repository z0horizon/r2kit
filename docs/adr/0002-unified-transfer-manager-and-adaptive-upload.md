# Unified Transfer Manager and Adaptive Upload Strategy

We introduce a two-tiered upload architecture featuring high-level transfer facades (`Bucket::upload_file`, `Bucket::upload`, and `Bucket::presign_upload`) backed by an internal `TransferManager` engine, alongside direct multipart aborting, canonical error representations, and streamlined ergonomics. This resolves protocol impedance mismatches (such as 0-byte file handling and upfront threshold branching) exposed during the real-world consumption of `r2kit` in `r2drive`.

## Status

Accepted

## Considered Options

- **Manual Caller Branching (Status Quo in `r2kit` 0.2.0):** Rejected because callers were forced to inspect local file metadata, maintain duplicated branching logic (< 10 MiB vs >= 10 MiB), and defensively catch `ValidationError::MultipartFileSizeZero` on 0-byte files which S3/R2 multipart uploads cannot process.
- **Standalone `TransferManager` Only (AWS SDK Style):** Rejected because requiring callers to instantiate a separate `TransferManager` handle for simple single-file uploads introduces unnecessary ceremony for common scripts and microservices.
- **In-Place Mutation of `ManagedMultipartBuilder`:** Rejected because silently executing a single PUT inside a builder named "multipart" is semantically dishonest, confuses telemetry, and fails to solve presigned upload coordination or in-memory byte streams.
- **Requiring `MultipartSessionSnapshot` to Abort:** Rejected because the S3/R2 `AbortMultipartUpload` wire protocol only requires `Bucket`, `Key`, and `UploadId`. Forcing callers to reconstruct a `MultipartPlan` with `file_size` and `part_size` leaked internal state machine details and forced downstream applications (`r2drive`) to maintain persistent database records solely for cleanup.

## Consequences

- **Two-Tiered Transfer Architecture:**
  - `Bucket::upload_file(key, path)` provides fluent, adaptive file uploads automatically switching between atomic `PutObject` and pipelined multipart transfers based on `UploadThreshold` (validated >= 5 MiB, defaulting to 8 MiB).
  - 0-byte and sub-threshold files transparently execute via atomic single-part PUT operations without throwing multipart plan errors.
  - In 0.2.1, the adaptive transfer engine is encapsulated in `UploadFileBuilder`; a standalone `TransferManager` handle for cross-transfer concurrency coordination and global multi-file pools is reserved for a future release.
- **Coordinated Presigned Uploads:**
  - `Bucket::presign_upload(key, size, expires_in)` returns a typed `PresignedUploadPlan` (`Single` vs `Multipart`), eliminating duplicated threshold and signing logic in web backends coordinating browser-to-R2 transfers.
- **Direct Multipart Abort:**
  - `Bucket::abort_multipart_upload(key, upload_id)` enables direct cancellation of in-flight or orphaned uploads without needing a `MultipartSessionSnapshot` or prior knowledge of file/part dimensions.
- **Canonical `NotFound` Representation:**
  - All missing-resource outcomes (whether from `head`, `get`, or remote SDK HTTP 404s) converge into `r2kit::Error::NotFound { key: Option<String> }` with an `err.is_not_found()` helper.
- **Streaming Pagination:**
  - `ListObjectsBuilder` provides both `.into_pages()` and `.into_stream()` / `.into_objects()` for async pagination.
- **Refined Ergonomics:**
  - `MultipartSessionSnapshot` derives `Serialize` and `Deserialize` under the `serde` feature flag.
  - `PresignedRequest` exposes `.as_str()` and `.into_url_string()` while preserving `Debug` log redaction.
  - `builder.content_type()` accepts `impl IntoContentType` (`&str`, `String`, or `Mime`) with offline validation.
