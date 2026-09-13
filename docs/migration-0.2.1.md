# Migrating to 0.2.1 (Unified Transfer Manager)

`r2kit 0.2.1` introduces the Unified Transfer Manager, adaptive upload dispatch,
coordinated presigned uploads, direct multipart abort, canonical not-found detection,
and streamlined object listing streams.

Downstream applications such as `r2drive` can eliminate manual 0-byte bypasses,
complex snapshot reconstructions, and manual size-based branching.

---

## Key Highlights

| Feature | In 0.2.0 | In 0.2.1 |
|---|---|---|
| **Adaptive file upload** | Manual branching between `put_bytes`/`put_stream` and `managed_multipart` | Single facade: `bucket.upload_file("key", "path").await` |
| **0-byte file uploads** | `managed_multipart` errored with `MultipartFileSizeZero` | `upload_file` automatically dispatches single PUT with 100% progress |
| **Direct multipart abort** | Required restoring `MultipartSessionSnapshot` with dummy part sizing | Direct `bucket.abort_multipart_upload("key", upload_id).await` |
| **Coordinated presigning** | Manual threshold checks between `presign_put` and `presigned_multipart` | Coordinated `bucket.presign_upload("key", size, duration).await` |
| **Content-Type ergonomics** | Required manual parsing into `mime::Mime` | `IntoContentType` trait accepts `&str`, `String`, and `mime::Mime` |
| **Not-found checking** | Checked both `Error::NotFound` and `ServiceErrorKind::NotFound` | Unified `err.is_not_found()` helper method |
| **Listing objects** | Paged iteration via `into_pages()` requiring nested loops or flattening | Direct item stream via `bucket.list().into_stream()` |

---

## 1. Unified Adaptive File Uploads (`Bucket::upload_file`)

### Before (0.2.x)

Downstream consumers uploading local files had to implement size-dependent branching logic.
Calling `managed_multipart` on an empty file failed with `ValidationError::MultipartFileSizeZero`,
and uploading small files (< 5 MiB) through multipart introduced unnecessary latency and part overhead.

```rust
// In 0.2.x: Consumer had to branch manually
let file_size = std::fs::metadata(&path)?.len();

if file_size == 0 {
    // 0-byte bypass required
    bucket.put_bytes(key, vec![]).await?;
} else if file_size < 8 * 1024 * 1024 {
    // Small file bypass
    let bytes = std::fs::read(&path)?;
    bucket.put_bytes(key, bytes).await?;
} else {
    // Multipart pipeline
    bucket
        .managed_multipart(key)?
        .concurrency(4)
        .upload_file(&path)
        .await?;
}
```

### After (0.2.1)

Use `bucket.upload_file` for all file transfers. It adaptively determines the optimal
transfer strategy:

```rust
// Simplest adaptive upload
let result = bucket.upload_file("documents/report.pdf", "report.pdf").await?;
println!("uploaded {} bytes (etag: {})", result.size(), result.etag());

// Or with custom threshold, concurrency, and progress tracking:
let result = bucket
    .upload_file("media/video.mp4", "video.mp4")
    .threshold_mib(16)?
    .concurrency(4)
    .on_progress(|p| {
        println!("{}/{} bytes ({:.1}%)", p.transferred_bytes(), p.total_bytes(), p.percentage());
    })
    .await?;

println!("Strategy used: {:?}", result.strategy());
```

### Strategy Dispatch Details:
- **0-byte files:** Dispatches as `TransferStrategyUsed::SinglePut` with empty payload. Emits `TransferProgress` at 0/0 bytes and 100%, without invoking Cloudflare R2 multipart APIs.
- **Sub-threshold files:** Dispatches as `TransferStrategyUsed::SinglePut`, streaming file content directly with cooperative cancellation support.
- **Above-threshold files:** Dispatches as `TransferStrategyUsed::Multipart`, chunking into R2-compatible parts with bounded memory usage, concurrent streaming, and exact retry policies.

---

## 2. Direct Multipart Abort Without Snapshot (`Bucket::abort_multipart_upload`)

### Before (0.2.x)

Aborting an active or orphaned multipart upload required restoring a `MultipartSessionSnapshot`
with dummy or remembered `file_size` and `part_size` parameters just to obtain a session handle
to call `.abort()`.

```rust
// In 0.2.x: Required reconstructing snapshot with dummy dimensions
let snapshot = r2kit::MultipartSessionSnapshot::restore(
    bucket.name(),
    key,
    upload_id,
    dummy_file_size, // arbitrary placeholder
    dummy_part_size, // arbitrary placeholder
)?;

let session = bucket.resume_presigned_multipart(snapshot)?;
session.abort().await?;
```

### After (0.2.1)

Call `bucket.abort_multipart_upload` directly with the key and upload ID:

```rust
bucket.abort_multipart_upload("media/video.mp4", upload_id).await?;
```

- Performs local input validation (non-empty key and upload ID).
- Calls S3/R2 `AbortMultipartUpload` directly without requiring session state or part size metadata.
- Returns `Error::NotFound` if the upload ID does not exist or has already completed/aborted.

---

## 3. Coordinated Presigned Uploads (`Bucket::presign_upload`)

### Before (0.2.x)

When coordinating presigned uploads for browser or mobile clients, servers had to manually
decide between single PUT presigning and multipart session initialization based on file size.

```rust
// In 0.2.x: Manual size check and separate flows
let plan = if file_size < 8 * 1024 * 1024 {
    let put = bucket.presign_put(key, file_size, expires_in).await?;
    MyPlan::Single(put.request().url().expose().to_string())
} else {
    let session = bucket
        .presigned_multipart(key)?
        .file_size(file_size)
        .create()
        .await?;
    let upload_id = session.snapshot().expose_upload_id().to_string();
    MyPlan::Multipart { upload_id, part_count: session.part_count() }
};
```

### After (0.2.1)

Use `bucket.presign_upload` to obtain a `PresignedUploadPlan`:

```rust
use r2kit::{PartNumber, PresignedUploadPlan};

let plan = bucket
    .presign_upload("uploads/photo.jpg", file_size, Duration::from_secs(900))
    .await?;

match plan {
    PresignedUploadPlan::Single(put) => {
        // Direct signed PUT URL for small files
        println!("PUT URL: {}", put.as_str());
    }
    PresignedUploadPlan::Multipart(plan) => {
        // Initiated multipart upload session for large files
        println!("Upload ID: {}", plan.upload_id());
        println!("Part count: {}", plan.part_count());
        let part1 = plan
            .presign_part(PartNumber::try_from(1)?, Duration::from_secs(900))
            .await?;
        println!("Part 1 URL: {}", part1.request().as_str());
    }
}
```

- Offline validation of `expires_in` occurs upfront before any network I/O.
- Also supports `bucket.presign_upload_with_options` for typed metadata, MIME types, and cache-control headers.

---

## 4. Canonical `Error::NotFound` and `err.is_not_found()`

### Before (0.2.x)

Consumers checking if an operation failed due to a missing remote resource (such as a 404
or `NoSuchKey`/`NoSuchUpload`) had to inspect multiple error variants:

```rust
// In 0.2.x: Matching across top-level and nested ServiceError
match err {
    r2kit::Error::NotFound => {
        // Handle 404 from get/head
    }
    r2kit::Error::Remote(ref se) if se.kind() == r2kit::ServiceErrorKind::NotFound => {
        // Handle 404 from other S3 operations
    }
    other => return Err(other.into()),
}
```

### After (0.2.1)

Use the canonical `err.is_not_found()` helper method:

```rust
if err.is_not_found() {
    println!("Resource does not exist on R2");
} else {
    eprintln!("Unexpected transfer failure: {err}");
}
```

The method returns `true` for both `Error::NotFound` and `ServiceErrorKind::NotFound` (matching HTTP 404, `NoSuchKey`, `NoSuchBucket`, and `NoSuchUpload`).

---

## 5. `IntoContentType` Trait Support for `&str` and `String`

### Before (0.2.x)

Specifying a MIME type required explicit parsing and error handling before calling builder methods:

```rust
// In 0.2.x: Manual parsing required
let mime = "image/jpeg"
    .parse::<r2kit::mime::Mime>()
    .map_err(|e| MyError::BadRequest(e.to_string()))?;

let options = r2kit::ObjectUploadOptions::new()
    .with_content_type(mime);
```

### After (0.2.1)

Pass `&str`, `String`, or `mime::Mime` directly to `with_content_type` or `UploadFileBuilder::content_type`:

```rust
// String slices and Strings work directly:
let options = r2kit::ObjectUploadOptions::new()
    .with_content_type("application/pdf")?;

// In UploadFileBuilder:
let result = bucket
    .upload_file("docs/sheet.csv", "sheet.csv")
    .content_type("text/csv")
    .await?;
```

The `IntoContentType` trait validates that the string is a valid MIME media type, returning `Error::InvalidInput` if invalid.

---

## 6. Flattened Object Streaming (`into_stream` / `into_objects`)

### Before (0.2.x)

Listing objects via `bucket.list().into_pages()` yielded pages of objects (`ObjectPage`),
requiring application code to manage nested iteration or stream flattening:

```rust
// In 0.2.x: Required flattening page stream manually
use futures_util::TryStreamExt;

let pages: Vec<_> = bucket
    .list()
    .prefix("backups/")
    .into_pages()
    .try_collect()
    .await?;

for page in pages {
    for object in page.objects() {
        println!("object: {}", object.key());
    }
}
```

### After (0.2.1)

Use `into_stream()` (or `into_objects()`) to obtain a flattened stream of `Result<ObjectItem, Error>`:

```rust
use futures_util::TryStreamExt;

let mut stream = std::pin::pin!(bucket.list().prefix("backups/").into_stream());
while let Some(object) = stream.try_next().await? {
    println!("object: {} ({} bytes)", object.key(), object.size());
}
```

`ObjectItem` is re-exported at the crate root as an alias for `ObjectSummary`.

---

## 7. Public API Re-exports

All transfer and coordination types are re-exported at the crate root:

| Type | Description |
|---|---|
| `UploadThreshold` | Validated threshold (5 MiB ..= 5 TiB, default 8 MiB) for upload strategy selection |
| `TransferProgress` | Point-in-time progress snapshot (`transferred_bytes`, `total_bytes`, `percentage`, `fraction`) |
| `TransferResult` | Upload outcome with `key`, `etag`, `size`, and `strategy` |
| `TransferStrategyUsed` | Enum indicating `SinglePut` or `Multipart { part_count }` |
| `PresignedUploadPlan` | Enum coordinating `Single(PresignedPutObject)` vs `Multipart(PresignedMultipartPlan)` |
| `PresignedMultipartPlan` | Active presigned multipart plan exposing `upload_id`, `part_count`, and `presign_part` |
| `IntoContentType` | Trait enabling seamless conversion from `&str`, `String`, and `Mime` |
| `UploadFileBuilder` | Fluent builder for adaptive file uploads with `IntoFuture` direct `.await` support |
| `ObjectItem` | Type alias for `ObjectSummary` returned by `into_stream()` |
