# r2kit — Cloudflare R2 object transfers for Rust

[![Crates.io](https://img.shields.io/crates/v/r2kit.svg)](https://crates.io/crates/r2kit)
[![Documentation](https://docs.rs/r2kit/badge.svg)](https://docs.rs/r2kit)
[![CI](https://github.com/zer0horizon/r2kit/actions/workflows/ci.yml/badge.svg)](https://github.com/zer0horizon/r2kit/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Ergonomic, safety-first Cloudflare R2 transfers for Rust.

`r2kit` handles the R2-specific details around the official AWS S3 SDK: the
account endpoint, `auto` signing region, secret-safe presigned requests,
resumable multipart sessions, and managed file uploads with bounded concurrency
and exact retries.

> **Status:** `0.3.0` introduces the Unified Transfer Manager with adaptive
> file uploads, coordinated presigned upload planning, direct multipart abort,
> and flattened listing streams, verified against live Cloudflare R2.

## Why r2kit?

- **R2-native setup:** build a correctly configured client from three required
  environment variables instead of wiring the S3 endpoint and signing behavior
  yourself. Temporary credentials can add an optional session token.
- **Safe secret boundaries:** credentials, upload IDs, and presigned URLs are
  redacted from `Debug`; exposing bearer values requires explicitly named APIs.
- **Transfer workflows included:** use simple object operations, managed local
  file uploads, or a server-controlled presigned multipart protocol.
- **Recovery by design:** snapshot, resume, reconcile, cancel, and clean up
  multipart uploads without inventing a persistence format.
- **No lock-in:** access the underlying `aws_sdk_s3::Client` whenever an
  operation is intentionally outside r2kit's scope.

## Quick start

Add r2kit and a Tokio runtime:

```sh
cargo add r2kit
cargo add tokio --features macros,rt-multi-thread
```

Create a bucket-scoped R2 token and set its S3 credentials:

```sh
export R2_ACCOUNT_ID="your-32-character-account-id"
export R2_ACCESS_KEY_ID="your-access-key-id"
export R2_SECRET_ACCESS_KEY="your-secret-access-key"
# export R2_SESSION_TOKEN="..." # only for temporary credentials
# export R2_JURISDICTION="eu"   # default, eu, us, or fedramp
```

Upload, inspect, download, list, and delete an object:

```rust,no_run
use r2kit::R2Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = R2Client::from_env()?.bucket("media")?;

    let uploaded = bucket
        .put_bytes("hello.txt", b"hello R2".to_vec())
        .await?;
    let metadata = bucket.head("hello.txt").await?;
    assert_eq!(metadata.etag(), uploaded.etag());

    let downloaded = bucket.get("hello.txt").await?;
    let bytes = downloaded.into_body().collect().await?.into_bytes();
    assert_eq!(bytes.as_ref(), b"hello R2");

    let page = bucket.list().prefix("hello").limit(100).send().await?;
    assert_eq!(page.objects().len(), 1);

    bucket.delete("hello.txt").await?;
    Ok(())
}
```

## Client configuration

The default jurisdiction uses
`https://<ACCOUNT_ID>.r2.cloudflarestorage.com`. Buckets with a data-residency
jurisdiction require the matching `eu`, `us`, or `fedramp` endpoint. A
jurisdiction is not a bucket location hint and does not change the signing
region, which remains `auto`.

Use explicit configuration when an application needs transport bounds or SDK
retry control:

```rust,no_run
use std::time::Duration;

use r2kit::{R2Client, R2Config, R2Jurisdiction};

fn client() -> Result<R2Client, r2kit::ConfigError> {
    let config = R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("access-key-id")
        .secret_access_key("secret-access-key")
        .jurisdiction(R2Jurisdiction::Eu)
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(30))
        .operation_attempt_timeout(Duration::from_secs(45))
        .operation_timeout(Duration::from_secs(120))
        .sdk_max_attempts(3)
        .build()?;

    Ok(R2Client::new(config))
}
```

Timeouts must be non-zero, and the per-attempt timeout cannot exceed the total
operation timeout. `sdk_max_attempts` includes the initial request. It controls
ordinary AWS SDK operations; managed multipart `UploadPart` requests disable
SDK retries and use `ManagedMultipartBuilder::max_attempts` as their exact
limit.

Leaving a transport option unset preserves the AWS SDK default. For custom
credential providers, HTTP clients, proxies, endpoint resolvers, or other
advanced SDK behavior, construct `aws_sdk_s3::Client` yourself and pass it to
`R2Client::from_sdk`. That escape hatch cannot verify the R2 endpoint, `auto`
region, credentials, timeouts, or retry policy, so the caller owns those
invariants.

## Choose the right API

| Use case | API |
|---|---|
| Upload bytes already in memory | `Bucket::put_bytes` |
| Upload a known-length async body | `Bucket::put_stream` |
| Download without buffering the whole object | `Bucket::get` |
| Upload a local file with adaptive single/multipart strategy | `Bucket::upload_file` |
| Upload a local file with explicit multipart options | `Bucket::managed_multipart` |
| Copy an object without downloading it | `Bucket::copy` |
| Coordinated presigned upload (single PUT or multipart) | `Bucket::presign_upload` |
| Direct client multipart upload coordination | `Bucket::presigned_multipart` |
| Abort an active or orphaned multipart upload directly | `Bucket::abort_multipart_upload` |
| Stream objects across listing pages | `Bucket::list().into_stream()` |
| Resume a persisted upload session | `Bucket::resume_managed_multipart` or `resume_presigned_multipart` |
| Verify bucket existence and list permission at startup | `R2Client::validate_bucket` or `Bucket::validate_access` |
| Use an S3 operation not wrapped by r2kit | `R2Client::as_sdk` |

Bucket selection is offline by default. Applications that prefer a fail-fast
startup check can explicitly perform one read-only request:

```rust,no_run
#[tokio::main]
async fn main() -> Result<(), r2kit::Error> {
    let client = r2kit::R2Client::from_env()?;
    let bucket = client.validate_bucket("media").await?;

    assert_eq!(bucket.name(), "media");
    Ok(())
}
```

## Typed object metadata

Use ecosystem media and header types instead of assembling security-sensitive
HTTP values by hand. Existing upload methods keep their metadata-free behavior;
the options variants opt into metadata explicitly.

```rust,no_run
use std::time::Duration;

use r2kit::{CacheControl, ObjectUploadOptions, R2Client, mime};

#[tokio::main]
async fn main() -> Result<(), r2kit::Error> {
    let bucket = R2Client::from_env()?.bucket("media")?;
    let options = ObjectUploadOptions::new()
        .with_content_type(mime::IMAGE_JPEG)
        .with_cache_control(
            CacheControl::new()
                .with_public()
                .with_max_age(Duration::from_secs(3_600)),
        )
        .with_content_disposition("attachment; filename=cat.jpg")
        .with_content_language("en")
        .with_custom_metadata("tenant-id", "tenant-42");

    bucket
        .put_bytes_with_options("photos/cat.jpg", vec![], options)
        .await?;
    Ok(())
}
```

For a presigned single PUT, typed metadata becomes part of the signature. The
uploader must replay every header in `PresignedRequest::required_headers`
exactly, and the bucket CORS policy must allow those headers. Multipart metadata
is applied by the trusted server during `CreateMultipartUpload`; individual
`UploadPart` requests do not repeat it.

Custom metadata keys omit the `x-amz-meta-` prefix and are canonicalized to
lowercase. r2kit accepts portable ASCII metadata keys and values, rejects
case-insensitive duplicates, and validates R2's 8,192-byte metadata limit
before network I/O. Content languages are validated structurally as one or more
comma-separated BCP 47 language tags; registry-level language policy remains an
application concern.

The core crate does not infer a MIME type from a filename. Browser applications
should validate and parse `File.type`; local-file applications may deliberately
use an extension-based helper such as `mime_guess` when guessing is acceptable.

## Paginated listings and batch deletion

`send()` intentionally fetches one bounded listing page. Use `into_stream()`
(or its alias `into_objects()`) to stream individual objects across all pages
automatically, or `into_pages()` to follow continuation tokens while preserving
page boundaries and common prefixes. Stream extension methods require
`futures-util` in the application.

```rust,no_run
use futures_util::TryStreamExt;
use r2kit::R2Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = R2Client::from_env()?.bucket("media")?;

    // Stream individual objects across all pages:
    let mut stream = std::pin::pin!(bucket.list().prefix("documents/").into_stream());
    while let Some(object) = stream.try_next().await? {
        println!("found object: {} ({} bytes)", object.key(), object.size());
    }

    // Or process bounded pages:
    let pages: Vec<_> = bucket
        .list()
        .prefix("temporary/")
        .into_pages()
        .try_collect()
        .await?;

    let keys = pages
        .iter()
        .flat_map(|page| page.objects())
        .map(|object| object.key().to_owned());
    let deleted = bucket.delete_objects(keys).await?;
    for failure in deleted.failures() {
        eprintln!("could not delete {}: {:?}", failure.key(), failure.code());
    }
    Ok(())
}
```

`delete_objects` validates every key before deleting anything, sends sequential
batches of at most 1,000 keys, and reports service-level failures per key. A
request-level `BatchDeleteError` retains results from batches that had already
completed.

## Managed file uploads

`bucket.upload_file` provides an adaptive transfer manager that automatically selects
between a single `PutObject` request (for 0-byte or files below threshold) and a managed
multipart upload (for larger files) with bounded concurrency, part retry policies, and
monotonic progress reporting.

```rust,no_run
use r2kit::R2Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = R2Client::from_env()?.bucket("media")?;

    // Simplest adaptive upload: selects single PUT (< 8 MiB) or multipart (>= 8 MiB)
    let result = bucket.upload_file("documents/report.pdf", "report.pdf").await?;
    println!("uploaded {} bytes (etag: {})", result.size(), result.etag());

    // Or configure custom threshold, concurrency, and progress tracking:
    let result = bucket
        .upload_file("videos/demo.mp4", "demo.mp4")
        .threshold_mib(16)?
        .concurrency(4)
        .max_attempts(4)
        .on_progress(|progress| {
            eprintln!(
                "{}/{} bytes ({:.1}%)",
                progress.transferred_bytes(),
                progress.total_bytes(),
                progress.percentage()
            );
        })
        .await?;

    eprintln!("upload strategy used: {:?}", result.strategy());
    Ok(())
}
```

Adaptive file dispatch:
- **0-byte files:** Dispatched as a single PUT with an empty payload. Avoids multipart overhead and prevents `ValidationError::MultipartFileSizeZero`.
- **Sub-threshold files:** Streamed via a single PUT with cooperative cancellation support.
- **Above-threshold files:** Split into R2-compatible parts and uploaded in parallel with bounded concurrency and exact retries.

For advanced low-level multipart workflows, `bucket.managed_multipart` remains available to
explicitly configure multipart sessions.

The uploader owns the `UploadPart` retry policy. Network failures, HTTP 408,
429, and 5xx responses are retried with exponential full jitter capped at 30
seconds. Numeric `Retry-After` seconds and AWS-compatible
`x-amz-retry-after` milliseconds raise the delay up to that cap. AWS SDK
retries are disabled for that operation, so `max_attempts` is the exact
request-attempt limit.

Each in-flight part is buffered in memory. Before opening the file or contacting
R2, the builder verifies that `part_size * concurrency` fits the 256 MiB default
part-buffer budget. Configure an intentional larger bound with
`max_buffered_bytes` or `max_buffered_mib`; this controls payload buffers rather
than all process or SDK overhead.

Failures trigger a best-effort abort by default. Use `abort_on_error(false)` to
retain `ManagedUploadError::snapshot()` for a later resume. The source file must
not change while an upload is running. r2kit compares its size, modification
time, and file identity where supported before completion, but applications
should still treat immutability as a caller-owned invariant.

### Cancellation

Cancellation is cooperative: signal it from another task and continue awaiting
the upload so r2kit can abort the remote multipart session.

```rust,no_run
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = r2kit::R2Client::from_env()?.bucket("media")?;
    let cancellation = r2kit::ManagedUploadCancellation::new();
    let signal = cancellation.clone();

    let upload = bucket
        .managed_multipart("videos/demo.mp4")?
        .cancellation_token(cancellation)
        .upload_file("videos/demo.mp4");

    signal.cancel();
    let error = upload.await.unwrap_err();
    assert!(matches!(error.error(), r2kit::Error::Cancelled));
    Ok(())
}
```

Dropping the future cannot perform asynchronous cleanup. Signal cancellation
and keep awaiting it instead.

### Direct multipart abort and lifecycle cleanup

Downstream applications can directly abort an active or orphaned multipart upload
without needing a session snapshot or prior knowledge of part sizing:

```rust,no_run
use r2kit::R2Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = R2Client::from_env()?.bucket("media")?;

    // Directly abort an orphaned or cancelled multipart upload
    bucket.abort_multipart_upload("videos/demo.mp4", "upload-id-here").await?;
    Ok(())
}
```

Cloudflare R2 automatically aborts incomplete multipart uploads seven days
after initiation by default. Treat that bucket lifecycle rule as a final safety
net rather than the primary cleanup path: keep awaiting cooperative cancellation
or invoke `abort_multipart_upload` so r2kit can abort promptly. Verify that the default rule remains enabled, or
configure a shorter interval when abandoned uploads should be reclaimed sooner.

## Direct browser and mobile uploads

The trusted server creates a multipart session and signs each part. The
untrusted uploader receives short-lived bearer URLs but never receives the R2
access key or secret.

### Coordinated presigned upload plan

When a web or mobile client reports an expected file size, `bucket.presign_upload`
coordinates whether to generate a single presigned PUT URL (for sub-threshold files)
or initiate a presigned multipart upload session:

```rust,no_run
use std::time::Duration;
use r2kit::{PartNumber, PresignedUploadPlan, R2Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = R2Client::from_env()?.bucket("media")?;

    // Automatically coordinates single PUT (< 8 MiB) or multipart (>= 8 MiB)
    let plan = bucket
        .presign_upload("uploads/video.mp4", 50 * 1024 * 1024, Duration::from_secs(900))
        .await?;

    match plan {
        PresignedUploadPlan::Single(put) => {
            println!("Single PUT URL: {}", put.as_str());
        }
        PresignedUploadPlan::Multipart(multipart) => {
            println!("Multipart upload ID: {}", multipart.upload_id());
            println!("Part count: {}", multipart.part_count());
            let part = multipart
                .presign_part(PartNumber::try_from(1)?, Duration::from_secs(900))
                .await?;
            println!("Part 1 URL: {}", part.request().as_str());
        }
    }
    Ok(())
}
```

### Server-controlled multipart protocol

`file_size` is intentionally required for this server-controlled flow. A web
client sends its `File.size` when requesting a new upload; the server must treat
that value as untrusted. r2kit validates it before contacting R2, uses it to
calculate the number of parts and exact final-part length, rejects plans over
R2's object or 10,000-part limits, and verifies the same plan before completion.
If the trusted application is uploading a local path instead, use
`upload_file(key, path)`: that API reads and validates the size itself.

```text
trusted server                browser/mobile                    Cloudflare R2
      | create session               |                                |
      | sign part requests ---------->                                |
      |                              | PUT parts with signed headers ->|
      |<--------- exact ETags -------|<-------------------------------|
      | reconcile + complete ---------------------------------------->|
```

```rust,no_run
use std::time::Duration;

use r2kit::{CompletionManifest, PartMd5, PartNumber, R2Client};

async fn sign_and_complete(
    file_size_from_browser: u64,
    part_md5_base64: &str,
) -> Result<(), r2kit::Error> {
    let bucket = R2Client::from_env()?.bucket("media")?;
    let upload = bucket
        .presigned_multipart("videos/demo.mp4")?
        .file_size(file_size_from_browser)
        .part_size_mib(5)
        .create()
        .await?;

    let part = upload
        .presign_part_with_md5(
            PartNumber::try_from(1)?,
            PartMd5::try_from(part_md5_base64)?,
            Duration::from_secs(15 * 60),
        )
        .await?;

    // Deliberate exposure boundary for sending the bearer request to the uploader.
    let _request = part.into_protocol_request()?;

    // After collecting and validating every uploader receipt:
    let remote = upload.reconcile().await?;
    if remote.is_complete() {
        let manifest: CompletionManifest = remote.into_completion_manifest()?;
        upload.complete_verified(manifest).await?;
    }
    Ok(())
}
```

For browser uploads, configure bucket CORS to allow `Content-MD5` and expose
`ETag`. The uploader must replay every signed header exactly. Multipart ETags
are opaque completion identifiers, not whole-object content hashes.

## Persistence and feature flags

The default build has no optional features enabled.

| Feature | Purpose |
|---|---|
| `serde` | Serialize versioned multipart session records, signed request DTOs, and uploader receipts |
| `tracing` | Emit secret-safe diagnostic events through the application's existing `tracing` subscriber |
| `live-tests` | Compile the credential-gated Cloudflare R2 integration tests; not intended for applications |

Enable Serde when a session or protocol DTO crosses a storage or JSON boundary:

```toml
[dependencies]
r2kit = { version = "0.2.1", features = ["serde"] }
```

`MultipartSessionSnapshot::into_persistence_record()` deliberately exposes a
secret-bearing persistence value. Store it as securely as an API credential.

## Errors and observability

Known numeric constraints are rejected locally before file or network I/O and
include the supplied and accepted values. This includes multipart part size,
part number, part count, object size, concurrency, attempt count, list limit,
single-request upload size, and presign expiry.

```rust,no_run
use r2kit::{Error, ValidationError};

fn inspect(error: Error) {
    match error {
        Error::Validation(ValidationError::PartSizeOutOfRange {
            provided,
            min,
            max,
        }) => eprintln!("part size {provided} must be within {min}..={max} bytes"),
        Error::Remote(remote) => eprintln!(
            "{} failed: {} (status {:?})",
            remote.operation(),
            remote.kind(),
            remote.status()
        ),
        other => eprintln!("{other}"),
    }
}
```

Remote failures are reduced to a stable `ServiceErrorKind`, operation name, and
optional HTTP status. Raw AWS SDK errors are intentionally not retained because
they may contain signed request details. Object `GET` and `HEAD` preserve the
convenient `Error::NotFound` result.

Tracing is opt-in and disabled by default:

```toml
[dependencies]
r2kit = { version = "0.2.1", features = ["tracing"] }
```

The library emits events to target `r2kit` but never installs a subscriber.
Events contain operation/category/status and bounded transfer settings only;
bucket names, object keys, local paths, account IDs, credentials, upload IDs,
presigned URLs, and signed headers are excluded.

## Security model

- Credentials, upload IDs, and presigned URLs are redacted from `Debug` and
  error messages.
- Presigned URLs remain bearer credentials. Authorize before issuing them, use
  short expirations, and never log them.
- Deterministic input failures are validated before network requests whenever
  possible.
- R2 multipart plans enforce parts from 5 MiB through R2's effective maximum
  of 5 MiB below 5 GiB, at most 10,000 parts, equal non-final part sizes, and
  the effective multipart object limit.
- Managed uploads use bounded concurrency, a configurable part-buffer memory
  budget, capped full-jitter retries, and an exact retry limit.
- Applications still own authorization, rate limiting, CORS policy, and the
  lifecycle policy for abandoned uploads.

Report vulnerabilities through GitHub's private security advisory flow. See
[SECURITY.md](SECURITY.md) for scope and reporting guidance.

## Examples and API documentation

- [Object round trip](examples/object_round_trip.rs)
- [Managed file upload](examples/managed_upload.rs)
- [Streaming download to a local file](examples/download_to_file.rs)
- [Axum endpoint returning a presigned upload request](examples/presigned_upload_axum.rs)
- [Error handling cookbook](docs/error-handling.md)
- [Transfer limits and memory guidance](docs/limits.md)
- [Architecture and protocol invariants](docs/design.md)
- [API documentation](https://docs.rs/r2kit)

The runnable examples use the `R2_*` credentials described in Quick start.
Object examples also require `R2_BUCKET` and `R2_KEY`:

```sh
cargo run --example object_round_trip
cargo run --example managed_upload -- ./large-video.mp4
cargo run --example download_to_file -- ./download.bin
```

`download_to_file` truncates an existing destination. The Axum example only
requires `R2_BUCKET`; start it and request an exact-length, ten-minute upload:

```sh
cargo run --example presigned_upload_axum
curl -X POST 'http://127.0.0.1:3000/uploads/photos/cat.jpg?content_length=12345'
```

The JSON response contains a bearer URL and all signed headers the client must
replay exactly. A production handler must authenticate the caller, authorize
the object key, enforce size/rate limits, and avoid logging the response.

## Compatibility and scope

- Minimum supported Rust version: **1.94.1**.
- Runtime: Tokio.
- Backend: Cloudflare R2 through `aws-sdk-s3`.
- License: MIT or Apache-2.0, at your option.

ACLs, tagging, versioning, object lock, folder sync, a CLI, and a custom SigV4
implementation are deliberately out of scope.

## Development

The repository pins its Rust toolchain and keeps Git hooks in version control.
After cloning, enable the hooks once:

```sh
./scripts/install-git-hooks.sh
```

Run the offline quality suite:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --no-default-features -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
cargo package
```

Property, fuzz, live contract, and 64 MiB stress-test commands are documented in
[CONTRIBUTING.md](CONTRIBUTING.md). Live tests only use the dedicated bucket and
prefix supplied by the test operator, and clean up completed objects and active
multipart sessions.

Contributions are welcome when they simplify an R2 transfer workflow, enforce
an R2 invariant, or improve recovery and observability without hiding the
underlying SDK. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a
pull request.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
