# Unified Transfer Manager and Upload Abstractions in r2kit
## Architectural Research, Industry Precedents, and Design Specification

**Author:** r2kit Research Subagent  
**Date:** September 2026  
**Status:** Proposed Architecture & RFC  
**Target:** `r2kit` 0.3.0+ / Downstream Consumer: `r2drive`  

---

### Executive Summary

In `r2kit` 0.2.0, uploading objects requires consumers to make a manual, upfront decision between two divergent APIs:
1. `Bucket::put_bytes(key, bytes)` for atomic single-part PUT operations.
2. `Bucket::managed_multipart(key).upload_file(path)` for chunked, concurrent multipart transfers.

Practical consumption of `r2kit` in real-world applications—most notably `r2drive` (a high-performance CLI and cloud synchronization engine)—has surfaced significant architectural friction:
- **Immediate Rejection of 0-Byte Files:** Calling `managed_multipart` on an empty file immediately fails offline with `ValidationError::MultipartFileSizeZero`.
- **Manual Threshold Branching in Callers:** Downstream code is forced to inspect local file metadata and implement arbitrary branching logic (e.g. `if size < 10_MB { put_bytes } else { managed_multipart }`).
- **Fragmented Presigned Upload Coordination:** Applications coordinating direct client-to-R2 transfers must implement parallel branches for single presigned PUTs vs presigned multipart sessions, duplicating threshold and signing logic.
- **Cognitive Overhead and Safety Gaps:** Callers must manually orchestrate memory budgets, concurrency limits, and retry policies instead of relying on the SDK to enforce safe transfer boundaries.

This research report investigates industry precedents across major cloud storage SDKs (AWS SDK for Java 2.x, AWS Boto3 for Python, AWS SDK for Go v2, AWS SDK for Rust, OpenDAL, and Apache Arrow `object_store`), cross-references Cloudflare R2 and Amazon S3 protocol specifications, and presents three candidate architectural designs for `r2kit`. 

The central finding across all leading cloud storage implementations is that **a unified upload abstraction must decouple caller intent ("upload this source to this key") from transfer strategy selection ("single PUT vs multipart upload")**. Empty files and small payloads must transparently resolve to single-part PUT operations, while large files automatically trigger pipelined multipart transfers. 

Finally, this document presents a recommended **Two-Tiered Hybrid Architecture** for `r2kit`: a high-level, fluent `Bucket::upload_file` / `Bucket::uploader()` interface backed by a dedicated, reusable `TransferManager` engine, paired with a unified `Bucket::presign_upload` coordinator.

---

### 1. Problem Statement & Practical Motivation

#### 1.1 Current Architecture of `r2kit`

`r2kit` was designed around strong type-safety, offline preflight validation, memory budgeting, and secret redaction (as codified in ADR 0001). For uploads, it currently provides two disjoint mechanisms:

```rust
// Approach 1: Single PUT (in-memory bytes only)
bucket.put_bytes("path/to/file.txt", data).await?;

// Approach 2: Managed Multipart (disk file only)
bucket.managed_multipart("path/to/large.iso")
    .part_size(8 * 1024 * 1024)
    .concurrency(4)
    .upload_file("path/to/large.iso")
    .await?;
```

Under the hood, `ManagedMultipartBuilder::upload_file` evaluates the file size and passes it directly into `MultipartPlan::new(file_size, part_size)` in `src/multipart.rs`:

```rust
impl MultipartPlan {
    pub(crate) fn new(file_size: u64, part_size: u64) -> Result<Self, Error> {
        if file_size == 0 {
            return Err(ValidationError::MultipartFileSizeZero.into());
        }
        // ...
    }
}
```

#### 1.2 Practical Friction in Downstream Consumption (`r2drive`)

When building `r2drive` on top of `r2kit`, several workarounds were required to handle routine uploads. In `r2drive/src/cli/upload.rs` (lines 146–158):

```rust
// Edge case: 0-byte file cannot be uploaded via multipart plan
if file_size == 0 {
    bucket
        .put_bytes(&resolved_key, Vec::new())
        .await
        .map_err(crate::r2::map_r2_error)?;
    println!("Uploaded {} to {} (0 bytes)", local_path.display(), resolved_key);
    return Ok(());
}
```

Without this manual bypass, attempting to upload an empty file (such as a `.gitkeep`, an empty log, or a touch file) via `managed_multipart` crashes with `ValidationError::MultipartFileSizeZero`.

Furthermore, in `r2drive/src/r2/transfer.rs`, the team had to write and maintain two completely separate presigning workflows:
- `init_single_presigned_upload_with_content_type`: Calls `bucket.presign_put_with_options`.
- `init_presigned_upload_with_content_type`: Calls `bucket.presigned_multipart`.

Every caller in the WebConsole or CLI must manually duplicate threshold logic, determine whether the file meets a multipart cutoff, and branch between disparate types.

---

### 2. Industry Precedents & Architectural Survey

A comparative study of production cloud storage toolkits reveals consistent patterns in handling transfer strategy selection, thresholds, 0-byte files, and memory limits.

| SDK / Tool | Primary Abstraction | Default Threshold | Default Part Size | 0-Byte Strategy | In-Flight Memory Control |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **AWS SDK for Java 2.x** | `S3TransferManager` | 8 MiB | 8 MiB | Auto Single PUT | Thread pool & buffer queues |
| **AWS SDK Python (Boto3)** | `s3transfer.S3Transfer` | 8 MiB | 8 MiB | Auto Single PUT | `max_io_queue` & chunk limits |
| **AWS SDK for Go v2** | `feature/s3/manager.Uploader` | 5 MiB (`MinUploadPartSize`) | 5 MiB | Auto Single PUT | Stream buffer chunking |
| **AWS SDK for Rust** | `aws-sdk-s3-transfer-manager` | 8 MiB (`PartSize`) | 8 MiB | Auto Single PUT | `MemoryBudgetConfig` |
| **Apache OpenDAL** | `opendal::Operator` / `Writer` | Adaptive (8 MiB buffer) | 8 MiB | `write_once` (Single PUT) | Buffer capacity flushing |
| **Apache Arrow** | `object_store::buffered::BufWriter` | Capacity-based (10 MiB) | Capacity | `put_opts` on close | Bounded internal buffer |

#### 2.1 AWS SDK for Python (Boto3 / `s3transfer`)

In Boto3, file uploads are handled through `s3.upload_file(Filename, Bucket, Key, Config=...)`, backed by the internal `s3transfer` manager.

Configuration is governed by `TransferConfig`:
```python
class boto3.s3.transfer.TransferConfig(
    multipart_threshold=8388608,  # 8 MiB
    max_concurrency=10,
    multipart_chunksize=8388608,  # 8 MiB
    max_io_queue=100,
    io_chunksize=262144           # 256 KiB
)
```

**Strategy Resolution:**
- When `upload_file` is invoked, `s3transfer` queries `os.path.getsize(filename)`.
- If `file_size < multipart_threshold` (which includes `0` bytes): It automatically routes the request to `PutObjectSubscriber`. A standard `PutObject` HTTP request is issued with `Content-Length: 0`.
- If `file_size >= multipart_threshold`: It routes to `MultipartUploadSubscriber`, executing `CreateMultipartUpload`, concurrent `UploadPart` tasks, and `CompleteMultipartUpload`.
- The caller never sees or branches on this distinction.

#### 2.2 AWS SDK for Java 2.x (`S3TransferManager`)

In AWS Java SDK v2, `S3TransferManager` provides high-level transfer methods:
```java
UploadFileRequest uploadRequest = UploadFileRequest.builder()
    .putObjectRequest(b -> b.bucket(bucket).key(key))
    .source(path)
    .build();
FileUpload upload = transferManager.uploadFile(uploadRequest);
CompletedFileUpload completed = upload.completionFuture().join();
```

Configuration is set on the underlying multipart-enabled async client:
- `thresholdInBytes`: Default 8 MiB (8 * 1024 * 1024).
- `minimumPartSizeInBytes`: Default 8 MiB.
- `maximumMemoryUsageInBytes`: Limits buffer allocation across concurrent uploads.

**Zero-Byte Handling:**
The Java transfer manager checks `Files.size(path)`. If the file is 0 bytes or below the threshold, it invokes `S3AsyncClient::putObject`. Multipart coordination is only instantiated when payload length exceeds `thresholdInBytes`.

#### 2.3 AWS SDK for Go (v2 `feature/s3/manager`)

The Go SDK provides `manager.Uploader`:
```go
uploader := manager.NewUploader(client, func(u *manager.Uploader) {
    u.PartSize = manager.DefaultUploadPartSize // 5 MiB (MinUploadPartSize)
    u.Concurrency = 5
})
result, err := uploader.Upload(ctx, &s3.PutObjectInput{
    Bucket: aws.String(bucket),
    Key:    aws.String(key),
    Body:   fileReader,
})
```

**Stream Inspection & Small File Fallback:**
If the `Body` implements `io.ReadSeeker` (e.g. `*os.File`), `manager.Uploader` queries the length via `Seek`. If `len < u.PartSize`, it issues a single `PutObject`.
If the body is a non-seekable stream (`io.Reader`), the uploader buffers the first `PartSize` bytes. If it encounters `io.EOF` during the first buffer read (including immediate EOF for 0-byte streams), it never calls `CreateMultipartUpload`; it simply flushes the buffered slice via a single `PutObject`.

#### 2.4 AWS SDK for Rust (`aws-sdk-s3-transfer-manager` 0.2.0)

AWS's official Rust Transfer Manager crate introduces an asynchronous, high-throughput abstraction over `aws-sdk-s3`:
```rust
let config = aws_sdk_s3_transfer_manager::from_env().load().await;
let client = aws_sdk_s3_transfer_manager::Client::new(config);

let handle = client
    .upload()
    .bucket("my-bucket")
    .key("my-key")
    .body(InputStream::from_path(path).await?)
    .initiate()?;

let output = handle.join().await?;
```

**Key Features:**
- `InputStream`: Unifies `from_path`, `from_static`, `read_from`, and streaming part readers.
- `multipart_threshold`: Configured via `types::PartSize` (default 8 MiB).
- `memory_budget`: Enforced via `MemoryBudgetConfig` to prevent memory starvation when uploading many files concurrently.
- `FailedMultipartUploadPolicy`: Declaratively specifies whether to abort, retain, or snapshot failed uploads.

#### 2.5 Apache OpenDAL (`opendal::Operator`)

OpenDAL abstracts cloud storage through a single unified `Operator`:
```rust
// Simple upload
op.write("path/to/key", data).await?;

// Streaming writer with chunk and concurrency control
let mut writer = op.writer_with("path/to/key")
    .chunk(8 * 1024 * 1024)
    .concurrent(4)
    .await?;

writer.write(chunk1).await?;
writer.close().await?;
```

**Adaptive Single PUT vs Multipart Execution:**
OpenDAL's S3 backend buffers incoming writes. 
- If `writer.close()` is called and total written bytes do not exceed the chunk size, OpenDAL invokes `write_once` (issuing a single HTTP PUT).
- If the accumulated bytes exceed the chunk size, OpenDAL seamlessly triggers `initiate_part` and switches into multipart streaming mode.
- A 0-byte write naturally flushes via `write_once` with an empty body, completely avoiding multipart errors.

#### 2.6 Apache Arrow (`object_store::buffered::BufWriter`)

Apache Arrow's `object_store` crate offers `buffered::BufWriter`:
> *"An async buffered writer compatible with the tokio IO traits. This writer adaptively uses `ObjectStore::put_opts` or `ObjectStore::put_multipart_opts` depending on the amount of data that has been written. Up to capacity bytes will be buffered in memory, and flushed on shutdown using `ObjectStore::put_opts`. If capacity is exceeded, data will instead be streamed using `ObjectStore::put_multipart_opts`."*

If 0 bytes or less than 10 MiB are written before shutdown, it issues a single `put_opts`. Only when buffer capacity is exceeded does it lazily initiate an S3 multipart session.

---

### 3. Cloudflare R2 Protocol & Platform Constraints

Understanding Cloudflare R2's specific behavior and S3 compatibility is essential for crafting `r2kit`'s transfer manager.

#### 3.1 Cloudflare R2 Limits & API Specifications

Cloudflare R2 provides an S3-compatible API with specific operational characteristics (documented in official Cloudflare R2 Platform Limits):

| Metric | Cloudflare R2 Specification | Notes / S3 Comparison |
| :--- | :--- | :--- |
| **Max Object Size** | 4.995 TiB (multi-part) / 5 GiB (single-part) | AWS S3 allows up to 5 TiB / 5 GiB |
| **Max Parts per Upload** | 10,000 parts | Identical to AWS S3 |
| **Part Size Limits** | 5 MiB to 5 GiB | Last part may be < 5 MiB (down to 1 byte) |
| **0-Byte Multipart Upload** | **Invalid / Rejected** | S3 API requires at least 1 part; empty manifest returns `MalformedXML` |
| **0-Byte Single PUT** | **Supported** | HTTP PUT with `Content-Length: 0` creates 0-byte object |
| **Key Write Concurrency** | 1 write per second per key | Rate limit on mutating the same key |
| **Bucket Operation Rate** | 50 per second | Rate limit on bucket management calls |
| **Part Replacement** | In-place replacement | Re-uploading part N replaces existing part N |

#### 3.2 Why 0-Byte Files Cannot Be Multipart Uploads

Under the S3 Multipart protocol implemented by R2:
1. `CreateMultipartUpload` succeeds and returns an `UploadId`.
2. Calling `UploadPart` with 0 bytes is either disallowed or, if accepted as Part 1, triggers `EntityTooSmall` during completion if multiple parts exist.
3. If no `UploadPart` calls are made and `CompleteMultipartUpload` is called with an empty `<CompleteMultipartUpload></CompleteMultipartUpload>` manifest, R2 rejects the request with `MalformedXML` or `InvalidRequest` ("You must specify at least one part").

Therefore:
> **Rule of S3/R2 Multipart Uploads:**  
> A multipart upload requires at least one non-empty part to complete. A 0-byte object **cannot** be created via multipart upload. It **must** be created via single-part `PutObject` with `Content-Length: 0`.

This protocol constraint proves that `r2kit`'s low-level `MultipartPlan` was technically correct to reject `file_size == 0`, but the lack of a higher-level Transfer Manager forced this protocol limitation onto end-user applications.

#### 3.3 Presigned Transfer Coordination vs Server-Side Streaming

An upload toolkit for R2 must accommodate two fundamentally distinct network architectures:

```
Architecture A: Server-Side Streaming (Managed Multipart)
[Client] ---> (Full Data Stream) ---> [App Server (r2kit)] ---> (SigV4 HTTPS) ---> [Cloudflare R2]

Architecture B: Presigned Coordination (Delegated / Direct-to-Storage)
[Client] ---> (Request Plan) --------> [App Server (r2kit)]
[Client] <--- (Presigned URLs Plan) -- [App Server (r2kit)]
[Client] ---> (Direct HTTPS PUT) -------------------------------------------------> [Cloudflare R2]
[Client] ---> (Complete / Notify) ---> [App Server (r2kit)] ---> (Complete MPU) ---> [Cloudflare R2]
```

**Key Trade-offs:**
- **Server-Side Streaming:** Ideal for CLI tools (`r2drive upload`), automated backend workers, and server-managed pipelines. The toolkit owns the bytes, buffer memory, concurrency pool, and retry loop.
- **Presigned Coordination:** Essential for web consoles (browser uploads), mobile apps, and distributed microservices. Prevents application servers from becoming network bandwidth bottlenecks. The toolkit coordinates URLs and manifests, but the remote client transmits the data payload.

A production-grade toolkit must provide clean, symmetrical abstractions for both models without leaking S3 protocol details.

---

### 4. Architectural Alternatives for r2kit

We evaluate three candidate designs for `r2kit`'s upload architecture:

---

#### Alternative 1: Fluent Smart Upload Extension on `Bucket`

Add high-level `upload_file` and `upload` methods directly to the `Bucket` struct.

```rust
impl Bucket {
    /// Uploads a file from local disk, automatically choosing single PUT or multipart.
    pub fn upload_file<'a>(&'a self, key: impl IntoObjectKey, path: impl AsRef<Path>) -> UploadFileBuilder<'a> { ... }

    /// Uploads an in-memory buffer or stream, automatically choosing single PUT or multipart.
    pub fn upload<'a>(&'a self, key: impl IntoObjectKey) -> UploadBuilder<'a> { ... }
}
```

**Usage:**
```rust
// Upload file with zero boilerplate:
let result = bucket.upload_file("backups/app.zip", "./app.zip").await?;

// Configured upload:
let result = bucket.upload_file("backups/app.zip", "./app.zip")
    .multipart_threshold(16 * 1024 * 1024)
    .concurrency(8)
    .on_progress(|p| println!("{:.1}%", p.percentage()))
    .await?;
```

**Internal Logic:**
1. Offline preflight: validates key, resolves path metadata.
2. If `file_size == 0` or `file_size < multipart_threshold`:
   - Issues `PutObject` (reads file or streams directly).
   - Emits a single 100% progress event.
   - Returns unified `TransferResult`.
3. If `file_size >= multipart_threshold`:
   - Delegates to the managed multipart pipeline.

**Strengths:**
- Unrivaled ergonomics: 90% of user tasks become a one-liner.
- Seamless mental model: users don't need to learn a new concept like "TransferManager" unless they want advanced tuning.
- Preserves backward compatibility: existing `managed_multipart` remains available for explicit multipart control.

**Weaknesses:**
- Adds methods to `Bucket`, slightly widening its API surface.
- Reusing configuration across 100 uploads requires either a wrapper function or repetitive builder configuration.

---

#### Alternative 2: Dedicated `TransferManager` Subsystem

Introduce a dedicated, standalone `TransferManager` struct analogous to AWS SDK's Transfer Manager.

```rust
pub struct TransferManager {
    bucket: Bucket,
    config: TransferConfig,
    buffer_pool: Arc<BufferPool>,
    rate_limiter: Option<RateLimiter>,
}

impl TransferManager {
    pub fn builder(bucket: Bucket) -> TransferManagerBuilder { ... }
    
    pub async fn upload_file(&self, key: impl IntoObjectKey, path: impl AsRef<Path>) -> Result<TransferResult, TransferError> { ... }
    pub async fn upload_bytes(&self, key: impl IntoObjectKey, bytes: Bytes) -> Result<TransferResult, TransferError> { ... }
    pub async fn download_file(&self, key: impl IntoObjectKey, destination: impl AsRef<Path>) -> Result<DownloadResult, TransferError> { ... }
}
```

**Usage:**
```rust
let tm = TransferManager::builder(bucket.clone())
    .multipart_threshold(8 * 1024 * 1024)
    .part_size(8 * 1024 * 1024)
    .concurrency(4)
    .max_memory_budget(128 * 1024 * 1024)
    .build()?;

// Reuse the same transfer manager across thousands of operations
tm.upload_file("empty.txt", "path/to/empty.txt").await?;
tm.upload_file("large.iso", "path/to/large.iso").await?;
```

**Strengths:**
- Clean separation of concerns: `Bucket` remains a pure protocol handle; `TransferManager` owns transfer orchestration.
- Global Resource Management: Enforces a global memory budget across multiple concurrent file uploads.
- Extensible: Naturally houses directory upload/download (`upload_directory`), adaptive bandwidth throttling, and connection pooling.

**Weaknesses:**
- Higher ceremony for simple scripts and microservices that only upload one file.
- Additional indirection if callers only interact with `Bucket`.

---

#### Alternative 3: Auto-Switching `ManagedMultipartBuilder` (In-Place Refactor)

Modify `Bucket::managed_multipart` so that it silently switches to a single PUT if the provided file is 0 bytes or below a minimum threshold.

```rust
// Internally in ManagedMultipartBuilder::upload_file:
if file_size < self.part_size {
    return self.execute_single_put(path, file_size).await;
}
```

**Strengths:**
- Maximum backward compatibility: zero new top-level types.
- Directly fixes the `MultipartFileSizeZero` crash in existing `r2drive` code.

**Weaknesses:**
- Semantic dishonesty: Calling `bucket.managed_multipart(key).upload_file(path)` to upload a 0-byte or 2-byte file does *not* do a multipart upload. Naming it "multipart" creates confusion in logging, metrics, and documentation.
- Doesn't solve the presigned upload coordination dilemma.
- Doesn't support in-memory byte buffers or streaming readers ergonomically.

---

#### Comparison Matrix of Alternatives

| Evaluation Dimension | Option 1: Fluent `Bucket` Extension | Option 2: Dedicated `TransferManager` | Option 3: In-Place Multipart Refactor |
| :--- | :--- | :--- | :--- |
| **Ergonomics for Common Tasks** | ⭐⭐⭐⭐⭐ (Excellent) | ⭐⭐⭐ (Moderate ceremony) | ⭐⭐⭐⭐ (Familiar) |
| **Semantic Accuracy** | ⭐⭐⭐⭐⭐ (Clean upload facade) | ⭐⭐⭐⭐⭐ (Clean subsystem) | ⭐⭐ (Misleading naming) |
| **ADR 0001 Deep Value Types** | ⭐⭐⭐⭐⭐ (Fully typed) | ⭐⭐⭐⭐⭐ (Fully typed) | ⭐⭐⭐ (Compromised) |
| **ADR 0001 Offline Preflight** | ⭐⭐⭐⭐⭐ (Validated up front) | ⭐⭐⭐⭐⭐ (Validated at build/exec) | ⭐⭐⭐⭐ (Validated) |
| **ADR 0001 Memory Budgeting** | ⭐⭐⭐⭐ (Per-upload limit) | ⭐⭐⭐⭐⭐ (Global cross-upload limit) | ⭐⭐⭐ (Per-upload only) |
| **Presigned Upload Coordination** | ⭐⭐⭐⭐ (Via helper builder) | ⭐⭐⭐⭐⭐ (Native plan coordinator) | ⭐ (Not addressed) |
| **Implementation Complexity** | Low–Medium | Medium–High | Low |

---

### 5. Recommended Architecture & Detailed Specification

We recommend a **Two-Tiered Hybrid Architecture** combining the simplicity of **Option 1 (Fluent Bucket Extension)** with the architectural rigor and engine foundation of **Option 2 (Dedicated Transfer Engine)**, alongside a unified **Presigned Upload Coordinator**.

```
+-------------------------------------------------------------------------+
|                              PUBLIC API                                 |
+-------------------------------------------------------------------------+
|                                                                         |
|  [Bucket Convenience Methods]            [TransferManager Handle]       |
|  - bucket.upload_file(key, path)         - TransferManager::new(bucket) |
|  - bucket.upload(key)                    - tm.upload_file(key, path)    |
|  - bucket.presign_upload(key, size)      - tm.upload_stream(key, stream)|
|                                                                         |
+------------------------------------+------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                        TRANSFER ENGINE (CORE)                           |
+-------------------------------------------------------------------------+
|                                                                         |
|  * Strategy Selector (file_size < threshold ? SinglePut : Multipart)    |
|  * Memory Budget Allocator & Chunk Buffer Pool                          |
|  * Progress Dispatcher (TransferProgress)                               |
|  * Cooperative Cancellation Watcher                                     |
|  * Session Checkpointer (MultipartSessionSnapshot)                      |
|                                                                         |
+------------------------------------+------------------------------------+
                                     |
            +------------------------+------------------------+
            |                                                 |
            v                                                 v
+-----------------------+                         +-----------------------+
|   SINGLE PUT RUNNER   |                         |   MULTIPART RUNNER    |
|  - Content-Length: N  |                         |  - Concurrency Pool   |
|  - 0-byte & small file|                         |  - Part Sizing Math   |
|  - 100% Progress Event|                         |  - Checkpointing      |
+-----------------------+                         +-----------------------+
```

---

#### 5.1 Deep Value Types and Offline Validation (ADR 0001 Compliance)

In accordance with ADR 0001, primitive numbers must not be passed raw. All settings are validated offline prior to network execution:

```rust
/// Threshold deciding between single PUT and multipart upload.
///
/// In S3 and R2, parts must be at least 5 MiB. Therefore, the threshold
/// must be >= 5 MiB. Defaults to 8 MiB.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UploadThreshold(u64);

impl UploadThreshold {
    pub const MIN_BYTES: u64 = 5 * 1024 * 1024; // 5 MiB
    pub const DEFAULT_BYTES: u64 = 8 * 1024 * 1024; // 8 MiB

    pub fn new(bytes: u64) -> Result<Self, ValidationError> {
        if bytes < Self::MIN_BYTES {
            return Err(ValidationError::ThresholdTooSmall {
                provided: bytes,
                min: Self::MIN_BYTES,
            });
        }
        Ok(Self(bytes))
    }

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
```

```rust
/// Configuration for managed upload transfers.
#[derive(Clone, Debug)]
pub struct TransferConfig {
    threshold: UploadThreshold,
    part_size: u64,
    concurrency: usize,
    max_attempts: u8,
    max_buffered_bytes: u64,
}

impl TransferConfig {
    pub fn builder() -> TransferConfigBuilder { ... }
}
```

---

#### 5.2 Unified Transfer Progress & Unified Result

A single progress structure represents updates across both single-part and multipart uploads:

```rust
/// Point-in-time transfer progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferProgress {
    transferred_bytes: u64,
    total_bytes: u64,
    is_multipart: bool,
    completed_parts: u16,
    total_parts: u16,
}

impl TransferProgress {
    #[must_use]
    pub const fn transferred_bytes(self) -> u64 { self.transferred_bytes }

    #[must_use]
    pub const fn total_bytes(self) -> u64 { self.total_bytes }

    #[must_use]
    pub fn fraction(self) -> f64 {
        if self.total_bytes == 0 {
            1.0
        } else {
            self.transferred_bytes as f64 / self.total_bytes as f64
        }
    }

    #[must_use]
    pub fn percentage(self) -> f64 {
        self.fraction() * 100.0
    }

    #[must_use]
    pub const fn is_multipart(self) -> bool { self.is_multipart }
}
```

```rust
/// Outcome of a unified transfer.
#[derive(Clone, Debug)]
pub struct TransferResult {
    key: ObjectKey,
    etag: String,
    size: u64,
    strategy: TransferStrategyUsed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferStrategyUsed {
    SinglePut,
    Multipart { part_count: u16 },
}
```

---

#### 5.3 Unified Presigned Upload Coordination

To eliminate the code duplication in `r2drive/src/r2/transfer.rs`, `r2kit` provides an offline-validated presigning coordinator:

```rust
/// A presigned upload coordination plan.
#[derive(Clone, Debug)]
pub enum PresignedUploadPlan {
    /// Single PUT request for files smaller than the multipart threshold.
    Single(PresignedPutObject),
    /// Multipart upload session with presigned parts for files meeting or exceeding the threshold.
    Multipart(PresignedMultipartPlan),
}

impl PresignedUploadPlan {
    /// Returns whether this upload plan requires multipart coordination.
    #[must_use]
    pub fn is_multipart(&self) -> bool {
        matches!(self, Self::Multipart(_))
    }
}
```

**Fluent Coordinator Method on `Bucket`:**
```rust
impl Bucket {
    /// Creates a coordinated presigned upload plan for an object of known size.
    ///
    /// Automatically selects between a single presigned PUT and a presigned multipart
    /// upload session based on the specified or default threshold.
    pub async fn presign_upload(
        &self,
        key: impl IntoObjectKey,
        file_size: u64,
        expires_in: Duration,
    ) -> Result<PresignedUploadPlan, Error> {
        self.presign_upload_with_options(key, file_size, expires_in, ObjectUploadOptions::default()).await
    }
}
```

**Downstream Impact on `r2drive`:**
Instead of maintaining 100+ lines of duplicated code branching between `init_single_presigned_upload` and `init_presigned_upload`, `r2drive` reduces its endpoint to:

```rust
// r2drive web backend endpoint:
let plan = bucket.presign_upload(&key, file_size, Duration::from_secs(3600)).await?;
match plan {
    PresignedUploadPlan::Single(put) => {
        // Return single URL to browser
    }
    PresignedUploadPlan::Multipart(multi) => {
        // Return session and part URLs to browser
    }
}
```

---

#### 5.4 Ergonomic Fluent Builder: `bucket.upload_file(key, path)`

The primary consumer interface for local file transfers:

```rust
let result = bucket.upload_file("documents/report.pdf", "./report.pdf")
    .options(ObjectUploadOptions::default().with_content_type(mime::APPLICATION_PDF))
    .on_progress(|progress| {
        println!("Progress: {:.2}% ({} / {} bytes)", 
            progress.percentage(), 
            progress.transferred_bytes(), 
            progress.total_bytes()
        );
    })
    .cancellation_token(cancellation_signal)
    .await?;
```

**Execution Flow:**
1. **Preflight Checks:**
   - Validates `ObjectKey`.
   - Checks that `path` exists and is a regular file.
   - Retrieves `file_size = metadata.len()`.
   - Validates memory budget: `concurrency * part_size <= max_buffered_bytes`.
2. **Strategy Selection:**
   - **Case A: `file_size == 0`:**
     - Executes single PUT with empty body.
     - Emits initial progress (0/0) and completed progress (0/0, 100%).
     - Returns `TransferResult` with `TransferStrategyUsed::SinglePut`.
   - **Case B: `file_size < threshold`:**
     - Executes single PUT using tokio file stream.
     - Emits progress events.
     - Returns `TransferResult` with `TransferStrategyUsed::SinglePut`.
   - **Case C: `file_size >= threshold`:**
     - Delegates to the managed multipart engine with concurrency, retry backoff, and chunk buffer pooling.
     - Emits per-part progress.
     - Returns `TransferResult` with `TransferStrategyUsed::Multipart`.

---

#### 5.5 Handling Resumption and Cancellation

- **Cancellation:** Built on `tokio::sync::watch` (as in `ManagedUploadCancellation`). If cancelled during a single PUT, the in-flight HTTP request is aborted immediately. If cancelled during a multipart upload, the transfer loop exits cooperatively and optionally retains or aborts the session based on `abort_on_cancel`.
- **Resumption:** If a transfer is interrupted, callers can obtain a `MultipartSessionSnapshot`. Resumption is only applicable to multipart transfers; calling resume on a file smaller than the threshold simply uploads it via single PUT afresh.

---

### 6. Primary Source Citations & References

1. **Cloudflare R2 Limits Documentation:**  
   Cloudflare, Inc. *Platform Limits - Cloudflare R2 Documentation*.  
   URL: https://developers.cloudflare.com/r2/platform/limits/  
   *(Referenced for 4.995 TiB object limit, 5 GiB single-part limit, 10,000 part ceiling, 50 ops/sec bucket limit, and 1 write/sec per key constraint).*

2. **Cloudflare R2 S3 Compatibility API Reference:**  
   Cloudflare, Inc. *S3 API Compatibility - Cloudflare R2 Documentation*.  
   URL: https://developers.cloudflare.com/r2/api/s3/api/  
   *(Referenced for supported multipart actions: CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts).*

3. **Amazon S3 Multipart Upload Overview:**  
   Amazon Web Services, Inc. *Uploading and copying objects using multipart upload*. AWS S3 User Guide.  
   URL: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html  
   *(Referenced for 100 MB best practice threshold, 5 MiB part size minimum, and last-part exception).*

4. **Amazon S3 Multipart Upload Limits & Specifications:**  
   Amazon Web Services, Inc. *Amazon S3 multipart upload limits*. AWS S3 User Guide.  
   URL: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html  
   *(Referenced for Part numbers 1 to 10,000, 5 MiB to 5 GiB part sizing, and 0-byte part constraints).*

5. **AWS Boto3 S3Transfer Documentation & Source:**  
   Amazon Web Services, Inc. *boto3.s3.transfer.TransferConfig Reference*.  
   URL: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html  
   *(Referenced for default 8 MiB `multipart_threshold`, 8 MiB `multipart_chunksize`, and 0-byte single PUT fallback).*

6. **AWS SDK for Java 2.x S3 Transfer Manager Guide:**  
   Amazon Web Services, Inc. *Manage Amazon S3 transfers with S3 Transfer Manager*.  
   URL: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/transfer-manager.html  
   *(Referenced for `UploadFileRequest`, `FileUpload`, and asynchronous transfer execution).*

7. **AWS SDK for Java 2.x Multipart Configuration:**  
   Amazon Web Services, Inc. *Configure parallel transfer support*.  
   URL: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/s3-async-client-multipart.html  
   *(Referenced for 8 MiB default `thresholdInBytes` and 8 MiB `minimumPartSizeInBytes` settings).*

8. **AWS SDK for Go v2 S3 Manager Package:**  
   Amazon Web Services, Inc. *Package manager (feature/s3/manager)*. pkg.go.dev.  
   URL: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager  
   *(Referenced for `DefaultUploadPartSize = 5 MiB`, `MinUploadPartSize = 5 MiB`, and stream inspection logic).*

9. **AWS SDK for Rust Transfer Manager Crate:**  
   Amazon Web Services, Inc. *Crate aws_sdk_s3_transfer_manager*. docs.rs.  
   URL: https://docs.rs/aws-sdk-s3-transfer-manager/latest/aws_sdk_s3_transfer_manager/  
   *(Referenced for `UploadInput`, `InputStream`, `MemoryBudgetConfig`, and `UploadFluentBuilder` architectures).*

10. **Apache OpenDAL Operator & Writer Documentation:**  
    Apache Software Foundation. *Crate opendal - Operator and Writer*. docs.rs.  
    URL: https://docs.rs/opendal/latest/opendal/struct.Operator.html  
    *(Referenced for `write_once` vs multi-part switching and adaptive buffering).*

11. **Apache Arrow Object Store Buffered Writer:**  
    Apache Software Foundation. *Crate object_store - Module buffered::BufWriter*. docs.rs.  
    URL: https://docs.rs/object_store/latest/object_store/buffered/struct.BufWriter.html  
    *(Referenced for adaptive capacity switching between `put_opts` and `put_multipart_opts`)*.
