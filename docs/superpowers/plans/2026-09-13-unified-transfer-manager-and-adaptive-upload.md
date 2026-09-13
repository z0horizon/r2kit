# Unified Transfer Manager and Adaptive Upload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Unified Transfer Manager and adaptive upload architecture in `r2kit`, providing adaptive single PUT vs multipart transfers, 0-byte file support, direct multipart abort, canonical `NotFound` error representations, streaming object pagination, and refined caller ergonomics.

**Architecture:** A two-tiered hybrid architecture combining high-level convenience facades (`Bucket::upload_file`, `Bucket::upload`, and `Bucket::presign_upload`) with a dedicated `TransferManager` engine managing concurrency and memory budgets. This architecture eliminates upfront manual branching and protocol impedance mismatches identified during downstream consumption in `r2drive`.

**Tech Stack:** Rust 1.94+ (2024 edition), `aws-sdk-s3`, Tokio (fs, io-util, sync, time), `futures-util`, `bytes`, `mime`, `serde`.

**Spec:** [ADR 0002: Unified Transfer Manager and Adaptive Upload Strategy](file:///Users/trungdt/Workspace/lib/r2kit/docs/adr/0002-unified-transfer-manager-and-adaptive-upload.md) and [Research Report](file:///Users/trungdt/Workspace/lib/r2kit/docs/research/unified-transfer-manager-and-upload-abstractions.md).

## Global Constraints

- **Offline Validation:** Validate all limits, thresholds, and dimensions offline before issuing any network requests (ADR 0001).
- **Deep Value Types:** Enforce strong typing via newtypes (`UploadThreshold`, `ObjectKey`, `BucketName`) and conversion traits (`IntoObjectKey`, `IntoBucketName`, `IntoContentType`).
- **Memory Safety & Budgeting:** Concurrency and buffer allocations must be bounded by memory budgets (`max_buffered_bytes`).
- **Secret Redaction:** `Debug` implementations of presigned requests, snapshots, and credentials must never expose bearer query signatures or secrets.
- **Backwards Compatibility:** Existing primitives (`put_bytes`, `managed_multipart`, `presigned_multipart`) remain valid and unchanged.
- **Zero Warnings:** All code must compile cleanly with `cargo clippy --all-targets -- -D warnings` and `cargo fmt --all -- --check`.

---

## File Structure & Module Map

| File Path | Responsibility | Changes |
| :--- | :--- | :--- |
| `src/error.rs` | Domain error definitions and mapping | Add `Error::is_not_found()`, unify 404 remote service errors to `Error::NotFound` |
| `src/object.rs` | Bucket object operations and listing builders | Add `Bucket::abort_multipart_upload`, add `ListObjectsBuilder::into_stream` / `into_objects` |
| `src/types.rs` | Deep value types and conversion traits | Add `IntoContentType` trait, `UploadThreshold` newtype |
| `src/multipart.rs` | Multipart session, presigning, and snapshot | Derive `Serialize, Deserialize` on `MultipartSessionSnapshot`, add URL string accessors |
| `src/managed.rs` | Managed multipart and adaptive transfer engine | Add `TransferProgress`, `TransferResult`, `TransferConfig`, `TransferManager`, `Bucket::upload_file` |
| `src/lib.rs` | Public crate root exports | Re-export new public types and builders |
| `tests/object_contract.rs` | Contract test suite for object operations | Test direct abort, streaming object pagination, `NotFound` unification |
| `tests/managed_contract.rs` | Contract test suite for managed transfers | Test 0-byte file upload, sub-threshold PUT, multipart execution, cancellation |
| `tests/presign_contract.rs` | Contract test suite for presigned transfers | Test `PresignedUploadPlan`, URL string accessors, `IntoContentType` |
| `tests/public_api.rs` | Public API surface and Send/Sync verification | Test thread-safety, serialization round-trips, doc tests |

---

### Task 1: Direct Multipart Abort on `Bucket` & Canonical `NotFound` Error Representation

**Files:**
- Modify: `src/error.rs`
- Modify: `src/object.rs`
- Test: `tests/object_contract.rs`

- [ ] **Step 1.1: Write failing contract tests for `abort_multipart_upload` and `is_not_found`**
  In `tests/object_contract.rs`, add tests verifying:
  1. `bucket.abort_multipart_upload("empty-key", "upload-id")` fails offline with `InvalidInput` if key or upload_id is invalid.
  2. Calling `abort_multipart_upload` dispatches S3 `AbortMultipartUpload` request.
  3. `error.is_not_found()` returns `true` for `Error::NotFound` and `Error::Remote(se)` where `se.kind() == ServiceErrorKind::NotFound`.
  4. Remote HTTP 404 errors map canonically to `Error::NotFound`.

- [ ] **Step 1.2: Run tests and verify failure**
  Run: `cargo test --test object_contract`
  Confirm failure due to missing method `abort_multipart_upload` and `is_not_found`.

- [ ] **Step 1.3: Implement `is_not_found` and canonical 404 mapping in `src/error.rs`**
  Add helper to `Error`:
  ```rust
  impl Error {
      #[must_use]
      pub fn is_not_found(&self) -> bool {
          matches!(self, Self::NotFound | Self::Remote(se) if se.kind() == ServiceErrorKind::NotFound)
      }
  }
  ```
  Ensure `ServiceError::from_sdk` or `Error::from_sdk` maps HTTP 404 status codes to `Error::NotFound` when appropriate, preserving consistency.

- [ ] **Step 1.4: Implement `Bucket::abort_multipart_upload` in `src/object.rs`**
  Add public method to `Bucket`:
  ```rust
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
          .map_err(|err| self.client.map_sdk_error("AbortMultipartUpload", &err))?;
      Ok(())
  }
  ```

- [ ] **Step 1.5: Run tests and verify pass**
  Run: `cargo test --test object_contract`
  Confirm all tests pass.

- [ ] **Step 1.6: Commit**
  Run: `git add src/error.rs src/object.rs tests/object_contract.rs && git commit -m "feat(object): add direct Bucket::abort_multipart_upload and canonical is_not_found helper"`

---

### Task 2: Streaming Object Pagination (`into_stream` / `into_objects`)

**Files:**
- Modify: `src/object.rs`
- Test: `tests/object_contract.rs`

- [ ] **Step 2.1: Write failing test for `into_stream` and `into_objects`**
  In `tests/object_contract.rs`, add:
  ```rust
  #[test]
  fn listing_exposes_a_sendable_object_stream() {
      fn assert_send<T: Send>(_: &T) {}
      let stream = offline_bucket().list().prefix("logs/").into_stream();
      assert_send(&stream);
  }
  ```

- [ ] **Step 2.2: Run test and verify failure**
  Run: `cargo test --test object_contract listing_exposes_a_sendable_object_stream`
  Confirm compilation failure due to missing method `into_stream`.

- [ ] **Step 2.3: Implement `into_stream` on `ListObjectsBuilder` in `src/object.rs`**
  Use `futures_util::stream::StreamExt` to flatten `into_pages()` into a stream of items:
  ```rust
  pub fn into_stream(self) -> impl Stream<Item = Result<ObjectItem, Error>> + Send {
      self.into_pages()
          .map(|page_res| match page_res {
              Ok(page) => {
                  let items: Vec<Result<ObjectItem, Error>> = page.into_objects().into_iter().map(Ok).collect();
                  stream::iter(items)
              }
              Err(err) => stream::iter(vec![Err(err)]),
          })
          .flatten()
  }

  #[inline]
  pub fn into_objects(self) -> impl Stream<Item = Result<ObjectItem, Error>> + Send {
      self.into_stream()
  }
  ```

- [ ] **Step 2.4: Run tests and verify pass**
  Run: `cargo test --test object_contract listing_exposes_`
  Confirm stream and page tests pass.

- [ ] **Step 2.5: Commit**
  Run: `git add src/object.rs tests/object_contract.rs && git commit -m "feat(object): add into_stream and into_objects pagination streams to ListObjectsBuilder"`

---

### Task 3: Ergonomics & Serialization Refinement (`IntoContentType`, URL String Helpers, Snapshot Serde)

**Files:**
- Modify: `src/types.rs`
- Modify: `src/multipart.rs`
- Modify: `src/object.rs`
- Test: `tests/presign_contract.rs`
- Test: `tests/public_api.rs`

- [ ] **Step 3.1: Write failing tests for `IntoContentType`, URL string accessors, and Snapshot serde**
  In `tests/presign_contract.rs` and `tests/public_api.rs`:
  1. Test passing `"application/json"` (`&str`), `String::from("text/plain")`, and `mime::APPLICATION_OCTET_STREAM` to `with_content_type`.
  2. Test `presigned.as_str()` and `presigned.into_url_string()` returning the valid URL while `Debug` preserves `[REDACTED]`.
  3. Test `serde_json::to_string(&snapshot)` and `serde_json::from_str::<MultipartSessionSnapshot>(&json)` round-trip.

- [ ] **Step 3.2: Run tests and verify failure**
  Run: `cargo test --test presign_contract`
  Confirm failure due to missing methods and traits.

- [ ] **Step 3.3: Implement `IntoContentType` trait in `src/types.rs`**
  ```rust
  pub trait IntoContentType {
      fn into_content_type(self) -> Result<mime::Mime, Error>;
  }

  impl IntoContentType for mime::Mime {
      fn into_content_type(self) -> Result<mime::Mime, Error> {
          Ok(self)
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
  ```
  Update `ObjectUploadOptions::with_content_type` and `PresignedMultipartBuilder::content_type` to accept `impl IntoContentType`.

- [ ] **Step 3.4: Implement URL accessors on `PresignedRequest` and `PresignedPutObject` in `src/multipart.rs`**
  Add methods:
  ```rust
  impl PresignedRequest {
      #[must_use]
      pub fn as_str(&self) -> &str {
          self.url.expose()
      }

      #[must_use]
      pub fn into_url_string(self) -> String {
          self.url.into_exposed_string()
      }
  }
  ```
  Forward accessors to `PresignedPutObject`.

- [ ] **Step 3.5: Derive `Serialize` and `Deserialize` on `MultipartSessionSnapshot`**
  Add `#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]` to `MultipartSessionSnapshot` in `src/multipart.rs`.

- [ ] **Step 3.6: Run tests and verify pass**
  Run: `cargo test --test presign_contract && cargo test --test public_api`
  Confirm all tests pass.

- [ ] **Step 3.7: Commit**
  Run: `git add src/types.rs src/multipart.rs src/object.rs tests/presign_contract.rs tests/public_api.rs && git commit -m "feat(transfer): add IntoContentType, URL string accessors, and direct snapshot serialization"`

---

### Task 4: Core Value Types & Coordinated Presigned Upload Plan

**Files:**
- Modify: `src/types.rs`
- Modify: `src/multipart.rs`
- Modify: `src/object.rs`
- Test: `tests/presign_contract.rs`

- [ ] **Step 4.1: Write failing tests for `UploadThreshold` and `Bucket::presign_upload`**
  In `tests/presign_contract.rs`, add tests:
  1. `UploadThreshold::new(4 * 1024 * 1024)` fails validation (< 5 MiB).
  2. `UploadThreshold::new(8 * 1024 * 1024)` succeeds.
  3. `bucket.presign_upload(key, 2 * 1024 * 1024, expires_in)` returns `PresignedUploadPlan::Single`.
  4. `bucket.presign_upload(key, 12 * 1024 * 1024, expires_in)` returns `PresignedUploadPlan::Multipart`.

- [ ] **Step 4.2: Run tests and verify failure**
  Run: `cargo test --test presign_contract`
  Confirm missing types and methods.

- [ ] **Step 4.3: Implement `UploadThreshold` in `src/types.rs`**
  ```rust
  #[derive(Clone, Copy, Debug, Eq, PartialEq)]
  pub struct UploadThreshold(u64);

  impl UploadThreshold {
      pub const MIN_BYTES: u64 = 5 * 1024 * 1024;
      pub const DEFAULT_BYTES: u64 = 8 * 1024 * 1024;

      pub fn new(bytes: u64) -> Result<Self, ValidationError> {
          if bytes < Self::MIN_BYTES {
              return Err(ValidationError::PartSizeOutOfRange {
                  provided: bytes,
                  min: Self::MIN_BYTES,
                  max: crate::types::MAX_MULTIPART_OBJECT_SIZE,
              });
          }
          Ok(Self(bytes))
      }

      #[must_use]
      pub const fn get(self) -> u64 { self.0 }
  }

  impl Default for UploadThreshold {
      fn default() -> Self { Self(Self::DEFAULT_BYTES) }
  }
  ```

- [ ] **Step 4.4: Implement `PresignedUploadPlan` in `src/multipart.rs` and `Bucket::presign_upload` in `src/object.rs`**
  ```rust
  #[derive(Clone)]
  pub enum PresignedUploadPlan {
      Single(PresignedPutObject),
      Multipart(PresignedMultipartPlan),
  }
  ```
  In `Bucket`:
  ```rust
  pub async fn presign_upload(
      &self,
      key: impl IntoObjectKey,
      file_size: u64,
      expires_in: Duration,
  ) -> Result<PresignedUploadPlan, Error> {
      self.presign_upload_with_options(key, file_size, expires_in, ObjectUploadOptions::default()).await
  }

  pub async fn presign_upload_with_options(
      &self,
      key: impl IntoObjectKey,
      file_size: u64,
      expires_in: Duration,
      options: ObjectUploadOptions,
  ) -> Result<PresignedUploadPlan, Error> {
      let key = key.into_object_key()?;
      let threshold = UploadThreshold::default().get();
      if file_size < threshold {
          let put = self.presign_put_with_options(&key, file_size, expires_in, options).await?;
          Ok(PresignedUploadPlan::Single(put))
      } else {
          let session = self.presigned_multipart(&key)?
              .file_size(file_size)
              .upload_options(options)
              .create()
              .await?;
          Ok(PresignedUploadPlan::Multipart(PresignedMultipartPlan::from_session(session)))
      }
  }
  ```

- [ ] **Step 4.5: Run tests and verify pass**
  Run: `cargo test --test presign_contract`
  Confirm all tests pass.

- [ ] **Step 4.6: Commit**
  Run: `git add src/types.rs src/multipart.rs src/object.rs tests/presign_contract.rs && git commit -m "feat(presign): implement UploadThreshold and coordinated PresignedUploadPlan"`

---

### Task 5: Adaptive `TransferManager` Engine & `Bucket::upload_file` Facade

**Files:**
- Modify: `src/managed.rs`
- Modify: `src/object.rs`
- Test: `tests/managed_contract.rs`

- [ ] **Step 5.1: Write failing tests for `upload_file` on 0-byte, small, and large files**
  In `tests/managed_contract.rs`, add tests verifying:
  1. `bucket.upload_file("empty.txt", empty_temp_file).await` succeeds (0 bytes, strategy SinglePut).
  2. `bucket.upload_file("small.txt", 1mb_temp_file).await` succeeds (strategy SinglePut).
  3. `bucket.upload_file("large.bin", 12mb_temp_file).await` executes multipart upload.
  4. Progress callbacks report accurate byte counts and percentages.
  5. Cooperative cancellation stops the transfer promptly.

- [ ] **Step 5.2: Run tests and verify failure**
  Run: `cargo test --test managed_contract upload_file`
  Confirm compilation failure due to missing `upload_file`.

- [ ] **Step 5.3: Implement `TransferProgress`, `TransferResult`, and `UploadFileBuilder` in `src/managed.rs`**
  Add progress structs:
  ```rust
  #[derive(Clone, Copy, Debug, Eq, PartialEq)]
  pub struct TransferProgress {
      transferred_bytes: u64,
      total_bytes: u64,
      is_multipart: bool,
  }

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

- [ ] **Step 5.4: Implement strategy dispatch in `UploadFileBuilder`**
  If `file_size == 0`:
  Read 0 bytes, dispatch `put_bytes(key, Vec::new())`, fire progress (0/0, 100%), return `TransferResult`.
  If `file_size < threshold`:
  Dispatch single-part PUT streaming from file via `tokio::fs::File`, fire progress events, return `TransferResult`.
  If `file_size >= threshold`:
  Delegate to `ManagedMultipartBuilder` execution pipeline, return `TransferResult`.

- [ ] **Step 5.5: Wire `Bucket::upload_file` in `src/object.rs`**
  ```rust
  impl Bucket {
      pub fn upload_file<'a>(
          &'a self,
          key: impl IntoObjectKey,
          path: impl AsRef<Path>,
      ) -> UploadFileBuilder<'a> {
          UploadFileBuilder::new(self.clone(), key, path)
      }
  }
  ```

- [ ] **Step 5.6: Run tests and verify pass**
  Run: `cargo test --test managed_contract`
  Confirm all tests pass, especially 0-byte file and small file tests.

- [ ] **Step 5.7: Commit**
  Run: `git add src/managed.rs src/object.rs tests/managed_contract.rs && git commit -m "feat(managed): add Bucket::upload_file with adaptive single PUT and multipart dispatch"`

---

### Task 6: Public API Exports, Workspace Verification & Migration Guide

**Files:**
- Modify: `src/lib.rs`
- Modify: `README.md`
- Create: `docs/migration-0.3.0.md`

- [ ] **Step 6.1: Re-export all new public types in `src/lib.rs`**
  Re-export:
  - `UploadThreshold`
  - `TransferProgress`
  - `TransferResult`
  - `TransferStrategyUsed`
  - `PresignedUploadPlan`
  - `IntoContentType`
  - `UploadFileBuilder`

- [ ] **Step 6.2: Update `README.md` with modern upload examples**
  Add clear documentation showing:
  - `bucket.upload_file("key", "file.pdf").await`
  - `bucket.abort_multipart_upload("key", upload_id).await`
  - `bucket.presign_upload("key", size, duration).await`
  - `bucket.list().into_stream()`

- [ ] **Step 6.3: Document migration guide in `docs/migration-0.3.0.md`**
  Explain how downstream applications (`r2drive`) can eliminate manual 0-byte bypasses and simplify presigned upload routing.

- [ ] **Step 6.4: Run full verification suite**
  Run:
  - `cargo fmt --all -- --check`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test --all-targets`
  - `cargo test --doc`
  Confirm 0 errors, 0 warnings across entire workspace.

- [ ] **Step 6.5: Commit**
  Run: `git add src/lib.rs README.md docs/migration-0.3.0.md && git commit -m "docs: re-export unified transfer types, update README and migration guide"`
