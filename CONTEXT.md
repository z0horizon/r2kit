# r2kit

A safe, ergonomic Rust toolkit for Cloudflare R2 object storage with offline preflight validation and managed multipart transfers.

## Language

**ObjectKey**:
A validated UTF-8 string of 1 through 1,024 bytes identifying a stored object within an R2 bucket.
_Avoid_: Key, path, filename, object path

**BucketName**:
A validated bucket identifier of 3 through 63 lowercase ASCII alphanumeric characters or interior hyphens.
_Avoid_: Bucket, bucket identifier, bucket ID

**Bucket**:
An operational handle scoped to a single R2 bucket providing object and multipart operations.
_Avoid_: BucketService, BucketClient

**R2Client**:
An authenticated client configured for Cloudflare R2 holding account credentials, endpoints, and timeouts.
_Avoid_: S3Client, Client, StorageClient

**MultipartSession**:
An in-progress multipart upload workflow tracking upload ID, parts plan, and completion manifest.
_Avoid_: MultipartUpload, UploadSession

**TransferManager**:
An operational engine coordinating object transfers with adaptive strategy selection, concurrency pools, and memory budgeting.
_Avoid_: TransferService, UploadQueue, TransferPool

**UploadThreshold**:
A validated byte limit (at least 5 MiB, defaulting to 8 MiB) governing the boundary between atomic single-part PUT operations and multi-part transfers.
_Avoid_: ChunkThreshold, CutoffSize, MinMultipartSize

**PresignedUploadPlan**:
A preflight-validated transfer coordination plan resolving to either a single presigned PUT or a multipart upload session for remote clients.
_Avoid_: PresignedPlan, UploadScheme
