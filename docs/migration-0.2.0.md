# Migrating from 0.1.x to 0.2.0

## Breaking changes

### `ObjectUploadOptionsBuilder` removed

```rust
// Before (0.1.x)
let options = ObjectUploadOptions::builder()
    .content_type(mime::IMAGE_JPEG)
    .cache_control(CacheControl::new().with_public())
    .build();

// After (0.2.0)
let options = ObjectUploadOptions::new()
    .with_content_type(mime::IMAGE_JPEG)
    .with_cache_control(CacheControl::new().with_public());
```

The builder struct is gone. `ObjectUploadOptions::new()` returns a ready-to-use
value with chainable `.with_*()` methods. No `.build()` call needed.

### `into_string()` removed from `ObjectKey` and `BucketName`

```rust
// Before (0.1.x)
let s: String = key.into_string();

// After (0.2.0)
let s: String = key.into_inner();
```

Both newtypes now expose only `.into_inner()` for unwrapping.

## New features

### `get_bytes` — download an object into memory

```rust
let result = bucket.get_bytes("photos/cat.jpg").await?;
println!("{} bytes, etag = {:?}", result.bytes.len(), result.metadata.etag());
```

### `presign_delete` — presigned DELETE URL

```rust
let signed = bucket.presign_delete("tmp/file.bin", Duration::from_secs(300)).await?;
let (method, url, headers) = signed.into_exposed_parts();
```

### `upload_stream` — streaming multipart from any `AsyncRead`

```rust
use tokio::io::AsyncRead;

let reader = tokio::fs::File::open("video.mp4").await?;
let file_size = reader.metadata().await?.len();

bucket
    .managed_multipart("videos/clip.mp4")?
    .part_size_mib(10)
    .concurrency(4)
    .upload_stream(reader, file_size)
    .await?;
```

Unlike `upload_file`, `upload_stream` accepts any `impl AsyncRead + Unpin + Send + 'static`
source. The file size must be known upfront; unknown-length streams are planned for 0.3.0.
