use std::time::{Duration, UNIX_EPOCH};
use std::{env, time::SystemTime};

use futures_util::TryStreamExt;
use r2kit::{CacheControl, Error, ObjectUploadOptions, R2Client, R2Config, mime};

fn live_client() -> R2Client {
    assert_eq!(env::var("R2KIT_LIVE_TESTS").as_deref(), Ok("1"));
    assert_eq!(
        env::var("R2KIT_LIVE_BUCKET").as_deref(),
        Ok("r2kit-live-tests")
    );
    R2Client::new(R2Config::from_env().expect("R2 live credentials are required"))
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_bucket_preflight_confirms_read_access() {
    live_client()
        .validate_bucket("r2kit-live-tests")
        .await
        .expect("dedicated live bucket must be accessible");
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_core_object_round_trip_and_pagination() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let prefix = format!("_r2kit-tests/{}/objects/", uuid::Uuid::new_v4());
    let first_key = format!("{prefix}a.txt");
    let second_key = format!("{prefix}b.txt");
    let copied_key = format!("{prefix}copied file.txt");
    let first_body = b"r2kit object API: first".to_vec();
    let second_body = vec![
        0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x2b, 0x32, 0xca, 0xce, 0x2c,
        0x51, 0xc8, 0x4f, 0xca, 0x4a, 0x4d, 0x2e, 0x51, 0x70, 0x0c, 0xf0, 0xb4, 0x52, 0x28, 0x4e,
        0x4d, 0xce, 0xcf, 0x4b, 0x01, 0x00, 0xe2, 0x09, 0xb2, 0x17, 0x18, 0x00, 0x00, 0x00,
    ];
    let expires = UNIX_EPOCH + Duration::from_secs(1_893_456_000);

    let result = async {
        let options = ObjectUploadOptions::builder()
            .content_type(mime::TEXT_PLAIN_UTF_8)
            .content_disposition("attachment; filename=a.txt")
            .content_language("en-US, vi")
            .expires(expires)
            .custom_metadata("test-run", "object-round-trip")
            .custom_metadata("tenant-id", "tenant-42")
            .build();
        let put = bucket
            .put_bytes_with_options(&first_key, first_body.clone(), options)
            .await
            .map_err(|_| "first put failed")?;
        let encoded_options = ObjectUploadOptions::builder()
            .content_type(mime::TEXT_PLAIN_UTF_8)
            .content_encoding("gzip")
            .build();
        bucket
            .put_bytes_with_options(&second_key, second_body.clone(), encoded_options)
            .await
            .map_err(|_| "second put failed")?;

        let metadata = bucket.head(&first_key).await.map_err(|_| "head failed")?;
        if metadata.size() != first_body.len() as u64
            || metadata.etag() != put.etag()
            || metadata.content_disposition() != Some("attachment; filename=a.txt")
            || metadata.content_language() != Some("en-US, vi")
            || metadata.expires() != Some(expires)
            || metadata.custom().get("test-run").map(String::as_str) != Some("object-round-trip")
            || metadata.custom().get("tenant-id").map(String::as_str) != Some("tenant-42")
        {
            return Err("head metadata differs from put result");
        }

        let download = bucket.get(&first_key).await.map_err(|_| "get failed")?;
        if download.metadata().size() != first_body.len() as u64 {
            return Err("download metadata has wrong size");
        }
        let actual = download
            .into_body()
            .collect()
            .await
            .map_err(|_| "download body failed")?
            .into_bytes();
        if actual.as_ref() != first_body {
            return Err("downloaded bytes differ");
        }

        let encoded_metadata = bucket
            .head(&second_key)
            .await
            .map_err(|_| "encoded object head failed")?;
        if encoded_metadata.content_encoding() != Some("gzip") {
            return Err("encoded object metadata differs");
        }
        let encoded = bucket
            .get(&second_key)
            .await
            .map_err(|_| "encoded object get failed")?
            .into_body()
            .collect()
            .await
            .map_err(|_| "encoded object body failed")?
            .into_bytes();
        if encoded.as_ref() != second_body {
            return Err("encoded object bytes differ");
        }

        let copied = bucket
            .copy(&first_key, &copied_key)
            .await
            .map_err(|_| "server-side copy failed")?;
        let copied_metadata = bucket
            .head(&copied_key)
            .await
            .map_err(|_| "copied object head failed")?;
        if copied.etag() != copied_metadata.etag()
            || copied_metadata
                .custom()
                .get("tenant-id")
                .map(String::as_str)
                != Some("tenant-42")
        {
            return Err("copied object metadata differs");
        }

        let pages: Vec<_> = bucket
            .list()
            .prefix(&prefix)
            .limit(1)
            .into_pages()
            .try_collect()
            .await
            .map_err(|_| "page stream failed")?;
        if pages.len() != 3 || pages.iter().any(|page| page.objects().len() != 1) {
            return Err("page stream must return three one-object pages");
        }

        let deleted = bucket
            .delete_objects([&first_key, &second_key, &copied_key])
            .await
            .map_err(|_| "batch delete request failed")?;
        if !deleted.is_complete() || deleted.deleted_keys().len() != 3 {
            return Err("batch delete did not report all keys as deleted");
        }
        if !matches!(bucket.head(&first_key).await, Err(Error::NotFound)) {
            return Err("batch-deleted key is still readable");
        }
        Ok::<(), &'static str>(())
    }
    .await;

    let _ = bucket.delete(&first_key).await;
    let _ = bucket.delete(&second_key).await;
    let _ = bucket.delete(&copied_key).await;
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_presigned_put_and_get_round_trip() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let key = format!("_r2kit-tests/{}/presigned.bin", uuid::Uuid::new_v4());
    let body = b"r2kit presigned object contract".to_vec();
    let expires: SystemTime = UNIX_EPOCH + Duration::from_secs(1_893_456_000);

    let result = async {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "failed to build HTTP client")?;
        let options = ObjectUploadOptions::builder()
            .content_type(mime::IMAGE_JPEG)
            .cache_control(
                CacheControl::new()
                    .with_public()
                    .with_max_age(Duration::from_secs(3_600)),
            )
            .content_disposition("attachment; filename=presigned.bin")
            .content_language("en-US")
            .expires(expires)
            .custom_metadata("upload-mode", "presigned")
            .build();
        let put = bucket
            .presign_put_with_options(&key, body.len() as u64, Duration::from_secs(900), options)
            .await
            .map_err(|_| "PUT presign failed")?;
        let (method, url, headers) = put.into_request().into_exposed_parts();
        let method = method.parse().map_err(|_| "invalid PUT method")?;
        let mut request = http.request(method, url).body(body.clone());
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await.map_err(|_| "PUT transport failed")?;
        if !response.status().is_success() {
            return Err("R2 rejected presigned PUT");
        }

        let get = bucket
            .presign_get(&key, Duration::from_secs(900))
            .await
            .map_err(|_| "GET presign failed")?;
        let (method, url, headers) = get.into_exposed_parts();
        let method = method.parse().map_err(|_| "invalid GET method")?;
        let mut request = http.request(method, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await.map_err(|_| "GET transport failed")?;
        if !response.status().is_success() {
            return Err("R2 rejected presigned GET");
        }
        let actual = response.bytes().await.map_err(|_| "GET body failed")?;
        if actual.as_ref() != body {
            return Err("presigned GET bytes differ");
        }
        let metadata = bucket.head(&key).await.map_err(|_| "HEAD failed")?;
        if metadata.content_type() != Some("image/jpeg")
            || metadata
                .cache_control()
                .is_none_or(|value| !value.contains("public") || !value.contains("max-age=3600"))
            || metadata.content_disposition() != Some("attachment; filename=presigned.bin")
            || metadata.content_language() != Some("en-US")
            || metadata.expires() != Some(expires)
            || metadata.custom().get("upload-mode").map(String::as_str) != Some("presigned")
        {
            return Err("typed object metadata was not persisted");
        }
        Ok::<(), &'static str>(())
    }
    .await;

    let _ = bucket.delete(&key).await;
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_range_get_and_conditional_reads() {
    use r2kit::ByteRange;

    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let key = format!("_r2kit-tests/{}/range.bin", uuid::Uuid::new_v4());
    let body = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();

    let result = async {
        let put = bucket
            .put_bytes(&key, body.clone())
            .await
            .map_err(|_| "put failed")?;
        let etag = put.etag().ok_or("missing etag")?;

        // 1. Partial Bounded GET: bytes 0..=9
        let partial = bucket
            .get_object(&key)
            .range(ByteRange::Bounded(0, 9))
            .send()
            .await
            .map_err(|_| "partial bounded get failed")?
            .into_body()
            .collect()
            .await
            .map_err(|_| "partial body failed")?
            .into_bytes();
        if partial.as_ref() != &body[0..10] {
            return Err("bounded range body differs");
        }

        // 2. From offset GET: bytes 10..
        let from_offset = bucket
            .get_object(&key)
            .range(ByteRange::From(10))
            .send()
            .await
            .map_err(|_| "from offset get failed")?
            .into_body()
            .collect()
            .await
            .map_err(|_| "from offset body failed")?
            .into_bytes();
        if from_offset.as_ref() != &body[10..] {
            return Err("from range body differs");
        }

        // 3. Suffix GET: last 5 bytes
        let suffix = bucket
            .get_object(&key)
            .range(ByteRange::Suffix(5))
            .send()
            .await
            .map_err(|_| "suffix get failed")?
            .into_body()
            .collect()
            .await
            .map_err(|_| "suffix body failed")?
            .into_bytes();
        if suffix.as_ref() != &body[body.len() - 5..] {
            return Err("suffix range body differs");
        }

        // 4. If-Match matching ETag -> success
        let if_match_ok = bucket.get_object(&key).if_match(etag).send().await;
        if if_match_ok.is_err() {
            return Err("if_match with exact etag should succeed");
        }

        // 5. If-Match mismatch -> PreconditionFailed
        let if_match_err = bucket
            .get_object(&key)
            .if_match("\"mismatched-etag\"")
            .send()
            .await;
        if !matches!(if_match_err, Err(Error::PreconditionFailed)) {
            return Err("if_match with wrong etag must return PreconditionFailed");
        }

        // 6. If-None-Match matching ETag -> NotModified
        let if_none_match_not_mod = bucket.get_object(&key).if_none_match(etag).send().await;
        if !matches!(if_none_match_not_mod, Err(Error::NotModified)) {
            return Err("if_none_match with exact etag must return NotModified");
        }

        Ok::<(), &'static str>(())
    }
    .await;

    let _ = bucket.delete(&key).await;
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_download_file_to_local_disk() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let key = format!("_r2kit-tests/{}/download_file.bin", uuid::Uuid::new_v4());
    let body = b"streamed direct to disk via r2kit".to_vec();

    let result = async {
        bucket
            .put_bytes(&key, body.clone())
            .await
            .map_err(|_| "put failed")?;

        let temp_dir = tempfile::tempdir().map_err(|_| "tempdir failed")?;
        let file_path = temp_dir.path().join("downloaded.bin");

        let metadata = bucket
            .download_file(&key, &file_path)
            .await
            .map_err(|_| "download_file failed")?;
        if metadata.size() != body.len() as u64 {
            return Err("download_file metadata has wrong size");
        }

        let read_bytes = tokio::fs::read(&file_path)
            .await
            .map_err(|_| "read temp file failed")?;
        if read_bytes != body {
            return Err("file content on disk differs from uploaded body");
        }

        Ok::<(), &'static str>(())
    }
    .await;

    let _ = bucket.delete(&key).await;
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_checksum_upload_with_auto_and_precomputed() {
    #[cfg(feature = "checksum")]
    use base64::Engine as _;
    use r2kit::{ChecksumAlgorithm, ObjectUploadOptions};

    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let prefix = format!("_r2kit-tests/{}/checksum/", uuid::Uuid::new_v4());
    let sha256_key = format!("{prefix}sha256.bin");
    let crc32_key = format!("{prefix}crc32.bin");
    let body = b"r2kit checksum validation test content".to_vec();

    let result = async {
        // 1. Auto-computed SHA-256 upload
        let sha256_options = ObjectUploadOptions::builder()
            .checksum(ChecksumAlgorithm::Sha256)
            .build();
        let put_sha256 = bucket
            .put_bytes_with_options(&sha256_key, body.clone(), sha256_options)
            .await
            .map_err(|_| "auto sha256 upload failed")?;
        if put_sha256.etag().is_none() {
            return Err("missing etag on sha256 upload");
        }

        // 2. Pre-computed CRC32 upload
        // Base64 for CRC32 of `body`:
        #[cfg(feature = "checksum")]
        let crc32_b64 = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&body);
            let digest = hasher.finalize().to_be_bytes();
            base64::engine::general_purpose::STANDARD.encode(digest)
        };
        #[cfg(not(feature = "checksum"))]
        let crc32_b64 = "dummy".to_string();

        let crc32_options = ObjectUploadOptions::builder()
            .checksum_value(ChecksumAlgorithm::Crc32, crc32_b64)
            .build();
        let put_crc32 = bucket
            .put_bytes_with_options(&crc32_key, body.clone(), crc32_options)
            .await
            .map_err(|_| "precomputed crc32 upload failed")?;
        if put_crc32.etag().is_none() {
            return Err("missing etag on crc32 upload");
        }

        Ok::<(), &'static str>(())
    }
    .await;

    let _ = bucket.delete(&sha256_key).await;
    let _ = bucket.delete(&crc32_key).await;
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_list_multipart_uploads_and_cleanup() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let prefix = format!("_r2kit-tests/{}/mp_list/", uuid::Uuid::new_v4());
    let key = format!("{prefix}upload.bin");

    let result = async {
        // Start a multipart upload
        let session = bucket
            .presigned_multipart(&key)
            .map_err(|_| "invalid key")?
            .file_size(10 * 1024 * 1024)
            .part_size_mib(5)
            .create()
            .await
            .map_err(|_| "start multipart session failed")?;

        // List multipart uploads with prefix
        let pages: Vec<_> = bucket
            .list_multipart_uploads()
            .prefix(&prefix)
            .into_pages()
            .try_collect()
            .await
            .map_err(|_| "list_multipart_uploads into_pages failed")?;

        let all_uploads: Vec<_> = pages
            .into_iter()
            .flat_map(|page| page.uploads().to_vec())
            .collect();

        if !all_uploads.iter().any(|u| u.key() == key) {
            return Err("created multipart upload not found in list_multipart_uploads");
        }

        // Abort the session
        session.abort().await.map_err(|_| "abort session failed")?;

        // List again to verify it is gone
        let pages_after: Vec<_> = bucket
            .list_multipart_uploads()
            .prefix(&prefix)
            .into_pages()
            .try_collect()
            .await
            .map_err(|_| "list_multipart_uploads after abort failed")?;

        let remaining: Vec<_> = pages_after
            .into_iter()
            .flat_map(|page| page.uploads().to_vec())
            .collect();

        if remaining.iter().any(|u| u.key() == key) {
            return Err("aborted upload still visible in list_multipart_uploads");
        }

        Ok::<(), &'static str>(())
    }
    .await;

    result.unwrap();
}
