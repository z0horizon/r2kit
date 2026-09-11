use std::{
    env,
    sync::{Arc, Mutex},
};

use aws_sdk_s3::primitives::ByteStream;
use r2kit::{
    CacheControl, Error, ManagedUploadCancellation, ManagedUploadProgress, R2Client, R2Config, mime,
};

const MIB: usize = 1024 * 1024;

fn live_client() -> R2Client {
    assert_eq!(env::var("R2KIT_LIVE_TESTS").as_deref(), Ok("1"));
    assert_eq!(
        env::var("R2KIT_LIVE_BUCKET").as_deref(),
        Ok("r2kit-live-tests")
    );
    R2Client::new(R2Config::from_env().expect("R2 live credentials are required"))
}

fn test_body() -> Vec<u8> {
    [vec![0x41; 5 * MIB], vec![0x42; 5 * MIB], vec![0x43; MIB]]
        .into_iter()
        .flatten()
        .collect()
}

async fn assert_remote_bytes(bucket: &r2kit::Bucket, key: &str, expected: &[u8]) {
    let actual = bucket
        .get(key)
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(actual.as_ref(), expected);
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_managed_upload_reports_progress_and_cleans_up() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let id = uuid::Uuid::new_v4();
    let key = format!("_r2kit-tests/{id}/managed-new.bin");
    let path = env::temp_dir().join(format!("r2kit-{id}-managed-new.bin"));
    let body = test_body();
    tokio::fs::write(&path, &body).await.unwrap();
    let updates = Arc::new(Mutex::new(Vec::<ManagedUploadProgress>::new()));
    let captured = Arc::clone(&updates);

    let result = bucket
        .managed_multipart(&key)
        .unwrap()
        .part_size((5 * MIB) as u64)
        .content_type("video/mp4".parse::<mime::Mime>().unwrap())
        .cache_control(CacheControl::new().with_private())
        .concurrency(2)
        .max_attempts(4)
        .on_progress(move |progress| captured.lock().unwrap().push(progress))
        .upload_file(&path)
        .await;

    let _ = tokio::fs::remove_file(&path).await;
    match result {
        Ok(result) => {
            assert_eq!(result.file_size(), body.len() as u64);
            assert_eq!(result.part_count(), 3);
            assert_eq!(result.uploaded_parts(), 3);
            assert_eq!(result.reused_parts(), 0);
            assert_remote_bytes(&bucket, &key, &body).await;
            let metadata = bucket.head(&key).await.unwrap();
            assert_eq!(metadata.content_type(), Some("video/mp4"));
            assert!(
                metadata
                    .cache_control()
                    .is_some_and(|value| value.contains("private"))
            );
            let updates = updates.lock().unwrap();
            let final_update = updates.last().unwrap();
            assert_eq!(final_update.completed_parts(), 3);
            assert_eq!(final_update.transferred_bytes(), body.len() as u64);
        }
        Err(error) => panic!("managed upload failed: {error}"),
    }
    bucket.delete(&key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_managed_upload_stream_reports_progress_and_cleans_up() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let id = uuid::Uuid::new_v4();
    let key = format!("_r2kit-tests/{id}/managed-stream.bin");
    let body = test_body();
    let reader = std::io::Cursor::new(body.clone());
    let updates = Arc::new(Mutex::new(Vec::<ManagedUploadProgress>::new()));
    let captured = Arc::clone(&updates);

    let result = bucket
        .managed_multipart(&key)
        .unwrap()
        .part_size((5 * MIB) as u64)
        .content_type("video/mp4".parse::<mime::Mime>().unwrap())
        .cache_control(CacheControl::new().with_private())
        .concurrency(2)
        .max_attempts(4)
        .on_progress(move |progress| captured.lock().unwrap().push(progress))
        .upload_stream(reader, body.len() as u64)
        .await;

    match result {
        Ok(result) => {
            assert_eq!(result.file_size(), body.len() as u64);
            assert_eq!(result.part_count(), 3);
            assert_eq!(result.uploaded_parts(), 3);
            assert_eq!(result.reused_parts(), 0);
            assert_remote_bytes(&bucket, &key, &body).await;
            let metadata = bucket.head(&key).await.unwrap();
            assert_eq!(metadata.content_type(), Some("video/mp4"));
            assert!(
                metadata
                    .cache_control()
                    .is_some_and(|value| value.contains("private"))
            );
            let updates = updates.lock().unwrap();
            let final_update = updates.last().unwrap();
            assert_eq!(final_update.completed_parts(), 3);
            assert_eq!(final_update.transferred_bytes(), body.len() as u64);
        }
        Err(error) => panic!("managed stream upload failed: {error}"),
    }
    bucket.delete(&key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_managed_resume_reuses_an_existing_part() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let id = uuid::Uuid::new_v4();
    let key = format!("_r2kit-tests/{id}/managed-resume.bin");
    let path = env::temp_dir().join(format!("r2kit-{id}-managed-resume.bin"));
    let body = test_body();
    tokio::fs::write(&path, &body).await.unwrap();
    let session = bucket
        .presigned_multipart(&key)
        .unwrap()
        .file_size(body.len() as u64)
        .part_size((5 * MIB) as u64)
        .create()
        .await
        .unwrap();
    let snapshot = session.snapshot();
    client
        .as_sdk()
        .upload_part()
        .bucket("r2kit-live-tests")
        .key(&key)
        .upload_id(snapshot.expose_upload_id())
        .part_number(1)
        .content_length((5 * MIB) as i64)
        .body(ByteStream::from(body[..5 * MIB].to_vec()))
        .send()
        .await
        .unwrap();

    let result = bucket
        .resume_managed_multipart(snapshot)
        .unwrap()
        .concurrency(2)
        .max_attempts(4)
        .upload_file(&path)
        .await;

    let _ = tokio::fs::remove_file(&path).await;
    match result {
        Ok(result) => {
            assert_eq!(result.part_count(), 3);
            assert_eq!(result.uploaded_parts(), 2);
            assert_eq!(result.reused_parts(), 1);
            assert_remote_bytes(&bucket, &key, &body).await;
        }
        Err(error) => {
            let _ = session.abort().await;
            panic!("managed resume failed: {error}");
        }
    }
    bucket.delete(&key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_managed_failure_aborts_an_incompatible_session() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let id = uuid::Uuid::new_v4();
    let key = format!("_r2kit-tests/{id}/managed-abort.bin");
    let path = env::temp_dir().join(format!("r2kit-{id}-managed-abort.bin"));
    let body = vec![0x51; 6 * MIB];
    tokio::fs::write(&path, &body).await.unwrap();
    let session = bucket
        .presigned_multipart(&key)
        .unwrap()
        .file_size(body.len() as u64)
        .part_size((5 * MIB) as u64)
        .create()
        .await
        .unwrap();
    let snapshot = session.snapshot();
    client
        .as_sdk()
        .upload_part()
        .bucket("r2kit-live-tests")
        .key(&key)
        .upload_id(snapshot.expose_upload_id())
        .part_number(1)
        .content_length(MIB as i64)
        .body(ByteStream::from(body[..MIB].to_vec()))
        .send()
        .await
        .unwrap();

    let error = bucket
        .resume_managed_multipart(snapshot)
        .unwrap()
        .upload_file(&path)
        .await
        .unwrap_err();
    let _ = tokio::fs::remove_file(&path).await;
    if !error.was_aborted() {
        let _ = session.abort().await;
    }
    assert!(error.was_aborted());
    assert!(error.snapshot().is_none());
    let snapshot = session.snapshot();
    let parts = client
        .as_sdk()
        .list_parts()
        .bucket("r2kit-live-tests")
        .key(&key)
        .upload_id(snapshot.expose_upload_id())
        .send()
        .await;
    assert!(parts.is_err(), "aborted upload must no longer be listable");
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn live_managed_cancellation_aborts_the_remote_session() {
    let client = live_client();
    let bucket = client.bucket("r2kit-live-tests").unwrap();
    let id = uuid::Uuid::new_v4();
    let key = format!("_r2kit-tests/{id}/managed-cancel.bin");
    let path = env::temp_dir().join(format!("r2kit-{id}-managed-cancel.bin"));
    tokio::fs::write(&path, test_body()).await.unwrap();
    let cancellation = ManagedUploadCancellation::new();
    let cancel_after_first_part = cancellation.clone();

    let error = bucket
        .managed_multipart(&key)
        .unwrap()
        .part_size((5 * MIB) as u64)
        .concurrency(1)
        .cancellation_token(cancellation)
        .on_progress(move |progress| {
            if progress.completed_parts() == 1 {
                cancel_after_first_part.cancel();
            }
        })
        .upload_file(&path)
        .await
        .unwrap_err();
    let _ = tokio::fs::remove_file(&path).await;

    assert!(matches!(error.error(), Error::Cancelled));
    assert!(error.was_aborted());
    assert!(error.snapshot().is_none());
    assert!(bucket.head(&key).await.is_err());
}

#[tokio::test]
#[ignore = "requires explicit bucket-scoped R2 credentials"]
async fn zz_live_managed_suite_leaves_no_incomplete_uploads() {
    let client = live_client();
    let output = client
        .as_sdk()
        .list_multipart_uploads()
        .bucket("r2kit-live-tests")
        .prefix("_r2kit-tests/")
        .send()
        .await
        .unwrap();
    assert!(
        output.uploads().is_empty(),
        "live tests left an incomplete multipart upload"
    );
}
