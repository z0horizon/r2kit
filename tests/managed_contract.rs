use std::sync::{Arc, Mutex};

use r2kit::{
    Error, ManagedUploadCancellation, MultipartSessionSnapshot, R2Client, R2Config,
    TransferProgress, TransferResult, TransferStrategyUsed, UploadThreshold, ValidationError,
};

fn offline_bucket() -> r2kit::Bucket {
    let config = R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("managed-access")
        .secret_access_key("managed-secret")
        .build()
        .unwrap();
    R2Client::new(config).bucket("managed-tests").unwrap()
}

#[tokio::test]
async fn rejects_invalid_managed_limits_before_file_or_network_io() {
    let bucket = offline_bucket();

    let concurrency = bucket
        .managed_multipart("object.bin")
        .unwrap()
        .concurrency(0)
        .upload_file("path-does-not-exist")
        .await
        .unwrap_err();
    assert!(matches!(
        concurrency.error(),
        Error::Validation(ValidationError::ConcurrencyOutOfRange {
            provided: 0,
            min: 1,
            max: 64
        })
    ));

    let attempts = bucket
        .managed_multipart("object.bin")
        .unwrap()
        .max_attempts(11)
        .upload_file("path-does-not-exist")
        .await
        .unwrap_err();
    assert!(matches!(
        attempts.error(),
        Error::Validation(ValidationError::AttemptsOutOfRange {
            provided: 11,
            min: 1,
            max: 10
        })
    ));

    let part_size = bucket
        .managed_multipart("object.bin")
        .unwrap()
        .part_size(1024)
        .upload_file("path-does-not-exist")
        .await
        .unwrap_err();
    assert!(matches!(
        part_size.error(),
        Error::Validation(ValidationError::PartSizeOutOfRange {
            provided: 1024,
            min: 5_242_880,
            max: 5_363_466_240
        })
    ));

    let overflowed_mib = bucket
        .managed_multipart("object.bin")
        .unwrap()
        .part_size_mib(u64::MAX)
        .upload_file("path-does-not-exist")
        .await
        .unwrap_err();
    assert!(matches!(
        overflowed_mib.error(),
        Error::Validation(ValidationError::PartSizeOutOfRange {
            provided: u64::MAX,
            ..
        })
    ));

    let memory_budget = bucket
        .managed_multipart("object.bin")
        .unwrap()
        .part_size_mib(64)
        .concurrency(8)
        .max_buffered_mib(256)
        .upload_file("path-does-not-exist")
        .await
        .unwrap_err();
    assert!(matches!(
        memory_budget.error(),
        Error::Validation(ValidationError::ManagedMemoryBudgetExceeded {
            required: 536_870_912,
            max: 268_435_456,
        })
    ));

    let exact_memory_budget = bucket
        .managed_multipart("object.bin")
        .unwrap()
        .part_size_mib(64)
        .concurrency(4)
        .max_buffered_mib(256)
        .upload_file("path-does-not-exist")
        .await
        .unwrap_err();
    assert!(matches!(
        exact_memory_budget.error(),
        Error::Io {
            operation: "metadata"
        }
    ));
}

#[test]
fn resumed_builder_redacts_the_upload_id() {
    let bucket = offline_bucket();
    let snapshot = MultipartSessionSnapshot::restore(
        "managed-tests",
        "object.bin",
        "sensitive-managed-upload-id",
        11 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();
    let builder = bucket.resume_managed_multipart(snapshot).unwrap();
    let debug = format!("{builder:?}");
    assert!(!debug.contains("sensitive-managed-upload-id"));
}

#[tokio::test]
async fn pre_cancelled_upload_never_starts_a_remote_session() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("cancelled.bin");
    tokio::fs::write(&path, vec![0_u8; 5 * 1024 * 1024])
        .await
        .unwrap();
    let cancellation = ManagedUploadCancellation::new();
    cancellation.cancel();

    let error = offline_bucket()
        .managed_multipart("cancelled.bin")
        .unwrap()
        .cancellation_token(cancellation)
        .upload_file(path)
        .await
        .unwrap_err();

    assert!(matches!(error.error(), Error::Cancelled));
    assert!(!error.was_aborted());
    assert!(error.snapshot().is_none());
}

#[tokio::test]
async fn upload_stream_rejects_invalid_limits_before_network() {
    let bucket = offline_bucket();
    let reader = std::io::Cursor::new(vec![0u8; 64]);

    let error = bucket
        .managed_multipart("stream.bin")
        .unwrap()
        .concurrency(0)
        .upload_stream(reader, 64)
        .await
        .unwrap_err();
    assert!(matches!(
        error.error(),
        Error::Validation(ValidationError::ConcurrencyOutOfRange {
            provided: 0,
            min: 1,
            max: 64
        })
    ));
}

#[tokio::test]
async fn pre_cancelled_upload_stream_never_starts_a_remote_session() {
    let cancellation = ManagedUploadCancellation::new();
    cancellation.cancel();
    let reader = std::io::Cursor::new(vec![0u8; 5 * 1024 * 1024]);

    let error = offline_bucket()
        .managed_multipart("cancelled-stream.bin")
        .unwrap()
        .cancellation_token(cancellation)
        .upload_stream(reader, 5 * 1024 * 1024)
        .await
        .unwrap_err();

    assert!(matches!(error.error(), Error::Cancelled));
    assert!(!error.was_aborted());
    assert!(error.snapshot().is_none());
}

#[tokio::test]
async fn upload_stream_rejects_resumed_sessions() {
    let bucket = offline_bucket();
    let snapshot = MultipartSessionSnapshot::restore(
        "managed-tests",
        "stream.bin",
        "existing-upload-id",
        10 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();

    let reader = std::io::Cursor::new(vec![0u8; 10 * 1024 * 1024]);
    let error = bucket
        .resume_managed_multipart(snapshot)
        .unwrap()
        .upload_stream(reader, 10 * 1024 * 1024)
        .await
        .unwrap_err();

    assert!(matches!(
        error.error(),
        Error::InvalidInput {
            field: "resume",
            ..
        }
    ));
}

#[tokio::test]
async fn managed_multipart_rejects_checksum_and_conditional_headers() {
    let bucket = offline_bucket();
    let reader = std::io::Cursor::new(vec![0u8; 1024]);

    let options_checksum =
        r2kit::ObjectUploadOptions::new().with_checksum(r2kit::ChecksumAlgorithm::Sha256);
    let err = bucket
        .managed_multipart("key.bin")
        .unwrap()
        .upload_options(options_checksum)
        .upload_stream(reader, 1024)
        .await
        .unwrap_err();
    assert!(matches!(
        err.error(),
        Error::InvalidInput {
            field: "checksum",
            ..
        }
    ));

    let reader = std::io::Cursor::new(vec![0u8; 1024]);
    let options_if_match = r2kit::ObjectUploadOptions::new().with_if_match("etag");
    let err = bucket
        .managed_multipart("key.bin")
        .unwrap()
        .upload_options(options_if_match)
        .upload_stream(reader, 1024)
        .await
        .unwrap_err();
    assert!(matches!(
        err.error(),
        Error::InvalidInput {
            field: "if_match",
            ..
        }
    ));
}

#[tokio::test]
async fn upload_stream_bounds_channel_size_when_concurrency_exceeds_max_parts() {
    let bucket = offline_bucket();
    let reader = std::io::Cursor::new(vec![0u8; 100]);
    // max_buffered_bytes = 10 MiB, part_size = 10 MiB -> max_parts = 1.
    // concurrency = 4 -> max_parts.saturating_sub(concurrency) = 0 -> .max(1) guarantees channel capacity >= 1 without underflow or panic.
    let result = bucket
        .managed_multipart("key.bin")
        .unwrap()
        .part_size_mib(10)
        .max_buffered_bytes(10 * 1024 * 1024)
        .concurrency(4)
        .upload_stream(reader, 100)
        .await;
    let err = result.unwrap_err();
    assert!(!matches!(err.error(), Error::InvalidInput { .. }));
}

async fn start_mock_s3() -> (String, tokio::sync::oneshot::Sender<()>) {
    use axum::{
        Router,
        extract::{Query, Request},
        http::{HeaderMap, HeaderValue, StatusCode},
        response::IntoResponse,
        routing::any,
    };
    use std::collections::HashMap;

    let router = Router::new().fallback(any(
        |Query(params): Query<HashMap<String, String>>, req: Request| async move {
            let method = req.method().clone();
            let _ = axum::body::to_bytes(req.into_body(), usize::MAX).await;

            if method == axum::http::Method::POST && params.contains_key("uploads") {
                let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>managed-tests</Bucket>
    <Key>test-key</Key>
    <UploadId>mock-upload-id-999</UploadId>
</InitiateMultipartUploadResult>"#;
                return (StatusCode::OK, [("content-type", "application/xml")], body)
                    .into_response();
            }

            if method == axum::http::Method::POST && params.contains_key("uploadId") {
                let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Location>http://example.com/</Location>
    <Bucket>managed-tests</Bucket>
    <Key>test-key</Key>
    <ETag>"complete-multipart-etag"</ETag>
</CompleteMultipartUploadResult>"#;
                return (StatusCode::OK, [("content-type", "application/xml")], body)
                    .into_response();
            }

            if method == axum::http::Method::DELETE && params.contains_key("uploadId") {
                return StatusCode::NO_CONTENT.into_response();
            }

            if method == axum::http::Method::PUT && params.contains_key("partNumber") {
                let part_num = params.get("partNumber").cloned().unwrap_or_default();
                let mut headers = HeaderMap::new();
                headers.insert(
                    "etag",
                    HeaderValue::from_str(&format!("\"part-{part_num}-etag\"")).unwrap(),
                );
                return (StatusCode::OK, headers, ()).into_response();
            }

            if method == axum::http::Method::PUT {
                let mut headers = HeaderMap::new();
                headers.insert("etag", HeaderValue::from_static("\"single-put-etag\""));
                return (StatusCode::OK, headers, ()).into_response();
            }

            StatusCode::NOT_FOUND.into_response()
        },
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });
    (format!("http://127.0.0.1:{}", addr.port()), tx)
}

fn mock_bucket(endpoint: &str) -> r2kit::Bucket {
    let sdk_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "dummy-access",
            "dummy-secret",
            None,
            None,
            "contract-test",
        ))
        .build();
    let client = R2Client::from_sdk(aws_sdk_s3::Client::from_conf(sdk_config));
    client.bucket("managed-tests").unwrap()
}

#[tokio::test]
async fn upload_file_empty_file_succeeds_with_single_put_and_100_percent_progress() {
    let (endpoint, _shutdown) = start_mock_s3().await;
    let bucket = mock_bucket(&endpoint);

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("empty.txt");
    tokio::fs::write(&path, b"").await.unwrap();

    let updates = Arc::new(Mutex::new(Vec::<TransferProgress>::new()));
    let captured = Arc::clone(&updates);

    let result: TransferResult = bucket
        .upload_file("empty.txt", &path)
        .on_progress(move |progress| captured.lock().unwrap().push(progress))
        .await
        .unwrap();

    assert_eq!(result.key().as_str(), "empty.txt");
    assert_eq!(result.size(), 0);
    assert_eq!(result.strategy(), TransferStrategyUsed::SinglePut);
    assert!(result.strategy().is_single_put());
    assert!(!result.strategy().is_multipart());
    assert_eq!(result.etag(), "\"single-put-etag\"");

    let updates = updates.lock().unwrap();
    assert_eq!(updates.len(), 1);
    let update = updates[0];
    assert_eq!(update.transferred_bytes(), 0);
    assert_eq!(update.total_bytes(), 0);
    assert!(!update.is_multipart());
    assert_eq!(update.fraction(), 1.0);
    assert_eq!(update.percentage(), 100.0);
}

#[tokio::test]
async fn upload_file_sub_threshold_succeeds_with_single_put() {
    let (endpoint, _shutdown) = start_mock_s3().await;
    let bucket = mock_bucket(&endpoint);

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("small.txt");
    let content = vec![0x42; 1024 * 1024]; // 1 MiB (< 8 MiB threshold)
    tokio::fs::write(&path, &content).await.unwrap();

    let updates = Arc::new(Mutex::new(Vec::<TransferProgress>::new()));
    let captured = Arc::clone(&updates);

    let result = bucket
        .upload_file("small.txt", &path)
        .on_progress(move |progress| captured.lock().unwrap().push(progress))
        .await
        .unwrap();

    assert_eq!(result.key().as_str(), "small.txt");
    assert_eq!(result.size(), 1024 * 1024);
    assert_eq!(result.strategy(), TransferStrategyUsed::SinglePut);
    assert!(result.strategy().is_single_put());
    assert!(!result.strategy().is_multipart());
    assert_eq!(result.etag(), "\"single-put-etag\"");

    let updates = updates.lock().unwrap();
    assert!(!updates.is_empty());
    let last = *updates.last().unwrap();
    assert_eq!(last.transferred_bytes(), 1024 * 1024);
    assert_eq!(last.total_bytes(), 1024 * 1024);
    assert!(!last.is_multipart());
    assert_eq!(last.fraction(), 1.0);
    assert_eq!(last.percentage(), 100.0);
}

#[tokio::test]
async fn upload_file_above_threshold_executes_multipart() {
    let (endpoint, _shutdown) = start_mock_s3().await;
    let bucket = mock_bucket(&endpoint);

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("large.bin");
    let content = vec![0x43; 12 * 1024 * 1024]; // 12 MiB (>= 8 MiB threshold)
    tokio::fs::write(&path, &content).await.unwrap();

    let updates = Arc::new(Mutex::new(Vec::<TransferProgress>::new()));
    let captured = Arc::clone(&updates);

    let result = bucket
        .upload_file("large.bin", &path)
        .part_size(8 * 1024 * 1024)
        .concurrency(2)
        .on_progress(move |progress| captured.lock().unwrap().push(progress))
        .await
        .unwrap();

    assert_eq!(result.key().as_str(), "large.bin");
    assert_eq!(result.size(), 12 * 1024 * 1024);
    assert_eq!(
        result.strategy(),
        TransferStrategyUsed::Multipart { part_count: 2 }
    );
    assert!(result.strategy().is_multipart());
    assert!(!result.strategy().is_single_put());
    assert_eq!(result.etag(), "\"complete-multipart-etag\"");

    let updates = updates.lock().unwrap();
    assert!(!updates.is_empty());
    for u in updates.iter() {
        assert!(u.is_multipart());
        assert_eq!(u.total_bytes(), 12 * 1024 * 1024);
    }
    let last = *updates.last().unwrap();
    assert_eq!(last.transferred_bytes(), 12 * 1024 * 1024);
    assert_eq!(last.fraction(), 1.0);
    assert_eq!(last.percentage(), 100.0);
}

#[tokio::test]
async fn upload_file_progress_callbacks_report_accurate_byte_counts_and_percentages() {
    let (endpoint, _shutdown) = start_mock_s3().await;
    let bucket = mock_bucket(&endpoint);

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("progress.bin");
    let content = vec![0x44; 10 * 1024 * 1024]; // 10 MiB
    tokio::fs::write(&path, &content).await.unwrap();

    let updates = Arc::new(Mutex::new(Vec::<TransferProgress>::new()));
    let captured = Arc::clone(&updates);

    bucket
        .upload_file("progress.bin", &path)
        .part_size(5 * 1024 * 1024)
        .concurrency(1)
        .on_progress(move |progress| captured.lock().unwrap().push(progress))
        .await
        .unwrap();

    let updates = updates.lock().unwrap();
    assert!(updates.len() >= 2);
    let final_update = *updates.last().unwrap();
    assert_eq!(final_update.transferred_bytes(), 10 * 1024 * 1024);
    assert_eq!(final_update.total_bytes(), 10 * 1024 * 1024);
    assert_eq!(final_update.percentage(), 100.0);
}

#[tokio::test]
async fn upload_file_cooperative_cancellation_stops_promptly() {
    let (endpoint, _shutdown) = start_mock_s3().await;
    let bucket = mock_bucket(&endpoint);

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("cancel.bin");
    tokio::fs::write(&path, vec![0x45; 1024 * 1024])
        .await
        .unwrap();

    // 1. Pre-cancelled token stops before network I/O
    let cancellation = ManagedUploadCancellation::new();
    cancellation.cancel();

    let err = bucket
        .upload_file("cancel.bin", &path)
        .cancellation_token(cancellation)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Cancelled));

    // 2. In-flight cancellation on multipart upload
    let large_path = temp.path().join("large_cancel.bin");
    tokio::fs::write(&large_path, vec![0x45; 12 * 1024 * 1024])
        .await
        .unwrap();
    let cancellation2 = ManagedUploadCancellation::new();
    let c2 = cancellation2.clone();

    let err2 = bucket
        .upload_file("large_cancel.bin", &large_path)
        .part_size(5 * 1024 * 1024)
        .concurrency(1)
        .cancellation_token(cancellation2)
        .on_progress(move |progress| {
            if progress.transferred_bytes() > 0 {
                c2.cancel();
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(err2, Error::Cancelled));
}

#[tokio::test]
async fn upload_file_rejects_non_file_or_missing_path() {
    let bucket = offline_bucket();
    let temp = tempfile::tempdir().unwrap();

    // Directory path
    let err_dir = bucket
        .upload_file("dir.bin", temp.path())
        .await
        .unwrap_err();
    assert!(matches!(err_dir, Error::InvalidInput { field: "path", .. }));

    // Missing file path
    let missing_path = temp.path().join("non_existent_file.bin");
    let err_missing = bucket
        .upload_file("missing.bin", missing_path)
        .await
        .unwrap_err();
    assert!(matches!(
        err_missing,
        Error::Io {
            operation: "metadata"
        }
    ));
}

#[tokio::test]
async fn upload_file_honors_custom_threshold() {
    let (endpoint, _shutdown) = start_mock_s3().await;
    let bucket = mock_bucket(&endpoint);

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("threshold_test.bin");
    tokio::fs::write(&path, vec![0x46; 6 * 1024 * 1024])
        .await
        .unwrap(); // 6 MiB

    // With threshold 10 MiB, 6 MiB file is sub-threshold -> SinglePut
    let res_single = bucket
        .upload_file("threshold_test.bin", &path)
        .threshold(UploadThreshold::new(10 * 1024 * 1024).unwrap())
        .await
        .unwrap();
    assert_eq!(res_single.strategy(), TransferStrategyUsed::SinglePut);

    // With threshold 5 MiB, 6 MiB file is above-threshold -> Multipart
    let res_multi = bucket
        .upload_file("threshold_test.bin", &path)
        .threshold(UploadThreshold::new(5 * 1024 * 1024).unwrap())
        .part_size(5 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        res_multi.strategy(),
        TransferStrategyUsed::Multipart { part_count: 2 }
    );
}

#[tokio::test]
async fn upload_file_validates_limits_and_options_before_file_io() {
    let bucket = offline_bucket();

    // Invalid part size (< 5 MiB) fails before checking path
    let err_part = bucket
        .upload_file("key.bin", "non_existent_file.bin")
        .part_size(1024)
        .await
        .unwrap_err();
    assert!(matches!(
        err_part,
        Error::Validation(ValidationError::PartSizeOutOfRange { provided: 1024, .. })
    ));

    // Exceeded memory budget fails before checking path
    let err_mem = bucket
        .upload_file("key.bin", "non_existent_file.bin")
        .part_size(10 * 1024 * 1024)
        .concurrency(10)
        .max_buffered_bytes(5 * 1024 * 1024)
        .await
        .unwrap_err();
    assert!(matches!(
        err_mem,
        Error::Validation(ValidationError::ManagedMemoryBudgetExceeded { .. })
    ));

    // Invalid upload options fail before checking path
    let err_opt = bucket
        .upload_file("key.bin", "non_existent_file.bin")
        .upload_options(
            r2kit::ObjectUploadOptions::new().with_custom_metadata("Invalid Key!", "val"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err_opt,
        Error::InvalidInput {
            field: "custom_metadata",
            ..
        }
    ));
}
