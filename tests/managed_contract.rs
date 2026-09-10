use r2kit::{
    Error, ManagedUploadCancellation, MultipartSessionSnapshot, R2Client, R2Config, ValidationError,
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
