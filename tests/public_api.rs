use r2kit::{
    CompletionManifest, Error, MultipartPartReceipt, MultipartSessionSnapshot, PartNumber,
    ValidationError,
};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn async_handles_are_send_and_sync() {
    assert_send_sync::<r2kit::ObjectKey>();
    assert_send_sync::<r2kit::BucketName>();
    assert_send_sync::<r2kit::R2Config>();
    assert_send_sync::<r2kit::R2ConfigBuilder>();
    assert_send_sync::<r2kit::R2Jurisdiction>();
    assert_send_sync::<r2kit::R2Client>();
    assert_send_sync::<r2kit::Bucket>();
    assert_send_sync::<r2kit::BucketInfo>();
    assert_send_sync::<r2kit::ManagedMultipartBuilder>();
    assert_send_sync::<r2kit::ManagedUploadCancellation>();
    assert_send_sync::<r2kit::ManagedUploadError>();
    assert_send_sync::<r2kit::ManagedUploadProgress>();
    assert_send_sync::<r2kit::ManagedUploadResult>();
    assert_send_sync::<r2kit::PresignedMultipart>();
    assert_send_sync::<r2kit::PresignedMultipartBuilder>();
    assert_send_sync::<r2kit::CompletedObject>();
    assert_send_sync::<r2kit::CompletionManifest>();
    assert_send_sync::<r2kit::MultipartPartReceipt>();
    assert_send_sync::<r2kit::MultipartSessionRecord>();
    assert_send_sync::<r2kit::MultipartSessionSnapshot>();
    assert_send_sync::<r2kit::PartNumber>();
    assert_send_sync::<r2kit::ByteRange>();
    assert_send_sync::<r2kit::ChecksumAlgorithm>();
    assert_send_sync::<r2kit::MetadataDirective>();
    assert_send_sync::<r2kit::GetObjectBuilder>();
    assert_send_sync::<r2kit::HeadObjectBuilder>();
    assert_send_sync::<r2kit::CopyObjectBuilder>();
    assert_send_sync::<r2kit::CopyObjectResult>();
    assert_send_sync::<r2kit::ListObjectsBuilder>();
    assert_send_sync::<r2kit::ObjectUploadOptions>();
    assert_send_sync::<r2kit::ObjectBytes>();
    assert_send_sync::<r2kit::PutObjectResult>();
    assert_send_sync::<r2kit::PresignedPutObject>();
    assert_send_sync::<r2kit::DownloadedObject>();
    assert_send_sync::<r2kit::ObjectMetadata>();
    assert_send_sync::<r2kit::ObjectPage>();
    assert_send_sync::<r2kit::ObjectSummary>();
    assert_send_sync::<r2kit::DeleteObjectsResult>();
    assert_send_sync::<r2kit::BatchDeleteError>();
    assert_send_sync::<r2kit::DeleteObjectFailure>();
    assert_send_sync::<r2kit::ListMultipartUploadsBuilder>();
    assert_send_sync::<r2kit::MultipartUploadPage>();
    assert_send_sync::<r2kit::MultipartUploadSummary>();
}

#[test]
fn multipart_snapshot_exposes_every_persistence_field_deliberately() {
    let snapshot = MultipartSessionSnapshot::restore(
        "example-bucket",
        "videos/example.mp4",
        "sensitive-upload-id",
        11 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();

    assert_eq!(snapshot.bucket(), "example-bucket");
    assert_eq!(snapshot.key(), "videos/example.mp4");
    assert_eq!(snapshot.expose_upload_id(), "sensitive-upload-id");
    assert_eq!(snapshot.file_size(), 11 * 1024 * 1024);
    assert_eq!(snapshot.part_size(), 5 * 1024 * 1024);

    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("sensitive-upload-id"));
}

#[test]
fn multipart_snapshot_restore_validates_bucket_name() {
    let result = MultipartSessionSnapshot::restore(
        "INVALID.BUCKET.NAME",
        "videos/example.mp4",
        "sensitive-upload-id",
        11 * 1024 * 1024,
        5 * 1024 * 1024,
    );
    assert!(result.is_err());
}

#[test]
fn persistence_record_round_trips_through_validation() {
    let snapshot = MultipartSessionSnapshot::restore(
        "example-bucket",
        "videos/example.mp4",
        "sensitive-upload-id",
        11 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();
    let record = snapshot.into_persistence_record();

    assert_eq!(record.version(), 1);
    assert_eq!(record.expose_upload_id(), "sensitive-upload-id");
    assert!(!format!("{record:?}").contains("sensitive-upload-id"));

    let restored = MultipartSessionSnapshot::from_persistence_record(record).unwrap();
    assert_eq!(restored.key(), "videos/example.mp4");
}

#[test]
fn uploader_receipt_is_validated_at_the_trust_boundary() {
    let receipt = MultipartPartReceipt::new(1, "\"etag-from-r2\"");
    let uploaded = receipt.try_into_uploaded_part().unwrap();
    assert_eq!(uploaded.part_number().get(), 1);
    assert_eq!(uploaded.etag(), "\"etag-from-r2\"");

    assert!(
        MultipartPartReceipt::new(0, "etag")
            .try_into_uploaded_part()
            .is_err()
    );
    assert!(
        MultipartPartReceipt::new(1, "")
            .try_into_uploaded_part()
            .is_err()
    );

    let manifest = CompletionManifest::try_from_receipts([
        MultipartPartReceipt::new(2, "etag-2"),
        MultipartPartReceipt::new(1, "etag-1"),
    ])
    .unwrap();
    assert_eq!(
        manifest
            .parts()
            .map(|part| part.part_number().get())
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn part_number_errors_expose_the_exact_r2_bounds() {
    for provided in [0, 10_001] {
        assert!(matches!(
            PartNumber::try_from(provided),
            Err(Error::Validation(ValidationError::PartNumberOutOfRange {
                provided: actual,
                min: 1,
                max: 10_000
            })) if actual == provided
        ));
    }
    assert_eq!(PartNumber::try_from(1).unwrap().get(), 1);
    assert_eq!(PartNumber::try_from(10_000).unwrap().get(), 10_000);
}

#[cfg(feature = "serde")]
#[test]
fn persistence_record_and_receipt_support_serde_without_leaking_in_debug() {
    let snapshot = MultipartSessionSnapshot::restore(
        "example-bucket",
        "videos/example.mp4",
        "sensitive-upload-id",
        5 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();
    let json = serde_json::to_string(&snapshot.into_persistence_record()).unwrap();
    let record = serde_json::from_str(&json).unwrap();
    let restored = MultipartSessionSnapshot::from_persistence_record(record).unwrap();
    assert_eq!(restored.expose_upload_id(), "sensitive-upload-id");

    let receipt = MultipartPartReceipt::new(1, "etag");
    let json = serde_json::to_string(&receipt).unwrap();
    let decoded: MultipartPartReceipt = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, receipt);

    let snapshot = MultipartSessionSnapshot::restore(
        "example-bucket",
        "videos/example.mp4",
        "sensitive-upload-id",
        5 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();
    let mut unsupported = serde_json::to_value(snapshot.into_persistence_record()).unwrap();
    unsupported["version"] = serde_json::json!(2);
    let record = serde_json::from_value(unsupported).unwrap();
    assert!(MultipartSessionSnapshot::from_persistence_record(record).is_err());
}

#[test]
fn list_multipart_uploads_builder_debug_redacts_upload_id_marker() {
    let config = r2kit::R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("test")
        .secret_access_key("test")
        .build()
        .unwrap();
    let bucket = r2kit::R2Client::new(config).bucket("test-bucket").unwrap();
    let builder = bucket
        .list_multipart_uploads()
        .upload_id_marker("sensitive-upload-id-marker-98765");
    let debug = format!("{builder:?}");
    assert!(
        !debug.contains("sensitive-upload-id-marker-98765"),
        "upload_id_marker was leaked in Debug output: {debug}"
    );
}

#[test]
fn into_content_type_converts_str_string_and_mime() {
    use r2kit::IntoContentType;

    let mime_from_str = "application/json".into_content_type().unwrap();
    assert_eq!(mime_from_str, mime::APPLICATION_JSON);

    let mime_from_string = String::from("text/plain").into_content_type().unwrap();
    assert_eq!(mime_from_string, mime::TEXT_PLAIN);

    let mime_from_mime = mime::APPLICATION_OCTET_STREAM.into_content_type().unwrap();
    assert_eq!(mime_from_mime, mime::APPLICATION_OCTET_STREAM);

    let invalid = "not a valid mime type".into_content_type();
    assert!(matches!(
        invalid,
        Err(Error::InvalidInput {
            field: "content_type",
            ..
        })
    ));
}

#[cfg(feature = "serde")]
#[test]
fn multipart_session_snapshot_supports_direct_serde() {
    let snapshot = MultipartSessionSnapshot::restore(
        "example-bucket",
        "videos/example.mp4",
        "sensitive-upload-id-123",
        11 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();

    let json = serde_json::to_string(&snapshot).unwrap();
    assert!(json.contains("example-bucket"));
    assert!(json.contains("videos/example.mp4"));
    assert!(json.contains("sensitive-upload-id-123"));

    let restored: MultipartSessionSnapshot = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.bucket(), snapshot.bucket());
    assert_eq!(restored.key(), snapshot.key());
    assert_eq!(restored.expose_upload_id(), snapshot.expose_upload_id());
    assert_eq!(restored.file_size(), snapshot.file_size());
    assert_eq!(restored.part_size(), snapshot.part_size());
}
