use aws_sdk_s3::{config::Credentials, primitives::ByteStream};
use r2kit::{Error, R2Client, R2Config, ValidationError};

fn offline_client() -> R2Client {
    let config = R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("offline-access")
        .secret_access_key("offline-secret")
        .build()
        .unwrap();
    let sdk_config = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .endpoint_url(config.endpoint_url())
        .region(aws_sdk_s3::config::Region::new("auto"))
        .credentials_provider(Credentials::new(
            "offline-access",
            "offline-secret",
            None,
            None,
            "contract-test",
        ))
        .build();
    R2Client::from_sdk(aws_sdk_s3::Client::from_conf(sdk_config))
}

fn offline_bucket() -> r2kit::Bucket {
    offline_client().bucket("contract-tests").unwrap()
}

#[test]
fn enforces_the_documented_r2_bucket_name_length() {
    let config = R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("offline-access")
        .secret_access_key("offline-secret")
        .build()
        .unwrap();
    let client = R2Client::new(config);

    assert!(client.bucket("a".repeat(63)).is_ok());
    assert!(matches!(
        client.bucket("a".repeat(64)),
        Err(Error::InvalidInput {
            field: "bucket",
            ..
        })
    ));
}

#[tokio::test]
async fn rejects_invalid_list_options_before_network() {
    let bucket = offline_bucket();

    let zero = bucket.list().limit(0).send().await.unwrap_err();
    assert!(matches!(
        zero,
        Error::Validation(ValidationError::ListLimitOutOfRange {
            provided: 0,
            min: 1,
            max: 1_000
        })
    ));

    let too_large = bucket.list().limit(1_001).send().await.unwrap_err();
    assert!(matches!(
        too_large,
        Error::Validation(ValidationError::ListLimitOutOfRange {
            provided: 1_001,
            min: 1,
            max: 1_000
        })
    ));

    let empty_token = bucket
        .list()
        .continuation_token("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_token,
        Error::InvalidInput {
            field: "continuation_token",
            ..
        }
    ));
}

#[tokio::test]
async fn rejects_invalid_object_writes_before_network() {
    let bucket = offline_bucket();

    let empty_key = bucket.put_bytes("", Vec::new()).await.unwrap_err();
    assert!(matches!(
        empty_key,
        Error::InvalidInput { field: "key", .. }
    ));

    let too_large = bucket
        .put_stream(
            "large.bin",
            ByteStream::from_static(&[]),
            5 * 1024 * 1024 * 1024 - 5 * 1024 * 1024 + 1,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        too_large,
        Error::Validation(ValidationError::SingleUploadTooLarge {
            provided: 5_363_466_241,
            max: 5_363_466_240
        })
    ));
}

#[tokio::test]
async fn copy_validates_both_keys_before_network() {
    let bucket = offline_bucket();

    let invalid_source = bucket.copy("", "destination").await.unwrap_err();
    assert!(matches!(
        invalid_source,
        Error::InvalidInput { field: "key", .. }
    ));

    let invalid_destination = bucket.copy("source", "").await.unwrap_err();
    assert!(matches!(
        invalid_destination,
        Error::InvalidInput { field: "key", .. }
    ));
}

#[tokio::test]
async fn empty_batch_delete_is_a_no_op() {
    let result = offline_bucket()
        .delete_objects(Vec::<String>::new())
        .await
        .unwrap();

    assert!(result.is_complete());
    assert_eq!(result.request_count(), 0);
    assert!(result.deleted_keys().is_empty());
    assert!(result.failures().is_empty());
}

#[tokio::test]
async fn batch_delete_validates_every_key_before_network() {
    let error = offline_bucket()
        .delete_objects(["valid", ""])
        .await
        .unwrap_err();

    assert!(matches!(
        error.error(),
        Error::InvalidInput { field: "key", .. }
    ));
    assert_eq!(error.partial_result().request_count(), 0);
    assert!(error.partial_result().deleted_keys().is_empty());
}

#[test]
fn listing_exposes_a_sendable_page_stream() {
    fn assert_send<T: Send>(_: &T) {}

    let pages = offline_bucket().list().prefix("logs/").into_pages();
    assert_send(&pages);
}

#[test]
fn byte_range_formatting_and_validation() {
    use r2kit::ByteRange;

    assert_eq!(
        ByteRange::Bounded(0, 1023).as_header_value(),
        "bytes=0-1023"
    );
    assert_eq!(ByteRange::From(500).as_header_value(), "bytes=500-");
    assert_eq!(ByteRange::Suffix(500).as_header_value(), "bytes=-500");

    let _invalid_bounds = offline_bucket()
        .get_object("key.bin")
        .range(ByteRange::Bounded(100, 50));
    assert_eq!(ByteRange::Bounded(0, 10).as_header_value(), "bytes=0-10");
}

#[tokio::test]
async fn get_object_validates_inputs_before_network() {
    use r2kit::ByteRange;
    let bucket = offline_bucket();

    let invalid_key = bucket.get_object("").send().await.unwrap_err();
    assert!(matches!(
        invalid_key,
        Error::InvalidInput { field: "key", .. }
    ));

    let invalid_range = bucket
        .get_object("valid.bin")
        .range(ByteRange::Bounded(100, 50))
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_range,
        Error::InvalidInput { field: "range", .. }
    ));

    let invalid_suffix = bucket
        .get_object("valid.bin")
        .range(ByteRange::Suffix(0))
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_suffix,
        Error::InvalidInput { field: "range", .. }
    ));

    let empty_if_match = bucket
        .get_object("valid.bin")
        .if_match("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_if_match,
        Error::InvalidInput {
            field: "if_match",
            ..
        }
    ));

    let empty_if_none_match = bucket
        .get_object("valid.bin")
        .if_none_match("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_if_none_match,
        Error::InvalidInput {
            field: "if_none_match",
            ..
        }
    ));
}

#[tokio::test]
async fn head_object_validates_inputs_before_network() {
    let bucket = offline_bucket();

    let invalid_key = bucket.head_object("").send().await.unwrap_err();
    assert!(matches!(
        invalid_key,
        Error::InvalidInput { field: "key", .. }
    ));

    let empty_if_match = bucket
        .head_object("valid.bin")
        .if_match("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_if_match,
        Error::InvalidInput {
            field: "if_match",
            ..
        }
    ));

    let empty_if_none_match = bucket
        .head_object("valid.bin")
        .if_none_match("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_if_none_match,
        Error::InvalidInput {
            field: "if_none_match",
            ..
        }
    ));
}

#[tokio::test]
async fn copy_object_validates_options_before_network() {
    let bucket = offline_bucket();

    let invalid_source_bucket = bucket
        .copy_object("src.jpg", "dst.jpg")
        .source_bucket("in")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_source_bucket,
        Error::InvalidInput {
            field: "bucket",
            ..
        }
    ));

    let empty_if_match = bucket
        .copy_object("src.jpg", "dst.jpg")
        .source_if_match("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_if_match,
        Error::InvalidInput {
            field: "source_if_match",
            ..
        }
    ));

    let empty_if_none_match = bucket
        .copy_object("src.jpg", "dst.jpg")
        .source_if_none_match("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_if_none_match,
        Error::InvalidInput {
            field: "source_if_none_match",
            ..
        }
    ));
}

#[tokio::test]
async fn list_multipart_uploads_validates_options_before_network() {
    let bucket = offline_bucket();

    let zero = bucket
        .list_multipart_uploads()
        .limit(0)
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        zero,
        Error::Validation(ValidationError::ListLimitOutOfRange {
            provided: 0,
            min: 1,
            max: 1_000
        })
    ));

    let too_large = bucket
        .list_multipart_uploads()
        .limit(1_001)
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        too_large,
        Error::Validation(ValidationError::ListLimitOutOfRange {
            provided: 1_001,
            min: 1,
            max: 1_000
        })
    ));

    let empty_delimiter = bucket
        .list_multipart_uploads()
        .delimiter("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_delimiter,
        Error::InvalidInput {
            field: "delimiter",
            ..
        }
    ));

    let empty_key_marker = bucket
        .list_multipart_uploads()
        .key_marker("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_key_marker,
        Error::InvalidInput {
            field: "key_marker",
            ..
        }
    ));

    let empty_upload_id_marker = bucket
        .list_multipart_uploads()
        .upload_id_marker("")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        empty_upload_id_marker,
        Error::InvalidInput {
            field: "upload_id_marker",
            ..
        }
    ));
}

#[test]
fn list_multipart_uploads_exposes_a_sendable_page_stream() {
    fn assert_send<T: Send>(_: &T) {}

    let pages = offline_bucket()
        .list_multipart_uploads()
        .prefix("uploads/")
        .into_pages();
    assert_send(&pages);
}

#[tokio::test]
async fn bucket_management_validates_bucket_name_before_network() {
    let client = offline_client();

    assert!(matches!(
        client.create_bucket("ab").await.unwrap_err(),
        Error::InvalidInput {
            field: "bucket",
            ..
        }
    ));

    assert!(matches!(
        client.delete_bucket("-invalid").await.unwrap_err(),
        Error::InvalidInput {
            field: "bucket",
            ..
        }
    ));

    assert!(matches!(
        client.bucket_exists("INVALID_NAME").await.unwrap_err(),
        Error::InvalidInput {
            field: "bucket",
            ..
        }
    ));
}

#[tokio::test]
async fn download_file_validates_key_before_network() {
    let bucket = offline_bucket();
    let error = bucket.download_file("", "/tmp/test.tmp").await.unwrap_err();
    assert!(matches!(error, Error::InvalidInput { field: "key", .. }));
}

#[test]
fn upload_options_validates_checksum_inputs() {
    use r2kit::{ChecksumAlgorithm, ObjectUploadOptions};

    // Valid CRC32 Base64 (4 bytes)
    let options = ObjectUploadOptions::builder()
        .checksum_value(ChecksumAlgorithm::Crc32, "AAAAAA==")
        .build();
    assert!(!options.is_empty());

    // Invalid base64
    let options = ObjectUploadOptions::builder()
        .checksum_value(ChecksumAlgorithm::Crc32, "not-base-64!!")
        .build();
    let bucket = offline_bucket();
    let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
        bucket
            .put_stream_with_options("key", ByteStream::from_static(&[]), 0, options)
            .await
    });
    assert!(matches!(
        result.unwrap_err(),
        Error::InvalidInput {
            field: "checksum",
            ..
        }
    ));

    // Wrong digest length for CRC32 (1 byte instead of 4)
    let options = ObjectUploadOptions::builder()
        .checksum_value(ChecksumAlgorithm::Crc32, "AQ==")
        .build();
    let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
        bucket
            .put_stream_with_options("key", ByteStream::from_static(&[]), 0, options)
            .await
    });
    assert!(matches!(
        result.unwrap_err(),
        Error::InvalidInput {
            field: "checksum",
            ..
        }
    ));
}
