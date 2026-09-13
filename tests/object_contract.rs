use aws_sdk_s3::{config::Credentials, primitives::ByteStream};
use r2kit::{ChecksumAlgorithm, Error, ObjectUploadOptions, R2Client, R2Config, ValidationError};

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
fn listing_exposes_a_sendable_object_stream() {
    fn assert_send<T: Send>(_: &T) {}

    let stream = offline_bucket().list().prefix("logs/").into_stream();
    assert_send(&stream);

    let objects_stream = offline_bucket().list().prefix("logs/").into_objects();
    assert_send(&objects_stream);
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
    let options =
        ObjectUploadOptions::new().with_checksum_value(ChecksumAlgorithm::Crc32, "AAAAAA==");
    assert!(!options.is_empty());

    // Invalid base64
    let options =
        ObjectUploadOptions::new().with_checksum_value(ChecksumAlgorithm::Crc32, "not-base-64!!");
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
    let options = ObjectUploadOptions::new().with_checksum_value(ChecksumAlgorithm::Crc32, "AQ==");
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

#[tokio::test]
async fn get_bytes_rejects_invalid_key_before_network() {
    let bucket = offline_bucket();
    let result = bucket.get_bytes("").await;
    assert!(matches!(
        result.unwrap_err(),
        Error::InvalidInput { field: "key", .. }
    ));
}

#[tokio::test]
async fn put_stream_rejects_auto_checksum_without_precomputed_value() {
    let bucket = offline_bucket();
    let options = ObjectUploadOptions::new().with_checksum(ChecksumAlgorithm::Sha256);
    let result = bucket
        .put_stream_with_options("test-key", ByteStream::from_static(b""), 0, options)
        .await;
    assert!(matches!(
        result.unwrap_err(),
        Error::InvalidInput {
            field: "checksum",
            ..
        }
    ));
}

#[tokio::test]
async fn copy_object_validates_metadata_options_before_network() {
    let bucket = offline_bucket();
    let invalid_options =
        ObjectUploadOptions::new().with_content_language("invalid language tag!!!");
    let result = bucket
        .copy_object("src.txt", "dst.txt")
        .metadata_directive(r2kit::MetadataDirective::Replace)
        .upload_options(invalid_options)
        .send()
        .await;
    assert!(matches!(
        result.unwrap_err(),
        Error::InvalidInput {
            field: "content_language",
            ..
        }
    ));
}

#[tokio::test]
async fn conditional_headers_validate_and_build_before_network() {
    let bucket = offline_bucket();
    let now = std::time::SystemTime::now();

    // GetObjectBuilder with conditional headers
    let get_req = bucket
        .get_object("key.txt")
        .if_modified_since(now)
        .if_unmodified_since(now)
        .if_match("etag-1")
        .if_none_match("etag-2");
    assert!(format!("{get_req:?}").contains("key.txt"));

    // HeadObjectBuilder with conditional headers
    let head_req = bucket
        .head_object("key.txt")
        .if_modified_since(now)
        .if_unmodified_since(now)
        .if_match("etag-1")
        .if_none_match("etag-2");
    assert!(format!("{head_req:?}").contains("key.txt"));

    // CopyObjectBuilder with source conditional headers
    let copy_req = bucket
        .copy_object("src.txt", "dst.txt")
        .source_if_modified_since(now)
        .source_if_unmodified_since(now)
        .source_if_match("etag-1")
        .source_if_none_match("etag-2")
        .source_bucket("other-bucket")
        .metadata_directive(r2kit::MetadataDirective::Copy);
    assert!(format!("{copy_req:?}").contains("src.txt"));
}

#[tokio::test]
async fn copy_object_rejects_conflicting_or_unsupported_options() {
    let bucket = offline_bucket();
    // Rejects upload_options when directive is Copy
    let err = bucket
        .copy_object("src.txt", "dst.txt")
        .metadata_directive(r2kit::MetadataDirective::Copy)
        .upload_options(ObjectUploadOptions::new())
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput {
            field: "metadata_directive",
            ..
        }
    ));

    // Rejects checksum in upload_options for copy
    let err = bucket
        .copy_object("src.txt", "dst.txt")
        .upload_options(ObjectUploadOptions::new().with_checksum(r2kit::ChecksumAlgorithm::Sha256))
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput {
            field: "checksum",
            ..
        }
    ));

    // Rejects if_match in upload_options for copy
    let err = bucket
        .copy_object("src.txt", "dst.txt")
        .upload_options(ObjectUploadOptions::new().with_if_match("etag"))
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput {
            field: "if_match",
            ..
        }
    ));
}

#[tokio::test]
async fn conditional_headers_reject_newline_injection() {
    let bucket = offline_bucket();
    let err = bucket
        .get_object("key.txt")
        .if_match("etag\r\ninjected: true")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput {
            field: "if_match",
            ..
        }
    ));

    let err = bucket
        .head_object("key.txt")
        .if_none_match("etag\ninjected: true")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput {
            field: "if_none_match",
            ..
        }
    ));

    let err = bucket
        .copy_object("src.txt", "dst.txt")
        .source_if_match("etag\r\ninjected: true")
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput {
            field: "source_if_match",
            ..
        }
    ));
}

#[tokio::test]
async fn copy_object_defaults_to_replace_directive_when_upload_options_present() {
    let bucket = offline_bucket();
    let valid_options = ObjectUploadOptions::new().with_content_type(r2kit::mime::TEXT_PLAIN);
    // Does NOT explicitly call .metadata_directive(...)
    let result = bucket
        .copy_object("src.txt", "dst.txt")
        .upload_options(valid_options)
        .send()
        .await;
    // Input validation succeeds; fails on network/service dispatch because offline client endpoint is dummy
    let err = result.unwrap_err();
    assert!(!matches!(err, Error::InvalidInput { .. }));
}

#[tokio::test]
async fn download_file_cleans_up_file_on_stream_failure() {
    use std::io::Write;
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let response =
                "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nETag: \"etag\"\r\n\r\npartial";
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
            // Drop stream immediately to simulate connection reset / truncated stream
        }
    });

    let sdk_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
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
    let bucket = client.bucket("contract-tests").unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let target_path = temp_dir.path().join("partial_download.bin");

    let result = bucket.download_file("test.bin", &target_path).await;
    let _ = server.join();

    assert!(matches!(
        result.unwrap_err(),
        Error::Io {
            operation: "download_file"
        }
    ));
    assert!(
        !target_path.exists(),
        "partial file must be deleted if download stream fails"
    );
}

#[tokio::test]
async fn list_multipart_uploads_rejects_truncated_response_without_markers() {
    use std::io::Write;
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>contract-tests</Bucket>
    <IsTruncated>true</IsTruncated>
</ListMultipartUploadsResult>"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    let sdk_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
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
    let bucket = client.bucket("contract-tests").unwrap();

    let result = bucket.list_multipart_uploads().send().await;
    let _ = server.join();

    assert!(matches!(
        result.unwrap_err(),
        Error::Service {
            operation: "ListMultipartUploads"
        }
    ));
}

#[tokio::test]
async fn abort_multipart_upload_validates_inputs_before_network() {
    let bucket = offline_bucket();

    let empty_key = bucket
        .abort_multipart_upload("", "upload-123")
        .await
        .unwrap_err();
    assert!(matches!(
        empty_key,
        Error::InvalidInput { field: "key", .. }
    ));

    let empty_upload = bucket
        .abort_multipart_upload("valid-key", "")
        .await
        .unwrap_err();
    assert!(matches!(
        empty_upload,
        Error::InvalidInput {
            field: "upload_id",
            reason: "must not be empty",
        }
    ));

    let whitespace_upload = bucket
        .abort_multipart_upload("valid-key", "   \t\n")
        .await
        .unwrap_err();
    assert!(matches!(
        whitespace_upload,
        Error::InvalidInput {
            field: "upload_id",
            reason: "must not be empty",
        }
    ));
}

#[tokio::test]
async fn abort_multipart_upload_dispatches_request() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(
                req.starts_with("DELETE "),
                "expected DELETE request, got: {req}"
            );
            assert!(
                req.contains("uploadId=test-upload-id"),
                "expected uploadId query parameter in request"
            );

            let response = "HTTP/1.1 204 No Content\r\n\r\n";
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    let sdk_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
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
    let bucket = client.bucket("contract-tests").unwrap();

    let result = bucket
        .abort_multipart_upload("test-key", "test-upload-id")
        .await;
    let _ = server.join();

    assert!(result.is_ok(), "expected Ok(()), got: {result:?}");
}

#[tokio::test]
async fn is_not_found_helper_behavior() {
    assert!(Error::NotFound.is_not_found());

    assert!(!Error::PreconditionFailed.is_not_found());
    assert!(!Error::NotModified.is_not_found());
    assert!(!Error::Cancelled.is_not_found());
    assert!(!Error::Presign.is_not_found());
    assert!(!Error::InvalidSignedHeader.is_not_found());
    assert!(
        !Error::InvalidInput {
            field: "key",
            reason: "must not be empty",
        }
        .is_not_found()
    );

    use std::io::Write;
    use std::net::TcpListener;

    // Verify Error::Remote with 404 yields is_not_found() == true
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    let sdk_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
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
    let bucket = client.bucket("contract-tests").unwrap();

    let err = bucket.list_multipart_uploads().send().await.unwrap_err();
    let _ = server.join();

    assert!(err.is_not_found());
    if let Error::Remote(se) = &err {
        assert_eq!(se.kind(), r2kit::ServiceErrorKind::NotFound);
    } else {
        panic!("expected Error::Remote, got {err:?}");
    }

    // Verify Error::Remote with 403 yields is_not_found() == false
    let listener_403 = TcpListener::bind("127.0.0.1:0").unwrap();
    let port_403 = listener_403.local_addr().unwrap().port();

    let server_403 = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener_403.accept() {
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let response = "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n";
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    let sdk_config_403 = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(format!("http://127.0.0.1:{port_403}"))
        .force_path_style(true)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "dummy-access",
            "dummy-secret",
            None,
            None,
            "contract-test",
        ))
        .build();
    let client_403 = R2Client::from_sdk(aws_sdk_s3::Client::from_conf(sdk_config_403));
    let bucket_403 = client_403.bucket("contract-tests").unwrap();

    let err_403 = bucket_403
        .list_multipart_uploads()
        .send()
        .await
        .unwrap_err();
    let _ = server_403.join();

    assert!(!err_403.is_not_found());
    if let Error::Remote(se) = &err_403 {
        assert_eq!(se.kind(), r2kit::ServiceErrorKind::PermissionDenied);
    } else {
        panic!("expected Error::Remote, got {err_403:?}");
    }
}

#[tokio::test]
async fn remote_404_maps_canonically_to_not_found() {
    use std::io::Write;
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    let sdk_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
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
    let bucket = client.bucket("contract-tests").unwrap();

    let err = bucket
        .abort_multipart_upload("missing-key", "upload-id-404")
        .await
        .unwrap_err();
    let _ = server.join();

    assert_eq!(err, Error::NotFound);
    assert!(err.is_not_found());
}

#[tokio::test]
async fn listing_streams_yield_objects_across_pages() {
    use futures_util::StreamExt;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = std::thread::spawn(move || {
        for _ in 0..2 {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>contract-tests</Name>
    <Prefix>photos/</Prefix>
    <KeyCount>2</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>photos/1.jpg</Key>
        <LastModified>2026-01-01T00:00:00.000Z</LastModified>
        <ETag>"etag1"</ETag>
        <Size>1234</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
    <Contents>
        <Key>photos/2.jpg</Key>
        <LastModified>2026-01-01T00:00:00.000Z</LastModified>
        <ETag>"etag2"</ETag>
        <Size>5678</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
</ListBucketResult>"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        }
    });

    let sdk_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("auto"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
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
    let bucket = client.bucket("contract-tests").unwrap();

    let mut stream = std::pin::pin!(bucket.list().prefix("photos/").into_stream());
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.key(), "photos/1.jpg");
    assert_eq!(first.size(), 1234);

    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.key(), "photos/2.jpg");
    assert_eq!(second.size(), 5678);

    assert!(stream.next().await.is_none());

    let mut obj_stream = std::pin::pin!(bucket.list().prefix("photos/").into_objects());
    let first_obj = obj_stream.next().await.unwrap().unwrap();
    assert_eq!(first_obj.key(), "photos/1.jpg");
    assert_eq!(first_obj.size(), 1234);

    let second_obj = obj_stream.next().await.unwrap().unwrap();
    assert_eq!(second_obj.key(), "photos/2.jpg");
    assert_eq!(second_obj.size(), 5678);

    assert!(obj_stream.next().await.is_none());

    let _ = server.join();
}
