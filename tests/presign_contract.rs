use std::time::{Duration, UNIX_EPOCH};

use r2kit::{
    CacheControl, Error, MultipartSessionSnapshot, ObjectUploadOptions, PartMd5, PartNumber,
    R2Client, R2Config, ValidationError, mime,
};

fn offline_bucket() -> r2kit::Bucket {
    let config = R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("contract-access-key")
        .secret_access_key("contract-secret-key")
        .session_token("contract-session-token")
        .build()
        .unwrap();
    R2Client::new(config).bucket("r2kit").unwrap()
}

#[tokio::test]
async fn presigns_upload_part_without_exposing_secrets_in_debug() {
    let config = R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("contract-access-key")
        .secret_access_key("contract-secret-key")
        .session_token("contract-session-token")
        .build()
        .unwrap();
    let client = R2Client::new(config);
    let client_debug = format!("{client:?}");
    assert!(!client_debug.contains("contract-access-key"));
    assert!(!client_debug.contains("contract-secret-key"));
    assert!(!client_debug.contains("contract-session-token"));
    let bucket = client.bucket("r2kit").unwrap();
    let snapshot = MultipartSessionSnapshot::restore(
        "r2kit",
        "_r2kit-tests/offline/file.bin",
        "offline-upload-id",
        11 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();
    let session = bucket.resume_presigned_multipart(snapshot).unwrap();
    let part = session
        .presign_part(PartNumber::try_from(2).unwrap(), Duration::from_secs(900))
        .await
        .unwrap();

    assert_eq!(part.request().method(), "PUT");
    assert_eq!(part.content_length(), 5 * 1024 * 1024);
    let exposed = part.request().url().expose();
    assert!(exposed.contains("partNumber=2"));
    assert!(exposed.contains("uploadId=offline-upload-id"));
    assert!(exposed.contains("X-Amz-Signature="));

    let debug = format!("{part:?}");
    assert!(!debug.contains("X-Amz-Signature"));
    assert!(!debug.contains("contract-access-key"));
    assert!(!debug.contains("contract-session-token"));
}

#[tokio::test]
async fn rejects_invalid_presign_expiry_before_network() {
    let config = R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("access")
        .secret_access_key("secret")
        .build()
        .unwrap();
    let bucket = R2Client::new(config).bucket("r2kit").unwrap();
    let snapshot = MultipartSessionSnapshot::restore(
        "r2kit",
        "key",
        "upload-id",
        5 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();
    let session = bucket.resume_presigned_multipart(snapshot).unwrap();
    let part = PartNumber::try_from(1).unwrap();

    assert!(matches!(
        session.presign_part(part, Duration::ZERO).await,
        Err(Error::Validation(
            ValidationError::PresignExpiryOutOfRange {
                provided: Duration::ZERO,
                min,
                max
            }
        )) if min == Duration::from_secs(1) && max == Duration::from_secs(604_800)
    ));
    let too_long = Duration::from_secs(604_801);
    assert!(matches!(
        session.presign_part(part, too_long).await,
        Err(Error::Validation(
            ValidationError::PresignExpiryOutOfRange { provided, .. }
        )) if provided == too_long
    ));
}

#[tokio::test]
async fn checksum_presign_requires_content_md5_and_exposes_a_redacted_protocol_dto() {
    let bucket = offline_bucket();
    let snapshot = MultipartSessionSnapshot::restore(
        "r2kit",
        "objects/checksummed.bin",
        "checksum-upload-id",
        5 * 1024 * 1024,
        5 * 1024 * 1024,
    )
    .unwrap();
    let session = bucket.resume_presigned_multipart(snapshot).unwrap();
    let md5 = PartMd5::try_from("AAAAAAAAAAAAAAAAAAAAAA==").unwrap();
    let part = session
        .presign_part_with_md5(
            PartNumber::try_from(1).unwrap(),
            md5,
            Duration::from_secs(900),
        )
        .await
        .unwrap();

    assert_eq!(
        part.content_md5().map(PartMd5::as_base64),
        Some("AAAAAAAAAAAAAAAAAAAAAA==")
    );
    assert!(part.request().required_headers().any(|(name, value)| {
        name.eq_ignore_ascii_case("content-md5") && value == "AAAAAAAAAAAAAAAAAAAAAA=="
    }));

    let protocol = part.into_protocol_request().unwrap();
    assert_eq!(protocol.part_number(), 1);
    assert_eq!(protocol.content_length(), 5 * 1024 * 1024);
    assert_eq!(protocol.content_md5(), Some("AAAAAAAAAAAAAAAAAAAAAA=="));
    assert!(protocol.expose_url().contains("X-Amz-Signature="));
    assert!(!format!("{protocol:?}").contains("X-Amz-Signature"));
}

#[test]
fn rejects_noncanonical_or_wrong_length_md5() {
    assert!(PartMd5::try_from("not-base64").is_err());
    assert!(PartMd5::try_from("YWJj").is_err());
    assert!(PartMd5::try_from("AAAAAAAAAAAAAAAAAAAAAA").is_err());
}

#[tokio::test]
async fn presigns_single_get_and_put_with_redacted_bearer_urls() {
    let bucket = offline_bucket();
    let get = bucket
        .presign_get("objects/file.bin", Duration::from_secs(900))
        .await
        .unwrap();
    assert_eq!(get.method(), "GET");
    assert!(get.url().expose().contains("X-Amz-Signature="));
    assert!(!format!("{get:?}").contains("X-Amz-Signature"));

    let put = bucket
        .presign_put("objects/file.bin", 42, Duration::from_secs(900))
        .await
        .unwrap();
    assert_eq!(put.content_length(), 42);
    assert_eq!(put.request().method(), "PUT");
    assert!(put.request().url().expose().contains("X-Amz-Signature="));
    assert!(!format!("{put:?}").contains("X-Amz-Signature"));
}
#[tokio::test]
async fn presigned_delete_validates_key_and_expiry() {
    let bucket = offline_bucket();
    assert!(matches!(
        bucket
            .presign_delete("", Duration::from_secs(900))
            .await
            .unwrap_err(),
        r2kit::Error::InvalidInput { field: "key", .. }
    ));

    assert!(matches!(
        bucket
            .presign_delete("key", Duration::from_secs(999999999))
            .await
            .unwrap_err(),
        r2kit::Error::Validation(r2kit::ValidationError::PresignExpiryOutOfRange { .. })
    ));

    let req = bucket
        .presign_delete("key", Duration::from_secs(900))
        .await
        .unwrap();
    assert_eq!(req.method(), "DELETE");
}

#[tokio::test]
async fn presigned_put_signs_typed_object_metadata_as_required_headers() {
    let bucket = offline_bucket();
    let options = ObjectUploadOptions::new()
        .with_content_type(mime::IMAGE_JPEG)
        .with_cache_control(
            CacheControl::new()
                .with_public()
                .with_max_age(Duration::from_secs(3_600)),
        );
    let put = bucket
        .presign_put_with_options("photos/cat.jpg", 42, Duration::from_secs(900), options)
        .await
        .unwrap();
    let headers: Vec<_> = put.request().required_headers().collect();

    assert!(headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("content-type") && *value == "image/jpeg"
    }));
    assert!(headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("cache-control")
            && value.contains("public")
            && value.contains("max-age=3600")
    }));
    let url = put.request().url().expose();
    assert!(url.contains("X-Amz-SignedHeaders="));
    assert!(!format!("{put:?}").contains("X-Amz-Signature"));
}

#[tokio::test]
async fn presigned_put_signs_extended_headers_and_custom_metadata() {
    let bucket = offline_bucket();
    let expires = UNIX_EPOCH + Duration::from_secs(1_893_456_000);
    let options = ObjectUploadOptions::new()
        .with_content_disposition("attachment; filename=report.csv")
        .with_content_encoding("gzip")
        .with_content_language("en-US")
        .with_expires(expires)
        .with_custom_metadata("tenant-id", "tenant-42");
    let put = bucket
        .presign_put_with_options(
            "reports/report.csv.gz",
            42,
            Duration::from_secs(900),
            options,
        )
        .await
        .unwrap();
    let headers: Vec<_> = put.request().required_headers().collect();

    for (name, expected) in [
        ("content-disposition", "attachment; filename=report.csv"),
        ("content-encoding", "gzip"),
        ("content-language", "en-US"),
        ("x-amz-meta-tenant-id", "tenant-42"),
    ] {
        assert!(
            headers
                .iter()
                .any(|(actual, value)| actual.eq_ignore_ascii_case(name) && *value == expected),
            "missing signed header {name}"
        );
    }
    assert!(
        headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("expires"))
    );
}

#[tokio::test]
async fn rejects_invalid_upload_metadata_before_signing() {
    let bucket = offline_bucket();
    let duplicate = ObjectUploadOptions::new()
        .with_custom_metadata("Tenant", "one")
        .with_custom_metadata("tenant", "two");
    let error = bucket
        .presign_put_with_options("key", 1, Duration::from_secs(60), duplicate)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::InvalidInput {
            field: "custom_metadata",
            ..
        }
    ));

    let prefixed = ObjectUploadOptions::new().with_custom_metadata("x-amz-meta-tenant", "one");
    let error = bucket
        .presign_put_with_options("key", 1, Duration::from_secs(60), prefixed)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::InvalidInput {
            field: "custom_metadata",
            ..
        }
    ));
}

#[tokio::test]
async fn rejects_invalid_single_object_presign_contracts() {
    let bucket = offline_bucket();

    assert!(matches!(
        bucket.presign_get("key", Duration::from_millis(999)).await,
        Err(Error::Validation(
            ValidationError::PresignExpiryOutOfRange { .. }
        ))
    ));
    assert!(matches!(
        bucket
            .presign_put(
                "key",
                5 * 1024 * 1024 * 1024 - 5 * 1024 * 1024 + 1,
                Duration::from_secs(60),
            )
            .await,
        Err(Error::Validation(ValidationError::SingleUploadTooLarge {
            provided: 5_363_466_241,
            max: 5_363_466_240
        }))
    ));
}

#[tokio::test]
async fn presign_put_rejects_auto_checksum_without_precomputed_value() {
    let bucket = offline_bucket();
    let options = ObjectUploadOptions::new().with_checksum(r2kit::ChecksumAlgorithm::Sha256);
    let result = bucket
        .presign_put_with_options("test-key", 0, Duration::from_secs(60), options)
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
async fn presigned_multipart_rejects_checksum_and_conditional_headers() {
    let bucket = offline_bucket();

    let options_checksum =
        ObjectUploadOptions::new().with_checksum(r2kit::ChecksumAlgorithm::Sha256);
    let result = bucket
        .presigned_multipart("test-key")
        .unwrap()
        .file_size(10 * 1024 * 1024)
        .part_size_mib(5)
        .upload_options(options_checksum)
        .create()
        .await;
    assert!(matches!(
        result.unwrap_err(),
        Error::InvalidInput {
            field: "checksum",
            ..
        }
    ));

    let options_if_match = ObjectUploadOptions::new().with_if_match("etag");
    let result = bucket
        .presigned_multipart("test-key")
        .unwrap()
        .file_size(10 * 1024 * 1024)
        .part_size_mib(5)
        .upload_options(options_if_match)
        .create()
        .await;
    assert!(matches!(
        result.unwrap_err(),
        Error::InvalidInput {
            field: "if_match",
            ..
        }
    ));
}

#[tokio::test]
async fn presigned_requests_support_into_content_type_variants() {
    let bucket = offline_bucket();

    // 1. &str
    let opts_str = ObjectUploadOptions::new().with_content_type("application/json");
    let put_str = bucket
        .presign_put_with_options("test.json", 100, Duration::from_secs(900), opts_str)
        .await
        .unwrap();
    let headers: Vec<_> = put_str.request().required_headers().collect();
    assert!(
        headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("content-type") && *v == "application/json")
    );

    // 2. String
    let opts_string = ObjectUploadOptions::new().with_content_type(String::from("text/plain"));
    let put_string = bucket
        .presign_put_with_options("test.txt", 100, Duration::from_secs(900), opts_string)
        .await
        .unwrap();
    let headers: Vec<_> = put_string.request().required_headers().collect();
    assert!(
        headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("content-type") && *v == "text/plain")
    );

    // 3. mime::Mime
    let opts_mime = ObjectUploadOptions::new().with_content_type(mime::APPLICATION_OCTET_STREAM);
    let put_mime = bucket
        .presign_put_with_options("test.bin", 100, Duration::from_secs(900), opts_mime)
        .await
        .unwrap();
    let headers: Vec<_> = put_mime.request().required_headers().collect();
    assert!(
        headers.iter().any(
            |(k, v)| k.eq_ignore_ascii_case("content-type") && *v == "application/octet-stream"
        )
    );

    // PresignedMultipartBuilder.content_type variants
    let _b1 = bucket
        .presigned_multipart("test.json")
        .unwrap()
        .content_type("application/json");
    let _b2 = bucket
        .presigned_multipart("test.txt")
        .unwrap()
        .content_type(String::from("text/plain"));
    let _b3 = bucket
        .presigned_multipart("test.bin")
        .unwrap()
        .content_type(mime::APPLICATION_OCTET_STREAM);
}

#[tokio::test]
async fn into_content_type_rejects_invalid_mime_offline() {
    let bucket = offline_bucket();
    let opts_invalid = ObjectUploadOptions::new().with_content_type("not a valid mime type");
    let result = bucket
        .presign_put_with_options("test.bin", 100, Duration::from_secs(900), opts_invalid)
        .await;
    assert!(matches!(
        result.unwrap_err(),
        Error::InvalidInput {
            field: "content_type",
            ..
        }
    ));

    let result_mp = bucket
        .presigned_multipart("test.bin")
        .unwrap()
        .file_size(10 * 1024 * 1024)
        .part_size_mib(5)
        .content_type("invalid mime type")
        .create()
        .await;
    assert!(matches!(
        result_mp.unwrap_err(),
        Error::InvalidInput {
            field: "content_type",
            ..
        }
    ));
}

#[tokio::test]
async fn presigned_url_accessors_expose_url_while_redacting_debug() {
    let bucket = offline_bucket();
    let put = bucket
        .presign_put("photos/vacation.jpg", 1024, Duration::from_secs(900))
        .await
        .unwrap();

    // Test PresignedPutObject accessors
    let url_slice: &str = put.as_str();
    assert!(url_slice.starts_with("https://"));
    assert!(url_slice.contains("X-Amz-Signature="));
    assert!(url_slice.contains("photos/vacation.jpg"));

    // Debug on PresignedPutObject should redact the URL
    let put_debug = format!("{put:?}");
    assert!(!put_debug.contains("X-Amz-Signature="));
    assert!(put_debug.contains("[REDACTED PRESIGNED URL]"));

    // PresignedRequest accessors
    let req = put.request();
    let req_url_slice: &str = req.as_str();
    assert_eq!(req_url_slice, url_slice);

    let req_debug = format!("{req:?}");
    assert!(!req_debug.contains("X-Amz-Signature="));
    assert!(req_debug.contains("[REDACTED PRESIGNED URL]"));

    // Test into_url_string on PresignedRequest
    let req_cloned = req.clone();
    let req_url_str: String = req_cloned.into_url_string();
    assert_eq!(req_url_str, url_slice);

    // Test into_url_string on PresignedPutObject
    let expected_url = url_slice.to_string();
    let put_url_str: String = put.into_url_string();
    assert_eq!(put_url_str, expected_url);
}

#[test]
fn upload_threshold_validates_offline() {
    let result = r2kit::UploadThreshold::new(4 * 1024 * 1024);
    assert!(matches!(
        result,
        Err(ValidationError::PartSizeOutOfRange {
            provided: 4_194_304,
            min: 5_242_880,
            ..
        })
    ));

    let valid = r2kit::UploadThreshold::new(8 * 1024 * 1024).unwrap();
    assert_eq!(valid.get(), 8 * 1024 * 1024);
    assert_eq!(r2kit::UploadThreshold::default().get(), 8 * 1024 * 1024);

    let too_large = r2kit::UploadThreshold::new(u64::MAX);
    assert!(matches!(
        too_large,
        Err(ValidationError::PartSizeOutOfRange { .. })
    ));
}

#[tokio::test]
async fn presign_upload_returns_single_for_sub_threshold() {
    let bucket = offline_bucket();
    let plan = bucket
        .presign_upload("small.txt", 2 * 1024 * 1024, Duration::from_secs(900))
        .await
        .unwrap();

    assert!(!plan.is_multipart());
    match plan {
        r2kit::PresignedUploadPlan::Single(put) => {
            assert_eq!(put.content_length(), 2 * 1024 * 1024);
            assert!(put.as_str().contains("small.txt"));
        }
        r2kit::PresignedUploadPlan::Multipart(_) => panic!("expected single PUT plan"),
    }
}

#[tokio::test]
async fn presign_upload_handles_zero_byte_as_single() {
    let bucket = offline_bucket();
    let plan = bucket
        .presign_upload("empty.txt", 0, Duration::from_secs(900))
        .await
        .unwrap();

    assert!(!plan.is_multipart());
    match plan {
        r2kit::PresignedUploadPlan::Single(put) => {
            assert_eq!(put.content_length(), 0);
            assert!(put.as_str().contains("empty.txt"));
        }
        r2kit::PresignedUploadPlan::Multipart(_) => panic!("expected single PUT plan"),
    }
}

#[tokio::test]
async fn presign_upload_returns_multipart_for_above_threshold() {
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
                req.starts_with("POST "),
                "expected POST request, got: {req}"
            );
            assert!(req.contains("uploads"), "expected ?uploads query parameter");

            let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>contract-tests</Bucket>
    <Key>large.bin</Key>
    <UploadId>offline-upload-id-12345</UploadId>
</InitiateMultipartUploadResult>"#;
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

    let plan = bucket
        .presign_upload("large.bin", 12 * 1024 * 1024, Duration::from_secs(900))
        .await
        .unwrap();
    let _ = server.join();

    assert!(plan.is_multipart());
    match plan {
        r2kit::PresignedUploadPlan::Multipart(multi) => {
            assert_eq!(multi.upload_id(), "offline-upload-id-12345");
            assert_eq!(multi.file_size(), 12 * 1024 * 1024);
            assert_eq!(multi.part_size(), 8 * 1024 * 1024);
            assert_eq!(multi.part_count(), 2);

            let debug = format!("{multi:?}");
            assert!(!debug.contains("offline-upload-id-12345"));
            assert!(debug.contains("[REDACTED]"));
        }
        r2kit::PresignedUploadPlan::Single(_) => panic!("expected multipart plan"),
    }
}

#[tokio::test]
async fn presign_upload_validates_expiry_offline_for_both_branches() {
    let bucket = offline_bucket();

    // Small file (< threshold) with 0s expiry fails offline before signing
    let small_res = bucket
        .presign_upload("small.txt", 1024, Duration::ZERO)
        .await;
    assert!(matches!(
        small_res.unwrap_err(),
        Error::Validation(ValidationError::PresignExpiryOutOfRange {
            provided,
            ..
        }) if provided == Duration::ZERO
    ));

    // Large file (>= threshold) with 0s expiry fails offline before any network I/O
    let large_res = bucket
        .presign_upload("large.bin", 12 * 1024 * 1024, Duration::ZERO)
        .await;
    assert!(matches!(
        large_res.unwrap_err(),
        Error::Validation(ValidationError::PresignExpiryOutOfRange {
            provided,
            ..
        }) if provided == Duration::ZERO
    ));
}
