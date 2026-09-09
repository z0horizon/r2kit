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
