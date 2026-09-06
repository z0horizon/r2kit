use base64::{Engine as _, engine::general_purpose::STANDARD};
use proptest::{collection, prelude::*};
use r2kit::{
    CompletionManifest, MultipartPartReceipt, MultipartSessionSnapshot, PartMd5, PartNumber,
    R2Client, R2Config,
};

const MIB: u64 = 1024 * 1024;

fn offline_bucket() -> r2kit::Bucket {
    let config = R2Config::builder()
        .account_id("0123456789abcdef0123456789abcdef")
        .access_key_id("property-access-key")
        .secret_access_key("property-secret-key")
        .build()
        .unwrap();
    R2Client::new(config).bucket("property-bucket").unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 2_048,
        max_shrink_iters: 20_000,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn multipart_plan_covers_every_byte_exactly(
        part_size_mib in 5_u64..=64,
        part_count in 1_u16..=1_024,
        tail_seed in any::<u64>(),
    ) {
        let part_size = part_size_mib * MIB;
        let tail = tail_seed % part_size + 1;
        let file_size = u64::from(part_count - 1) * part_size + tail;
        let snapshot = MultipartSessionSnapshot::restore(
            "property-bucket",
            "property/object.bin",
            "property-upload-id",
            file_size,
            part_size,
        )
        .unwrap();
        let session = offline_bucket().resume_presigned_multipart(snapshot).unwrap();

        prop_assert_eq!(session.part_count(), part_count);
        let mut covered = 0_u64;
        for raw in 1..=part_count {
            let number = PartNumber::try_from(raw).unwrap();
            let length = session.part_length(number).unwrap();
            prop_assert!(length > 0);
            prop_assert!(length <= part_size);
            if raw < part_count {
                prop_assert_eq!(length, part_size);
            } else {
                prop_assert_eq!(length, tail);
            }
            covered = covered.checked_add(length).unwrap();
        }
        prop_assert_eq!(covered, file_size);
    }

    #[test]
    fn manifest_canonicalization_preserves_every_unique_receipt(
        parts in collection::btree_map(1_u16..=10_000, "[A-Za-z0-9]{1,40}", 1..256),
    ) {
        let mut receipts: Vec<_> = parts
            .iter()
            .rev()
            .map(|(&number, etag)| MultipartPartReceipt::new(number, etag.clone()))
            .collect();
        let manifest = CompletionManifest::try_from_receipts(receipts.drain(..)).unwrap();
        let actual: Vec<_> = manifest
            .parts()
            .map(|part| (part.part_number().get(), part.etag().to_owned()))
            .collect();
        let expected: Vec<_> = parts.into_iter().collect();
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn duplicate_receipts_are_always_rejected(
        number in 1_u16..=10_000,
        first in "[A-Za-z0-9]{1,40}",
        second in "[A-Za-z0-9]{1,40}",
    ) {
        let result = CompletionManifest::try_from_receipts([
            MultipartPartReceipt::new(number, first),
            MultipartPartReceipt::new(number, second),
        ]);
        prop_assert!(result.is_err());
    }

    #[test]
    fn md5_accepts_exactly_canonical_128_bit_values(bytes in any::<[u8; 16]>()) {
        let encoded = STANDARD.encode(bytes);
        let checksum = PartMd5::try_from(encoded.as_str()).unwrap();
        prop_assert_eq!(checksum.as_base64(), encoded.as_str());

        let unpadded = encoded.trim_end_matches('=');
        prop_assert!(PartMd5::try_from(unpadded).is_err());
    }

    #[test]
    fn snapshot_debug_never_exposes_random_upload_ids(secret in "[A-Za-z0-9_-]{16,96}") {
        let snapshot = MultipartSessionSnapshot::restore(
            "property-bucket",
            "property/object.bin",
            secret.clone(),
            5 * MIB,
            5 * MIB,
        )
        .unwrap();
        let snapshot_debug = format!("{snapshot:?}");
        prop_assert!(!snapshot_debug.contains(&secret));
        let record = snapshot.into_persistence_record();
        let record_debug = format!("{record:?}");
        prop_assert!(!record_debug.contains(&secret));
    }

    #[test]
    fn utf8_key_limit_is_measured_in_bytes(char_count in 257_usize..=400) {
        let key = "🦀".repeat(char_count);
        let result = MultipartSessionSnapshot::restore(
            "property-bucket",
            key,
            "property-upload-id",
            5 * MIB,
            5 * MIB,
        );
        prop_assert!(result.is_err());
    }
}

#[test]
fn multipart_boundaries_are_exact() {
    assert!(PartNumber::try_from(0).is_err());
    assert!(PartNumber::try_from(1).is_ok());
    assert!(PartNumber::try_from(10_000).is_ok());
    assert!(PartNumber::try_from(10_001).is_err());

    assert!(
        MultipartSessionSnapshot::restore(
            "property-bucket",
            "object.bin",
            "upload-id",
            1,
            5 * MIB,
        )
        .is_ok()
    );
    assert!(
        MultipartSessionSnapshot::restore(
            "property-bucket",
            "object.bin",
            "upload-id",
            0,
            5 * MIB,
        )
        .is_err()
    );
}

#[cfg(feature = "serde")]
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn persistence_json_round_trips_random_valid_sessions(
        part_size_mib in 5_u64..=64,
        part_count in 1_u16..=1_024,
        tail_seed in any::<u64>(),
        upload_id in "[A-Za-z0-9_-]{16,96}",
    ) {
        let part_size = part_size_mib * MIB;
        let tail = tail_seed % part_size + 1;
        let file_size = u64::from(part_count - 1) * part_size + tail;
        let original = MultipartSessionSnapshot::restore(
            "property-bucket",
            "property/object.bin",
            upload_id,
            file_size,
            part_size,
        )
        .unwrap();
        let json = serde_json::to_vec(&original.into_persistence_record()).unwrap();
        let record = serde_json::from_slice(&json).unwrap();
        let restored = MultipartSessionSnapshot::from_persistence_record(record).unwrap();

        prop_assert_eq!(restored.bucket(), "property-bucket");
        prop_assert_eq!(restored.key(), "property/object.bin");
        prop_assert_eq!(restored.file_size(), file_size);
        prop_assert_eq!(restored.part_size(), part_size);
    }

    #[test]
    fn byte_range_bounded_invariants(start in 0_u64..1_000_000, len in 0_u64..1_000_000) {
        let end = start + len;
        let range = r2kit::ByteRange::Bounded(start, end);
        prop_assert_eq!(range.as_header_value(), format!("bytes={start}-{end}"));
    }

    #[test]
    fn byte_range_from_invariants(offset in any::<u64>()) {
        let range = r2kit::ByteRange::From(offset);
        prop_assert_eq!(range.as_header_value(), format!("bytes={offset}-"));
    }

    #[test]
    fn byte_range_suffix_invariants(length in 1_u64..=u64::MAX) {
        let range = r2kit::ByteRange::Suffix(length);
        prop_assert_eq!(range.as_header_value(), format!("bytes=-{length}"));
    }
}
