use crate::ServiceError;

#[derive(Debug)]
#[cfg_attr(not(feature = "tracing"), allow(dead_code))]
enum SafeEvent {
    RemoteFailure {
        operation: &'static str,
        kind: &'static str,
        status: Option<u16>,
    },
    Preflight {
        phase: &'static str,
    },
    ManagedUpload {
        phase: &'static str,
        part_size: u64,
        concurrency: usize,
        max_attempts: u8,
    },
    UploadPartRetry {
        part_number: u16,
        attempt: u8,
        max_attempts: u8,
        delay_ms: u64,
    },
}

pub(crate) fn remote_failure(error: &ServiceError) {
    emit(SafeEvent::RemoteFailure {
        operation: error.operation(),
        kind: error.kind().as_str(),
        status: error.status(),
    });
}

pub(crate) fn preflight(phase: &'static str) {
    emit(SafeEvent::Preflight { phase });
}

pub(crate) fn managed_upload(
    phase: &'static str,
    part_size: u64,
    concurrency: usize,
    max_attempts: u8,
) {
    emit(SafeEvent::ManagedUpload {
        phase,
        part_size,
        concurrency,
        max_attempts,
    });
}

pub(crate) fn upload_part_retry(part_number: u16, attempt: u8, max_attempts: u8, delay_ms: u64) {
    emit(SafeEvent::UploadPartRetry {
        part_number,
        attempt,
        max_attempts,
        delay_ms,
    });
}

#[cfg(feature = "tracing")]
fn emit(event: SafeEvent) {
    match event {
        SafeEvent::RemoteFailure {
            operation,
            kind,
            status,
        } => tracing::warn!(
            target: "r2kit",
            operation,
            error.kind = kind,
            http.status = ?status,
            "R2 request failed"
        ),
        SafeEvent::Preflight { phase } => tracing::debug!(
            target: "r2kit",
            operation = "ListObjectsV2",
            preflight.phase = phase,
            "R2 bucket preflight"
        ),
        SafeEvent::ManagedUpload {
            phase,
            part_size,
            concurrency,
            max_attempts,
        } => tracing::debug!(
            target: "r2kit",
            operation = "ManagedMultipartUpload",
            upload.phase = phase,
            part_size,
            concurrency,
            max_attempts,
            "managed R2 upload"
        ),
        SafeEvent::UploadPartRetry {
            part_number,
            attempt,
            max_attempts,
            delay_ms,
        } => tracing::warn!(
            target: "r2kit",
            operation = "UploadPart",
            part_number,
            attempt,
            max_attempts,
            retry.delay_ms = delay_ms,
            "retrying R2 upload part"
        ),
    }
}

#[cfg(not(feature = "tracing"))]
fn emit(_event: SafeEvent) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_payloads_cannot_carry_sensitive_dimensions() {
        let events = [
            SafeEvent::RemoteFailure {
                operation: "PutObject",
                kind: "permission denied",
                status: Some(403),
            },
            SafeEvent::Preflight { phase: "start" },
            SafeEvent::ManagedUpload {
                phase: "start",
                part_size: 8 * 1024 * 1024,
                concurrency: 4,
                max_attempts: 3,
            },
            SafeEvent::UploadPartRetry {
                part_number: 2,
                attempt: 1,
                max_attempts: 3,
                delay_ms: 264,
            },
        ];
        let fields = events
            .iter()
            .map(|event| format!("{event:?}"))
            .collect::<String>()
            .to_ascii_lowercase();

        for forbidden in [
            "bucket",
            "key",
            "path",
            "account",
            "credential",
            "upload_id",
            "url",
            "header",
        ] {
            assert!(
                !fields.contains(forbidden),
                "unsafe event field: {forbidden}"
            );
        }
        for required in [
            "operation",
            "kind",
            "status",
            "part_size",
            "concurrency",
            "max_attempts",
            "part_number",
            "attempt",
            "delay_ms",
        ] {
            assert!(fields.contains(required), "missing event field: {required}");
        }
    }
}
