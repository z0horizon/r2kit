use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::Metadata,
    future::Future,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::retry::RetryConfig;
use futures_util::{
    StreamExt, TryStreamExt,
    future::{Either, select},
    stream,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;

use crate::{
    Bucket, CompletedObject, CompletionManifest, Error, IntoObjectKey, MultipartSessionSnapshot,
    ObjectUploadOptions, PartNumber, PresignedMultipart, UploadedPart, ValidationError,
    observability, types,
};

const DEFAULT_PART_SIZE: u64 = 8 * 1024 * 1024;
const DEFAULT_CONCURRENCY: usize = 4;
const DEFAULT_MAX_ATTEMPTS: u8 = 3;
const DEFAULT_MAX_BUFFERED_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CONCURRENCY: usize = 64;
const MAX_ATTEMPTS: u8 = 10;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

type ProgressCallback = Arc<dyn Fn(ManagedUploadProgress) + Send + Sync>;

/// A cloneable signal for explicitly cancelling a managed multipart upload.
///
/// Cancellation is cooperative: pass a clone to the builder, call [`Self::cancel`],
/// and continue awaiting `upload_file` so it can abort or return resumable state.
#[derive(Clone, Debug)]
pub struct ManagedUploadCancellation {
    signal: watch::Sender<bool>,
}

impl ManagedUploadCancellation {
    /// Creates a cancellation signal in the active state.
    #[must_use]
    pub fn new() -> Self {
        let (signal, _) = watch::channel(false);
        Self { signal }
    }

    /// Requests cancellation. Calling this method more than once is harmless.
    pub fn cancel(&self) {
        self.signal.send_replace(true);
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.signal.borrow()
    }

    async fn cancelled(&self) {
        let mut receiver = self.signal.subscribe();
        if *receiver.borrow_and_update() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow_and_update() {
                return;
            }
        }
    }
}

impl Default for ManagedUploadCancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time progress update for a managed multipart upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedUploadProgress {
    completed_parts: u16,
    total_parts: u16,
    transferred_bytes: u64,
    total_bytes: u64,
}

impl ManagedUploadProgress {
    /// Returns the number of uploaded or reused parts.
    #[must_use]
    pub const fn completed_parts(self) -> u16 {
        self.completed_parts
    }

    /// Returns the complete number of planned parts.
    #[must_use]
    pub const fn total_parts(self) -> u16 {
        self.total_parts
    }

    /// Returns bytes uploaded or verified from an existing session.
    #[must_use]
    pub const fn transferred_bytes(self) -> u64 {
        self.transferred_bytes
    }

    /// Returns the complete file size in bytes.
    #[must_use]
    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }

    /// Returns the completed fraction in the inclusive range `0.0..=1.0`.
    ///
    /// Managed multipart plans reject empty files, but the zero check keeps
    /// this point-in-time value safe if it is ever constructed internally
    /// before validation.
    #[must_use]
    pub fn fraction(self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.transferred_bytes as f64 / self.total_bytes as f64
        }
    }

    /// Returns the completed percentage in the inclusive range `0.0..=100.0`.
    #[must_use]
    pub fn percentage(self) -> f64 {
        self.fraction() * 100.0
    }
}

/// Successful outcome of a managed multipart file upload.
#[derive(Clone, Debug)]
pub struct ManagedUploadResult {
    object: CompletedObject,
    file_size: u64,
    part_count: u16,
    uploaded_parts: u16,
    reused_parts: u16,
}

impl ManagedUploadResult {
    /// Returns metadata from completing the R2 multipart object.
    #[must_use]
    pub const fn object(&self) -> &CompletedObject {
        &self.object
    }

    /// Returns the uploaded file size in bytes.
    #[must_use]
    pub const fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Returns the complete number of parts.
    #[must_use]
    pub const fn part_count(&self) -> u16 {
        self.part_count
    }

    /// Returns parts transferred during this invocation.
    #[must_use]
    pub const fn uploaded_parts(&self) -> u16 {
        self.uploaded_parts
    }

    /// Returns existing remote parts reused while resuming.
    #[must_use]
    pub const fn reused_parts(&self) -> u16 {
        self.reused_parts
    }
}

/// Failure from a managed upload, including recoverable session state.
pub struct ManagedUploadError {
    error: Error,
    snapshot: Option<Box<MultipartSessionSnapshot>>,
    aborted: bool,
}

impl ManagedUploadError {
    /// Returns the sanitized underlying failure.
    #[must_use]
    pub const fn error(&self) -> &Error {
        &self.error
    }

    /// Returns resumable state when the upload may still exist remotely.
    #[must_use]
    pub fn snapshot(&self) -> Option<&MultipartSessionSnapshot> {
        self.snapshot.as_deref()
    }

    /// Returns whether automatic abort completed successfully.
    #[must_use]
    pub const fn was_aborted(&self) -> bool {
        self.aborted
    }
}

impl fmt::Debug for ManagedUploadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedUploadError")
            .field("error", &self.error)
            .field("snapshot", &self.snapshot)
            .field("aborted", &self.aborted)
            .finish()
    }
}

impl fmt::Display for ManagedUploadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for ManagedUploadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Configures a bounded, retrying multipart upload from a local file.
#[derive(Clone)]
pub struct ManagedMultipartBuilder {
    bucket: Bucket,
    key: String,
    part_size: u64,
    concurrency: usize,
    max_attempts: u8,
    max_buffered_bytes: u64,
    abort_on_error: bool,
    resume: Option<MultipartSessionSnapshot>,
    progress: Option<ProgressCallback>,
    cancellation: Option<ManagedUploadCancellation>,
    options: ObjectUploadOptions,
}

impl fmt::Debug for ManagedMultipartBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedMultipartBuilder")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("part_size", &self.part_size)
            .field("concurrency", &self.concurrency)
            .field("max_attempts", &self.max_attempts)
            .field("max_buffered_bytes", &self.max_buffered_bytes)
            .field("abort_on_error", &self.abort_on_error)
            .field("resume", &self.resume)
            .field("progress", &self.progress.as_ref().map(|_| "callback"))
            .field("cancellation", &self.cancellation.is_some())
            .field("options", &self.options)
            .finish()
    }
}

impl ManagedMultipartBuilder {
    /// Sets typed metadata for a newly created multipart object.
    ///
    /// Metadata cannot be changed while resuming an existing remote session.
    #[must_use]
    pub fn upload_options(mut self, options: ObjectUploadOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets the completed object's MIME media type.
    #[must_use]
    pub fn content_type(mut self, value: mime::Mime) -> Self {
        self.options = self.options.with_content_type(value);
        self
    }

    /// Sets the completed object's typed HTTP cache policy.
    #[must_use]
    pub fn cache_control(mut self, value: headers::CacheControl) -> Self {
        self.options = self.options.with_cache_control(value);
        self
    }

    /// Sets the completed object's content disposition.
    #[must_use]
    pub fn content_disposition(mut self, value: impl Into<String>) -> Self {
        self.options = self.options.with_content_disposition(value);
        self
    }

    /// Sets the completed object's content encoding.
    #[must_use]
    pub fn content_encoding(mut self, value: impl Into<String>) -> Self {
        self.options = self.options.with_content_encoding(value);
        self
    }

    /// Sets the completed object's content language.
    #[must_use]
    pub fn content_language(mut self, value: impl Into<String>) -> Self {
        self.options = self.options.with_content_language(value);
        self
    }

    /// Sets the completed object's HTTP expiration time.
    #[must_use]
    pub fn expires(mut self, value: SystemTime) -> Self {
        self.options = self.options.with_expires(value);
        self
    }

    /// Adds user-defined metadata to the completed object.
    #[must_use]
    pub fn custom_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options = self.options.with_custom_metadata(key, value);
        self
    }

    /// Sets the uniform non-final part size. Resumed sessions must keep their original size.
    #[must_use]
    pub const fn part_size(mut self, bytes: u64) -> Self {
        self.part_size = bytes;
        self
    }

    /// Sets the multipart part size in mebibytes (MiB).
    ///
    /// This is equivalent to [`Self::part_size`] with a binary-unit conversion
    /// and avoids repeating `1024 * 1024` at call sites. Unrepresentable or
    /// out-of-range values are rejected before file or network I/O.
    #[must_use]
    pub const fn part_size_mib(mut self, mebibytes: u64) -> Self {
        self.part_size = types::mebibytes(mebibytes);
        self
    }

    /// Sets the maximum number of in-flight parts, from 1 through 64.
    #[must_use]
    pub const fn concurrency(mut self, value: usize) -> Self {
        self.concurrency = value;
        self
    }

    /// Sets total attempts per part, including the first request, from 1 through 10.
    #[must_use]
    pub const fn max_attempts(mut self, value: u8) -> Self {
        self.max_attempts = value;
        self
    }

    /// Sets the maximum memory used by in-flight part buffers.
    ///
    /// Validation uses the conservative upper bound `part_size * concurrency`
    /// and runs before opening the source file or contacting R2. The default is
    /// 256 MiB.
    #[must_use]
    pub const fn max_buffered_bytes(mut self, value: u64) -> Self {
        self.max_buffered_bytes = value;
        self
    }

    /// Sets the maximum memory used by in-flight part buffers in MiB.
    #[must_use]
    pub const fn max_buffered_mib(mut self, mebibytes: u64) -> Self {
        self.max_buffered_bytes = types::mebibytes(mebibytes);
        self
    }

    /// Chooses whether an error triggers a best-effort remote abort. The default is `true`.
    #[must_use]
    pub const fn abort_on_error(mut self, value: bool) -> Self {
        self.abort_on_error = value;
        self
    }

    /// Registers a fast, non-blocking callback invoked after progress changes.
    #[must_use]
    pub fn on_progress(
        mut self,
        callback: impl Fn(ManagedUploadProgress) + Send + Sync + 'static,
    ) -> Self {
        self.progress = Some(Arc::new(callback));
        self
    }

    /// Registers an explicit cooperative cancellation signal.
    ///
    /// With the default `abort_on_error(true)`, cancellation waits for a
    /// best-effort remote abort before returning [`Error::Cancelled`]. If abort
    /// fails, the returned error carries a snapshot for later cleanup or resume.
    #[must_use]
    pub fn cancellation_token(mut self, cancellation: ManagedUploadCancellation) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    /// Uploads an asynchronous stream of bytes.
    pub async fn upload_stream(
        self,
        reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
        file_size: u64,
    ) -> Result<ManagedUploadResult, ManagedUploadError> {
        self.validate().map_err(ManagedUploadError::before_start)?;
        observability::managed_upload("start", self.part_size, self.concurrency, self.max_attempts);
        if self
            .cancellation
            .as_ref()
            .is_some_and(ManagedUploadCancellation::is_cancelled)
        {
            return Err(ManagedUploadError::before_start(Error::Cancelled));
        }

        if self.resume.is_some() {
            return Err(ManagedUploadError::before_start(Error::InvalidInput {
                field: "resume",
                reason: "streaming uploads cannot resume an existing multipart session; use upload_file for resumable transfers",
            }));
        }

        let session = self
            .bucket
            .presigned_multipart(&self.key)
            .map_err(ManagedUploadError::before_start)?
            .file_size(file_size)
            .part_size(self.part_size)
            .upload_options(self.options.clone())
            .create()
            .await
            .map_err(ManagedUploadError::before_start)?;

        let run = self.run_stream(reader, &session);
        let result = match self.cancellation.as_ref() {
            Some(cancellation) => {
                let run = std::pin::pin!(run);
                let cancelled = std::pin::pin!(cancellation.cancelled());

                match select(run, cancelled).await {
                    Either::Left((result, _)) => result,
                    Either::Right(((), _)) => Err(Error::Cancelled),
                }
            }
            None => run.await,
        };

        match result {
            Ok(result) => {
                observability::managed_upload(
                    "complete",
                    self.part_size,
                    self.concurrency,
                    self.max_attempts,
                );
                Ok(result)
            }
            Err(error) => Err(self.after_start_error(error, &session).await),
        }
    }

    async fn run_stream<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
        &self,
        mut reader: R,
        session: &PresignedMultipart,
    ) -> Result<ManagedUploadResult, Error> {
        let max_parts = (self.max_buffered_bytes / self.part_size) as usize;
        let channel_size = max_parts.saturating_sub(self.concurrency).max(1);
        let (tx, rx) = tokio::sync::mpsc::channel::<(PartNumber, Vec<u8>)>(channel_size);

        let part_size = session.snapshot().part_size();
        let file_size = session.snapshot().file_size();
        let mut reader_set = tokio::task::JoinSet::new();
        reader_set.spawn(async move {
            let mut remaining = file_size;
            let mut number = 1;
            while remaining > 0 {
                let current_part_size = remaining.min(part_size);
                let buf_len =
                    usize::try_from(current_part_size).map_err(|_| Error::InvalidInput {
                        field: "part_size",
                        reason: "part size exceeds platform addressable memory",
                    })?;
                let mut buf = vec![0; buf_len];
                if reader.read_exact(&mut buf).await.is_err() {
                    return Err(Error::Io {
                        operation: "read_exact",
                    });
                }
                let part_number =
                    PartNumber::try_from(number).map_err(|_| Error::InvalidInput {
                        field: "part_number",
                        reason: "exceeded maximum parts",
                    })?;
                if tx.send((part_number, buf)).await.is_err() {
                    break;
                }
                remaining -= current_part_size;
                number += 1;
            }
            Ok::<(), Error>(())
        });

        let client = self.bucket.client.as_sdk().clone();
        let bucket = session.snapshot().bucket().to_owned();
        let key = session.snapshot().key().to_owned();
        let upload_id = session.snapshot().expose_upload_id().to_owned();
        let max_attempts = self.max_attempts;
        let concurrency = self.concurrency;
        let progress = self.progress.clone();
        let total_parts = session.part_count();
        let total_bytes = session.snapshot().file_size();
        let completed_parts = Arc::new(AtomicU16::new(0));
        let transferred_bytes = Arc::new(AtomicU64::new(0));

        let uploaded = stream::unfold(rx, |mut rx| async move {
            let item = rx.recv().await;
            item.map(|val| (val, rx))
        })
        .map(|(number, bytes)| {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let upload_id = upload_id.clone();
            let completed_parts = Arc::clone(&completed_parts);
            let transferred_bytes = Arc::clone(&transferred_bytes);
            let progress = progress.clone();
            async move {
                let length = bytes.len() as u64;
                let uploaded = upload_part_with_retry(
                    &client,
                    &bucket,
                    &key,
                    &upload_id,
                    number,
                    bytes,
                    max_attempts,
                )
                .await?;
                let parts = completed_parts.fetch_add(1, Ordering::Relaxed) + 1;
                let bytes_transferred =
                    transferred_bytes.fetch_add(length, Ordering::Relaxed) + length;
                if let Some(callback) = progress.as_ref() {
                    callback(ManagedUploadProgress {
                        completed_parts: parts,
                        total_parts,
                        transferred_bytes: bytes_transferred,
                        total_bytes,
                    });
                }
                Ok::<UploadedPart, Error>(uploaded)
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;

        reader_set
            .join_next()
            .await
            .expect("reader_set should contain exactly one task")
            .map_err(|_| Error::Io {
                operation: "reader_task_panic",
            })??;

        let mut manifest = BTreeMap::new();
        let uploaded_count = uploaded.len() as u16;
        for part in uploaded {
            manifest.insert(part.part_number(), part);
        }

        let completion = CompletionManifest::try_from_parts(manifest.into_values())?;
        let object = session.complete(completion).await?;
        Ok(ManagedUploadResult {
            object,
            file_size: total_bytes,
            part_count: total_parts,
            uploaded_parts: uploaded_count,
            reused_parts: 0,
        })
    }

    /// Uploads the local file, reusing validated remote parts when resuming.
    pub async fn upload_file(
        self,
        path: impl AsRef<Path>,
    ) -> Result<ManagedUploadResult, ManagedUploadError> {
        self.validate().map_err(ManagedUploadError::before_start)?;
        observability::managed_upload("start", self.part_size, self.concurrency, self.max_attempts);
        let path = path.as_ref().to_owned();
        let metadata = tokio::fs::metadata(&path).await.map_err(|_| {
            ManagedUploadError::before_start(Error::Io {
                operation: "metadata",
            })
        })?;
        if !metadata.is_file() {
            return Err(ManagedUploadError::before_start(Error::InvalidInput {
                field: "path",
                reason: "must identify a regular file",
            }));
        }
        if self
            .cancellation
            .as_ref()
            .is_some_and(ManagedUploadCancellation::is_cancelled)
        {
            return Err(ManagedUploadError::before_start(Error::Cancelled));
        }
        let file_size = metadata.len();
        let source_fingerprint = SourceFileFingerprint::from_metadata(&metadata);

        let session = match self.resume.clone() {
            Some(snapshot) => {
                if snapshot.file_size() != file_size || snapshot.part_size() != self.part_size {
                    return Err(ManagedUploadError::before_start(Error::InvalidInput {
                        field: "path",
                        reason: "file size or part size differs from the saved session",
                    }));
                }
                self.bucket
                    .resume_presigned_multipart(snapshot)
                    .map_err(ManagedUploadError::before_start)?
            }
            None => self
                .bucket
                .presigned_multipart(&self.key)
                .map_err(ManagedUploadError::before_start)?
                .file_size(file_size)
                .part_size(self.part_size)
                .upload_options(self.options.clone())
                .create()
                .await
                .map_err(ManagedUploadError::before_start)?,
        };

        let run = self.run(&path, &source_fingerprint, &session);
        let result = match self.cancellation.as_ref() {
            Some(cancellation) => {
                let run = std::pin::pin!(run);
                let cancelled = std::pin::pin!(cancellation.cancelled());

                match select(run, cancelled).await {
                    Either::Left((result, _)) => result,
                    Either::Right(((), _)) => Err(Error::Cancelled),
                }
            }
            None => run.await,
        };

        match result {
            Ok(result) => {
                observability::managed_upload(
                    "complete",
                    self.part_size,
                    self.concurrency,
                    self.max_attempts,
                );
                Ok(result)
            }
            Err(error) => Err(self.after_start_error(error, &session).await),
        }
    }

    fn validate(&self) -> Result<(), Error> {
        types::validate_part_size(self.part_size)?;
        if self.concurrency == 0 || self.concurrency > MAX_CONCURRENCY {
            return Err(ValidationError::ConcurrencyOutOfRange {
                provided: self.concurrency,
                min: 1,
                max: MAX_CONCURRENCY,
            }
            .into());
        }
        if self.max_attempts == 0 || self.max_attempts > MAX_ATTEMPTS {
            return Err(ValidationError::AttemptsOutOfRange {
                provided: self.max_attempts,
                min: 1,
                max: MAX_ATTEMPTS,
            }
            .into());
        }
        let required = required_buffer_bytes(self.part_size, self.concurrency);
        if required > self.max_buffered_bytes {
            return Err(ValidationError::ManagedMemoryBudgetExceeded {
                required,
                max: self.max_buffered_bytes,
            }
            .into());
        }
        self.options.validate()?;
        if self.options.checksum().is_some() {
            return Err(Error::InvalidInput {
                field: "checksum",
                reason: "checksum verification is not supported on managed multipart uploads",
            });
        }
        if self.options.if_match().is_some() || self.options.if_none_match().is_some() {
            return Err(Error::InvalidInput {
                field: "if_match",
                reason: "conditional match headers are not supported on managed multipart uploads",
            });
        }
        if self.resume.is_some() && !self.options.is_empty() {
            return Err(Error::InvalidInput {
                field: "upload_options",
                reason: "cannot change metadata while resuming a multipart session",
            });
        }
        Ok(())
    }

    async fn run(
        &self,
        path: &Path,
        source_fingerprint: &SourceFileFingerprint,
        session: &PresignedMultipart,
    ) -> Result<ManagedUploadResult, Error> {
        let existing = if self.resume.is_some() {
            list_uploaded_parts(session).await?
        } else {
            Vec::new()
        };
        let mut manifest = BTreeMap::new();
        let mut reused_bytes = 0_u64;
        for part in existing {
            let expected = session.part_length(part.part_number())?;
            reused_bytes = reused_bytes.checked_add(expected).ok_or(Error::Service {
                operation: "ListParts",
            })?;
            manifest.insert(part.part_number(), part);
        }

        let total_parts = session.part_count();
        let completed_parts = Arc::new(AtomicU16::new(manifest.len() as u16));
        let transferred_bytes = Arc::new(AtomicU64::new(reused_bytes));
        self.report_progress(
            completed_parts.load(Ordering::Relaxed),
            total_parts,
            transferred_bytes.load(Ordering::Relaxed),
            session.snapshot().file_size(),
        );

        let existing_numbers: BTreeSet<PartNumber> = manifest.keys().copied().collect();
        let missing: Vec<(PartNumber, u64)> = (1..=total_parts)
            .map(PartNumber::try_from)
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            .filter(|part| !existing_numbers.contains(part))
            .map(|part| {
                let length = session.part_length(part)?;
                Ok((part, length))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let snapshot = session.snapshot();
        let client = self.bucket.client.as_sdk().clone();
        let concurrency = self.concurrency;
        let max_attempts = self.max_attempts;
        let total_bytes = snapshot.file_size();
        let progress = self.progress.clone();
        let path = path.to_owned();
        let part_size = snapshot.part_size();
        let bucket = snapshot.bucket().to_owned();
        let key = snapshot.key().to_owned();
        let upload_id = snapshot.expose_upload_id().to_owned();

        let uploaded = stream::iter(missing.into_iter().map(|(number, length)| {
            let client = client.clone();
            let path = path.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let upload_id = upload_id.clone();
            let completed_parts = Arc::clone(&completed_parts);
            let transferred_bytes = Arc::clone(&transferred_bytes);
            let progress = progress.clone();
            async move {
                let bytes = read_part(&path, part_size, number, length).await?;
                let uploaded = upload_part_with_retry(
                    &client,
                    &bucket,
                    &key,
                    &upload_id,
                    number,
                    bytes,
                    max_attempts,
                )
                .await?;
                let parts = completed_parts.fetch_add(1, Ordering::Relaxed) + 1;
                let bytes = transferred_bytes.fetch_add(length, Ordering::Relaxed) + length;
                if let Some(callback) = progress.as_ref() {
                    callback(ManagedUploadProgress {
                        completed_parts: parts,
                        total_parts,
                        transferred_bytes: bytes,
                        total_bytes,
                    });
                }
                Ok::<UploadedPart, Error>(uploaded)
            }
        }))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;

        let mut uploaded_count = 0_u16;
        for part in uploaded {
            uploaded_count += 1;
            manifest.insert(part.part_number(), part);
        }

        let final_metadata = tokio::fs::metadata(path).await.map_err(|_| Error::Io {
            operation: "metadata",
        })?;
        if !source_fingerprint.matches(&final_metadata) {
            return Err(Error::InvalidInput {
                field: "path",
                reason: "file changed during upload",
            });
        }

        let completion = CompletionManifest::try_from_parts(manifest.into_values())?;
        let object = session.complete(completion).await?;
        Ok(ManagedUploadResult {
            object,
            file_size: total_bytes,
            part_count: total_parts,
            uploaded_parts: uploaded_count,
            reused_parts: existing_numbers.len() as u16,
        })
    }

    fn report_progress(
        &self,
        completed_parts: u16,
        total_parts: u16,
        transferred_bytes: u64,
        total_bytes: u64,
    ) {
        if let Some(callback) = self.progress.as_ref() {
            callback(ManagedUploadProgress {
                completed_parts,
                total_parts,
                transferred_bytes,
                total_bytes,
            });
        }
    }

    async fn after_start_error(
        &self,
        error: Error,
        session: &PresignedMultipart,
    ) -> ManagedUploadError {
        let snapshot = session.snapshot();
        if self.abort_on_error && session.abort().await.is_ok() {
            ManagedUploadError {
                error,
                snapshot: None,
                aborted: true,
            }
        } else {
            ManagedUploadError {
                error,
                snapshot: Some(Box::new(snapshot)),
                aborted: false,
            }
        }
    }
}

impl ManagedUploadError {
    fn before_start(error: Error) -> Self {
        Self {
            error,
            snapshot: None,
            aborted: false,
        }
    }
}

impl Bucket {
    /// Starts a new managed multipart upload for a local file.
    pub fn managed_multipart(
        &self,
        key: impl IntoObjectKey,
    ) -> Result<ManagedMultipartBuilder, Error> {
        let key = key.into_object_key()?;
        Ok(ManagedMultipartBuilder {
            bucket: self.clone(),
            key: key.into_inner(),
            part_size: DEFAULT_PART_SIZE,
            concurrency: DEFAULT_CONCURRENCY,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
            abort_on_error: true,
            resume: None,
            progress: None,
            cancellation: None,
            options: ObjectUploadOptions::default(),
        })
    }

    /// Resumes a managed upload from a saved multipart session snapshot.
    pub fn resume_managed_multipart(
        &self,
        snapshot: MultipartSessionSnapshot,
    ) -> Result<ManagedMultipartBuilder, Error> {
        self.resume_presigned_multipart(snapshot.clone())?;
        Ok(ManagedMultipartBuilder {
            bucket: self.clone(),
            key: snapshot.key().to_owned(),
            part_size: snapshot.part_size(),
            concurrency: DEFAULT_CONCURRENCY,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
            abort_on_error: true,
            resume: Some(snapshot),
            progress: None,
            cancellation: None,
            options: ObjectUploadOptions::default(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceFileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl SourceFileFingerprint {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
        }
    }

    fn matches(&self, metadata: &Metadata) -> bool {
        self == &Self::from_metadata(metadata)
    }
}

async fn read_part(
    path: &Path,
    part_size: u64,
    number: PartNumber,
    length: u64,
) -> Result<Vec<u8>, Error> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|_| Error::Io { operation: "open" })?;
    let offset = u64::from(number.get() - 1)
        .checked_mul(part_size)
        .ok_or(Error::InvalidInput {
            field: "part_number",
            reason: "part offset overflowed",
        })?;
    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|_| Error::Io { operation: "seek" })?;
    let capacity = usize::try_from(length).map_err(|_| Error::InvalidInput {
        field: "part_size",
        reason: "does not fit this platform's address space",
    })?;
    let mut bytes = vec![0; capacity];
    file.read_exact(&mut bytes)
        .await
        .map_err(|_| Error::Io { operation: "read" })?;
    Ok(bytes)
}

async fn upload_part_with_retry(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    number: PartNumber,
    bytes: Vec<u8>,
    max_attempts: u8,
) -> Result<UploadedPart, Error> {
    let bytes = bytes::Bytes::from(bytes);
    let output = retry_with_policy(
        max_attempts,
        number,
        || {
            client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(i32::from(number.get()))
                .content_length(
                    i64::try_from(bytes.len())
                        .expect("validated multipart part length fits in i64"),
                )
                .body(ByteStream::from(bytes.clone()))
                .customize()
                .config_override(
                    aws_sdk_s3::Config::builder().retry_config(RetryConfig::disabled()),
                )
                .send()
        },
        is_retryable,
        sdk_retry_after,
        tokio::time::sleep,
    )
    .await
    .map_err(|error| Error::remote("UploadPart", &error))?;
    let etag = output.e_tag().ok_or(Error::Service {
        operation: "UploadPart",
    })?;
    UploadedPart::new(number, etag)
}

async fn retry_with_policy<
    T,
    E,
    Operation,
    OperationFuture,
    Classify,
    RetryAfter,
    Sleep,
    SleepFuture,
>(
    max_attempts: u8,
    number: PartNumber,
    mut operation: Operation,
    classify: Classify,
    retry_after: RetryAfter,
    mut sleep: Sleep,
) -> Result<T, E>
where
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, E>>,
    Classify: Fn(&E) -> bool,
    RetryAfter: Fn(&E) -> Option<Duration>,
    Sleep: FnMut(Duration) -> SleepFuture,
    SleepFuture: Future<Output = ()>,
{
    let mut attempt = 1_u8;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < max_attempts && classify(&error) => {
                let delay = retry_delay(attempt, retry_after(&error), fastrand::u64(..));
                observability::upload_part_retry(
                    number.get(),
                    attempt,
                    max_attempts,
                    delay.as_millis().min(u128::from(u64::MAX)) as u64,
                );
                attempt += 1;
                sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_retryable<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool {
    use aws_sdk_s3::error::SdkError;

    match error {
        SdkError::ConstructionFailure(_) => false,
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            true
        }
        SdkError::ServiceError(_) => error
            .raw_response()
            .is_some_and(|response| is_retryable_status(response.status().as_u16())),
        _ => false,
    }
}

fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..=599).contains(&status)
}

fn sdk_retry_after<E>(error: &aws_sdk_s3::error::SdkError<E>) -> Option<Duration> {
    let headers = error.raw_response()?.headers();
    parse_retry_after(headers.get("retry-after"), headers.get("x-amz-retry-after"))
}

fn parse_retry_after(retry_after: Option<&str>, aws_retry_after: Option<&str>) -> Option<Duration> {
    retry_after
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .or_else(|| {
            aws_retry_after
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_millis)
        })
}

fn retry_ceiling(attempt: u8) -> Duration {
    let exponent = u32::from(attempt.saturating_sub(1).min(7));
    Duration::from_millis(250_u64 * (1_u64 << exponent)).min(MAX_RETRY_DELAY)
}

fn required_buffer_bytes(part_size: u64, concurrency: usize) -> u64 {
    part_size.saturating_mul(concurrency as u64)
}

fn retry_delay(attempt: u8, retry_after: Option<Duration>, random: u64) -> Duration {
    let ceiling = retry_ceiling(attempt);
    let ceiling_millis = ceiling.as_millis() as u64;
    let jitter = Duration::from_millis(random % (ceiling_millis + 1));
    jitter.max(retry_after.unwrap_or_default().min(MAX_RETRY_DELAY))
}

async fn list_uploaded_parts(session: &PresignedMultipart) -> Result<Vec<UploadedPart>, Error> {
    Ok(session.reconcile().await?.into_uploaded_parts())
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, fs::OpenOptions, future::ready};

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum InjectedFailure {
        NoResponse,
        Status(u16),
    }

    fn injected_retryable(error: &InjectedFailure) -> bool {
        match error {
            InjectedFailure::NoResponse => true,
            InjectedFailure::Status(status) => is_retryable_status(*status),
        }
    }

    fn run_async(future: impl Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future);
    }

    #[test]
    fn retry_uses_full_jitter_with_an_exponential_ceiling() {
        assert_eq!(retry_ceiling(1), Duration::from_millis(250));
        assert_eq!(retry_ceiling(2), Duration::from_millis(500));
        assert_eq!(retry_ceiling(7), Duration::from_secs(16));
        assert_eq!(retry_ceiling(8), MAX_RETRY_DELAY);
        assert_eq!(retry_ceiling(10), MAX_RETRY_DELAY);

        assert_eq!(retry_delay(1, None, 0), Duration::ZERO);
        assert_eq!(
            retry_delay(1, None, u64::MAX),
            Duration::from_millis(u64::MAX % 251)
        );
        assert!(retry_delay(10, None, u64::MAX) <= MAX_RETRY_DELAY);
    }

    #[test]
    fn retry_after_is_respected_and_bounded() {
        assert_eq!(
            parse_retry_after(Some("12"), None),
            Some(Duration::from_secs(12))
        );
        assert_eq!(
            parse_retry_after(None, Some("1500")),
            Some(Duration::from_millis(1_500))
        );
        assert_eq!(
            parse_retry_after(Some("invalid"), Some("also-invalid")),
            None
        );
        assert_eq!(
            retry_delay(1, Some(Duration::from_secs(10)), 0),
            Duration::from_secs(10)
        );
        assert_eq!(
            retry_delay(1, Some(Duration::from_secs(60)), 0),
            MAX_RETRY_DELAY
        );
    }

    #[test]
    fn buffered_byte_calculation_saturates_on_overflow() {
        assert_eq!(required_buffer_bytes(8 * 1024 * 1024, 4), 32 * 1024 * 1024);
        assert_eq!(required_buffer_bytes(u64::MAX, 2), u64::MAX);
    }

    #[test]
    fn source_fingerprint_accepts_unchanged_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.bin");
        std::fs::write(&path, b"unchanged").unwrap();
        let fingerprint = SourceFileFingerprint::from_metadata(&std::fs::metadata(&path).unwrap());

        assert!(fingerprint.matches(&std::fs::metadata(path).unwrap()));
    }

    #[test]
    fn source_fingerprint_detects_same_size_mtime_change() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.bin");
        std::fs::write(&path, b"before").unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        let fingerprint = SourceFileFingerprint::from_metadata(&metadata);
        let changed_time = metadata.modified().unwrap() + Duration::from_secs(60);
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(changed_time)
            .unwrap();

        assert!(!fingerprint.matches(&std::fs::metadata(path).unwrap()));
    }

    #[cfg(unix)]
    #[test]
    fn source_fingerprint_detects_same_size_file_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.bin");
        std::fs::write(&path, b"before").unwrap();
        let fingerprint = SourceFileFingerprint::from_metadata(&std::fs::metadata(&path).unwrap());
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"after!").unwrap();

        assert!(!fingerprint.matches(&std::fs::metadata(path).unwrap()));
    }

    #[test]
    fn retry_status_matrix_is_deliberately_bounded() {
        for retryable in [408, 429, 500, 502, 503, 599] {
            assert!(injected_retryable(&InjectedFailure::Status(retryable)));
        }
        for terminal in [400, 401, 403, 404, 409, 499, 600] {
            assert!(!injected_retryable(&InjectedFailure::Status(terminal)));
        }
        assert!(injected_retryable(&InjectedFailure::NoResponse));
    }

    #[test]
    fn sdk_construction_failures_are_terminal_but_timeouts_retry() {
        let construction: aws_sdk_s3::error::SdkError<std::io::Error> =
            aws_sdk_s3::error::SdkError::construction_failure(std::io::Error::other("injected"));
        let timeout: aws_sdk_s3::error::SdkError<std::io::Error> =
            aws_sdk_s3::error::SdkError::timeout_error(std::io::Error::other("injected"));

        assert!(!is_retryable(&construction));
        assert!(is_retryable(&timeout));
    }

    #[test]
    fn retry_runner_reaches_success_after_transient_faults() {
        run_async(async {
            let mut outcomes = VecDeque::from([
                Err(InjectedFailure::Status(408)),
                Err(InjectedFailure::Status(429)),
                Err(InjectedFailure::Status(503)),
                Ok("etag"),
            ]);
            let mut attempts = 0_u8;
            let mut delays = Vec::new();
            let result = retry_with_policy(
                4,
                PartNumber::try_from(3).unwrap(),
                || {
                    attempts += 1;
                    ready(outcomes.pop_front().unwrap())
                },
                injected_retryable,
                |_| None,
                |delay| {
                    delays.push(delay);
                    ready(())
                },
            )
            .await;

            assert_eq!(result, Ok("etag"));
            assert_eq!(attempts, 4);
            assert_eq!(delays.len(), 3);
            assert!(
                delays
                    .iter()
                    .enumerate()
                    .all(|(index, delay)| *delay <= retry_ceiling(index as u8 + 1))
            );
        });
    }

    #[test]
    fn retry_runner_stops_on_terminal_fault() {
        run_async(async {
            let mut attempts = 0_u8;
            let result = retry_with_policy(
                10,
                PartNumber::try_from(1).unwrap(),
                || {
                    attempts += 1;
                    ready(Err::<(), _>(InjectedFailure::Status(403)))
                },
                injected_retryable,
                |_| None,
                |_| ready(()),
            )
            .await;

            assert_eq!(result, Err(InjectedFailure::Status(403)));
            assert_eq!(attempts, 1);
        });
    }

    #[test]
    fn retry_runner_honors_the_exact_attempt_limit() {
        run_async(async {
            let mut attempts = 0_u8;
            let mut delays = 0_u8;
            let result = retry_with_policy(
                3,
                PartNumber::try_from(1).unwrap(),
                || {
                    attempts += 1;
                    ready(Err::<(), _>(InjectedFailure::NoResponse))
                },
                injected_retryable,
                |_| None,
                |_| {
                    delays += 1;
                    ready(())
                },
            )
            .await;

            assert_eq!(result, Err(InjectedFailure::NoResponse));
            assert_eq!(attempts, 3);
            assert_eq!(delays, 2);
        });
    }

    #[test]
    fn cancellation_wakes_all_waiters_and_is_idempotent() {
        run_async(async {
            let cancellation = ManagedUploadCancellation::new();
            let first = cancellation.clone();
            let second = cancellation.clone();
            let first_waiter = tokio::spawn(async move { first.cancelled().await });
            let second_waiter = tokio::spawn(async move { second.cancelled().await });

            assert!(!cancellation.is_cancelled());
            cancellation.cancel();
            cancellation.cancel();
            first_waiter.await.unwrap();
            second_waiter.await.unwrap();
            assert!(cancellation.is_cancelled());
            cancellation.cancelled().await;
        });
    }

    #[test]
    fn progress_reports_fraction_and_percentage() {
        let progress = ManagedUploadProgress {
            completed_parts: 1,
            total_parts: 4,
            transferred_bytes: 25,
            total_bytes: 100,
        };

        assert_eq!(progress.fraction(), 0.25);
        assert_eq!(progress.percentage(), 25.0);
    }

    #[test]
    fn progress_helpers_defensively_handle_zero_total_bytes() {
        let progress = ManagedUploadProgress {
            completed_parts: 0,
            total_parts: 0,
            transferred_bytes: 0,
            total_bytes: 0,
        };

        assert_eq!(progress.fraction(), 0.0);
        assert_eq!(progress.percentage(), 0.0);
    }
}
