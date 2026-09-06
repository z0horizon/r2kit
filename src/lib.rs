#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod client;
mod config;
mod error;
mod managed;
mod multipart;
mod object;
mod observability;
mod types;

pub use types::{BucketName, IntoBucketName, IntoObjectKey, ObjectKey};

pub use client::{Bucket, BucketInfo, R2Client};
pub use config::{R2Config, R2ConfigBuilder, R2Jurisdiction};
pub use error::{ConfigError, Error, ServiceError, ServiceErrorKind, ValidationError};
pub use headers::CacheControl;
pub use managed::{
    ManagedMultipartBuilder, ManagedUploadCancellation, ManagedUploadError, ManagedUploadProgress,
    ManagedUploadResult,
};
pub use mime::{self, Mime};
pub use multipart::{
    CompletedObject, CompletionManifest, ListMultipartUploadsBuilder, MultipartPartReceipt,
    MultipartReconciliation, MultipartSessionRecord, MultipartSessionSnapshot, MultipartUploadPage,
    MultipartUploadPartRequest, MultipartUploadSummary, PartMd5, PartNumber, PresignedMultipart,
    PresignedMultipartBuilder, PresignedRequest, PresignedUploadPart, SecretUrl, UploadedPart,
};
pub use object::{
    BatchDeleteError, ByteRange, ChecksumAlgorithm, CopyObjectBuilder, CopyObjectResult,
    DeleteObjectFailure, DeleteObjectsResult, DownloadedObject, GetObjectBuilder,
    HeadObjectBuilder, ListObjectsBuilder, MetadataDirective, ObjectMetadata, ObjectPage,
    ObjectSummary, ObjectUploadOptions, ObjectUploadOptionsBuilder, PresignedPutObject,
    PutObjectResult,
};
