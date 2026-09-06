use std::{fmt, sync::Arc, time::SystemTime};

use aws_sdk_s3::config::{Credentials, Region};
use aws_smithy_types::{retry::RetryConfig, timeout::TimeoutConfig};

use crate::{
    Error, R2Config, observability,
    types::{BucketName, IntoBucketName},
};

/// Information about an R2 bucket returned by [`R2Client::list_buckets`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketInfo {
    name: String,
    creation_date: Option<SystemTime>,
}

impl BucketInfo {
    /// Returns the bucket name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the bucket creation date when reported.
    #[must_use]
    pub const fn creation_date(&self) -> Option<SystemTime> {
        self.creation_date
    }
}

/// A configured Cloudflare R2 client.
#[derive(Clone)]
pub struct R2Client {
    inner: aws_sdk_s3::Client,
}

impl fmt::Debug for R2Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("R2Client").finish_non_exhaustive()
    }
}

impl R2Client {
    /// Creates an R2 client from explicit configuration.
    #[must_use]
    pub fn new(config: R2Config) -> Self {
        let credentials = Credentials::new(
            config.access_key_id(),
            config.secret_access_key(),
            config.session_token().map(ToOwned::to_owned),
            None,
            "r2kit",
        );
        let mut sdk_builder = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("auto"))
            .endpoint_url(config.endpoint_url())
            .credentials_provider(credentials);

        if config.connect_timeout().is_some()
            || config.read_timeout().is_some()
            || config.operation_timeout().is_some()
            || config.operation_attempt_timeout().is_some()
        {
            let mut timeouts = TimeoutConfig::builder();
            if let Some(value) = config.connect_timeout() {
                timeouts = timeouts.connect_timeout(value);
            }
            if let Some(value) = config.read_timeout() {
                timeouts = timeouts.read_timeout(value);
            }
            if let Some(value) = config.operation_timeout() {
                timeouts = timeouts.operation_timeout(value);
            }
            if let Some(value) = config.operation_attempt_timeout() {
                timeouts = timeouts.operation_attempt_timeout(value);
            }
            sdk_builder = sdk_builder.timeout_config(timeouts.build());
        }
        if let Some(max_attempts) = config.sdk_max_attempts() {
            sdk_builder =
                sdk_builder.retry_config(RetryConfig::standard().with_max_attempts(max_attempts));
        }

        let sdk_config = sdk_builder.build();

        Self {
            inner: aws_sdk_s3::Client::from_conf(sdk_config),
        }
    }

    /// Creates an R2 client from the standard `R2_*` environment variables.
    pub fn from_env() -> Result<Self, Error> {
        Ok(Self::new(R2Config::from_env()?))
    }

    /// Wraps a preconfigured AWS S3 client.
    ///
    /// The caller is responsible for configuring an R2-compatible endpoint,
    /// the `auto` signing region, credentials, timeouts, and retries. This
    /// escape hatch cannot enforce the invariants applied by [`R2Config`].
    #[must_use]
    pub fn from_sdk(client: aws_sdk_s3::Client) -> Self {
        Self { inner: client }
    }

    /// Returns the underlying AWS S3 client for operations not wrapped by `r2kit`.
    #[must_use]
    pub fn as_sdk(&self) -> &aws_sdk_s3::Client {
        &self.inner
    }

    /// Lists all buckets owned by the authenticated account.
    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>, Error> {
        let output = self
            .inner
            .list_buckets()
            .send()
            .await
            .map_err(|error| Error::remote("ListBuckets", &error))?;

        let buckets = output
            .buckets()
            .iter()
            .map(|bucket| {
                let name = bucket.name().ok_or(Error::Service {
                    operation: "ListBuckets",
                })?;
                let creation_date = bucket
                    .creation_date()
                    .cloned()
                    .map(SystemTime::try_from)
                    .transpose()
                    .map_err(|_| Error::Service {
                        operation: "ListBuckets",
                    })?;
                Ok(BucketInfo {
                    name: name.to_owned(),
                    creation_date,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(buckets)
    }

    /// Creates a new R2 bucket with a validated bucket name.
    pub async fn create_bucket(&self, name: impl IntoBucketName) -> Result<Bucket, Error> {
        let name = name.into_bucket_name()?;

        self.inner
            .create_bucket()
            .bucket(name.as_str())
            .send()
            .await
            .map_err(|error| Error::remote("CreateBucket", &error))?;

        Ok(Bucket {
            client: Arc::new(self.clone()),
            name,
        })
    }

    /// Deletes an empty R2 bucket.
    pub async fn delete_bucket(&self, name: impl IntoBucketName) -> Result<(), Error> {
        let name = name.into_bucket_name()?;

        self.inner
            .delete_bucket()
            .bucket(name.as_str())
            .send()
            .await
            .map_err(|error| Error::remote("DeleteBucket", &error))?;

        Ok(())
    }

    /// Checks whether a bucket exists and is accessible.
    pub async fn bucket_exists(&self, name: impl IntoBucketName) -> Result<bool, Error> {
        let name = name.into_bucket_name()?;

        let result = self.inner.head_bucket().bucket(name.as_str()).send().await;
        match result {
            Ok(_) => Ok(true),
            Err(error) => {
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 404)
                {
                    Ok(false)
                } else {
                    Err(Error::remote("HeadBucket", &error))
                }
            }
        }
    }

    /// Selects and validates an R2 bucket.
    pub fn bucket(&self, name: impl IntoBucketName) -> Result<Bucket, Error> {
        let name = name.into_bucket_name()?;

        Ok(Bucket {
            client: Arc::new(self.clone()),
            name,
        })
    }

    /// Selects a bucket and verifies that the current credentials can list it.
    ///
    /// This performs one `ListObjectsV2` request with a one-object page limit.
    /// Use [`Self::bucket`] when startup network access is not desired.
    pub async fn validate_bucket(&self, name: impl IntoBucketName) -> Result<Bucket, Error> {
        let bucket = self.bucket(name)?;
        bucket.validate_access().await?;
        Ok(bucket)
    }
}

/// Operations scoped to one R2 bucket.
#[derive(Clone)]
pub struct Bucket {
    pub(crate) client: Arc<R2Client>,
    pub(crate) name: BucketName,
}

impl fmt::Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bucket")
            .field("name", &self.name.as_str())
            .finish()
    }
}

impl Bucket {
    /// Returns the bucket name as a string slice.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the typed [`BucketName`].
    #[must_use]
    pub fn bucket_name(&self) -> &BucketName {
        &self.name
    }

    /// Verifies that this bucket exists and the current credentials can list it.
    ///
    /// The check performs one read-only `ListObjectsV2` request and does not
    /// return or log object keys. It is optional and never runs implicitly.
    pub async fn validate_access(&self) -> Result<(), Error> {
        observability::preflight("start");
        self.client
            .as_sdk()
            .list_objects_v2()
            .bucket(self.name.as_str())
            .max_keys(1)
            .send()
            .await
            .map_err(|error| Error::remote("ListObjectsV2", &error))?;
        observability::preflight("complete");
        Ok(())
    }
}
