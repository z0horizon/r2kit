use std::{env, path::PathBuf};

use r2kit::R2Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket_name = env::var("R2_BUCKET")?;
    let key = env::var("R2_KEY")?;
    let destination = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("download.bin"));

    let bucket = R2Client::from_env()?.bucket(bucket_name)?;
    let metadata = bucket.download_file(key, &destination).await?;

    println!(
        "downloaded {} bytes to {}",
        metadata.size(),
        destination.display()
    );
    Ok(())
}
