use std::{collections::HashMap, env, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::post,
};
use r2kit::Bucket;
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = r2kit::R2Client::from_env()?.bucket(env::var("R2_BUCKET")?)?;
    let app = Router::new()
        .route("/uploads/{*key}", post(create_upload))
        .with_state(bucket);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn create_upload(
    State(bucket): State<Bucket>,
    Path(key): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    // Authenticate and authorize the caller for `key` before signing in a real app.
    let content_length = query
        .get("content_length")
        .ok_or((StatusCode::BAD_REQUEST, "content_length is required".into()))?
        .parse::<u64>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid content_length".into()))?;
    let upload = bucket
        .presign_put(key, content_length, Duration::from_secs(10 * 60))
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let (method, url, headers) = upload.into_request().into_exposed_parts();

    Ok(Json(json!({
        "method": method,
        "url": url,
        "headers": headers,
        "content_length": content_length,
    })))
}
