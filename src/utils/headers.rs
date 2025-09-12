use axum::http::{HeaderMap, HeaderValue};

// Build common headers for file responses.
// Caller sets size to bytes in body (full size for GET, length for 206, total for HEAD).
pub fn file_headers_common(revision: &str, size: u64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Length",
        HeaderValue::from_str(&size.to_string()).unwrap(),
    );
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert("Accept-Ranges", HeaderValue::from_static("bytes"));
    headers.insert(
        "x-repo-commit",
        HeaderValue::from_str(revision).unwrap_or(HeaderValue::from_static("-")),
    );
    headers.insert(
        "x-revision",
        HeaderValue::from_str(revision).unwrap_or(HeaderValue::from_static("-")),
    );
    headers
}

pub fn set_content_range(headers: &mut HeaderMap, start: u64, end: u64, total: u64) {
    headers.insert(
        "Content-Range",
        HeaderValue::from_str(&format!("bytes {start}-{end}/{total}")).unwrap(),
    );
}

