use std::{io, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};
use tracing::warn;

use crate::status::Metrics;

const MAX_CONCURRENT_CONNECTIONS: usize = 32;
const MAX_REQUEST_BYTES: usize = 8 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn serve(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    filters_endpoint_enabled: bool,
) -> Result<()> {
    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));

    loop {
        let permit = Arc::clone(&permits)
            .acquire_owned()
            .await
            .context("HTTP status connection semaphore closed")?;
        let (stream, peer) = listener
            .accept()
            .await
            .context("failed to accept HTTP status connection")?;
        let connection_metrics = Arc::clone(&metrics);
        let connection_filters_enabled = filters_endpoint_enabled;

        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) =
                handle_connection(stream, connection_metrics, connection_filters_enabled).await
            {
                warn!(peer = %peer, error = %error, "http_status_connection_failed");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    metrics: Arc<Metrics>,
    filters_endpoint_enabled: bool,
) -> Result<()> {
    let response = match timeout(REQUEST_TIMEOUT, read_request(&mut stream)).await {
        Ok(Ok(request)) => route_request(
            &request.method,
            &request.target,
            &metrics,
            filters_endpoint_enabled,
        ),
        Ok(Err(RequestReadError::TooLarge)) => HttpResponse::text(
            431,
            "Request Header Fields Too Large",
            "Request headers are too large.\n",
        ),
        Ok(Err(RequestReadError::BadRequest)) => {
            HttpResponse::text(400, "Bad Request", "The request is invalid.\n")
        }
        Ok(Err(RequestReadError::Io(error))) => {
            return Err(error).context("failed to read HTTP status request");
        }
        Err(_) => HttpResponse::text(408, "Request Timeout", "The request timed out.\n"),
    };

    timeout(RESPONSE_TIMEOUT, write_response(&mut stream, &response))
        .await
        .context("timed out writing HTTP status response")?
        .context("failed to write HTTP status response")
}

#[derive(Debug)]
struct ParsedRequest {
    method: String,
    target: String,
}

enum RequestReadError {
    BadRequest,
    TooLarge,
    Io(io::Error),
}

async fn read_request(
    stream: &mut TcpStream,
) -> std::result::Result<ParsedRequest, RequestReadError> {
    let mut buffer = [0_u8; MAX_REQUEST_BYTES];
    let mut length = 0;

    loop {
        if let Some(header_end) = find_header_end(&buffer[..length]) {
            return parse_request(&buffer[..header_end]);
        }
        if length == buffer.len() {
            return Err(RequestReadError::TooLarge);
        }

        stream.readable().await.map_err(RequestReadError::Io)?;
        match stream.try_read(&mut buffer[length..]) {
            Ok(0) => return Err(RequestReadError::BadRequest),
            Ok(bytes_read) => length += bytes_read,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(RequestReadError::Io(error)),
        }
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn parse_request(bytes: &[u8]) -> std::result::Result<ParsedRequest, RequestReadError> {
    let text = std::str::from_utf8(bytes).map_err(|_| RequestReadError::BadRequest)?;
    let text = text
        .strip_suffix("\r\n\r\n")
        .ok_or(RequestReadError::BadRequest)?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().ok_or(RequestReadError::BadRequest)?;
    let mut parts = request_line.split_ascii_whitespace();
    let method = parts.next().ok_or(RequestReadError::BadRequest)?;
    let target = parts.next().ok_or(RequestReadError::BadRequest)?;
    let version = parts.next().ok_or(RequestReadError::BadRequest)?;
    if parts.next().is_some() || target.is_empty() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(RequestReadError::BadRequest);
    }

    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            return Err(RequestReadError::BadRequest);
        };
        if name.is_empty()
            || !name.bytes().all(is_header_name_byte)
            || value
                .bytes()
                .any(|byte| (byte < b' ' && byte != b'\t') || byte == 0x7f)
        {
            return Err(RequestReadError::BadRequest);
        }
    }

    Ok(ParsedRequest {
        method: method.to_owned(),
        target: target.to_owned(),
    })
}

fn is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

struct HttpResponse {
    status: u16,
    reason: &'static str,
    content_type: &'static str,
    body: Vec<u8>,
    allow_get: bool,
}

impl HttpResponse {
    fn text(status: u16, reason: &'static str, body: &str) -> Self {
        Self {
            status,
            reason,
            content_type: "text/plain; charset=utf-8",
            body: body.as_bytes().to_vec(),
            allow_get: false,
        }
    }

    fn json(status: u16, reason: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            reason,
            content_type: "application/json",
            body,
            allow_get: false,
        }
    }
}

fn route_request(
    method: &str,
    target: &str,
    metrics: &Metrics,
    filters_endpoint_enabled: bool,
) -> HttpResponse {
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    match (method, path) {
        ("GET", "/") => match serde_json::to_vec(&metrics.snapshot()) {
            Ok(body) => HttpResponse::json(200, "OK", body),
            Err(_) => HttpResponse::text(
                500,
                "Internal Server Error",
                "The status snapshot could not be serialized.\n",
            ),
        },
        ("GET", "/filters") if filters_endpoint_enabled => {
            match serde_json::to_vec(&metrics.channel_caches_snapshot()) {
                Ok(body) => HttpResponse::json(200, "OK", body),
                Err(_) => HttpResponse::text(
                    500,
                    "Internal Server Error",
                    "The channel cache snapshot could not be serialized.\n",
                ),
            }
        }
        ("GET", "/filters") => {
            HttpResponse::text(404, "Not Found", "The requested endpoint was not found.\n")
        }
        (_, "/filters") if filters_endpoint_enabled => {
            let mut response = HttpResponse::text(
                405,
                "Method Not Allowed",
                "Only GET requests are supported.\n",
            );
            response.allow_get = true;
            response
        }
        (_, "/") => {
            let mut response = HttpResponse::text(
                405,
                "Method Not Allowed",
                "Only GET requests are supported.\n",
            );
            response.allow_get = true;
            response
        }
        _ => HttpResponse::text(404, "Not Found", "The requested endpoint was not found.\n"),
    }
}

async fn write_response(stream: &mut TcpStream, response: &HttpResponse) -> io::Result<()> {
    let allow_header = if response.allow_get {
        "Allow: GET\r\n"
    } else {
        ""
    };
    let headers = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n{}\r\n",
        response.status,
        response.reason,
        response.content_type,
        response.body.len(),
        allow_header,
    );
    let mut bytes = headers.into_bytes();
    bytes.extend_from_slice(&response.body);
    let mut written = 0;

    while written < bytes.len() {
        stream.writable().await?;
        match stream.try_write(&bytes[written..]) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "socket closed")),
            Ok(bytes_written) => written += bytes_written,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/http_status.rs"]
mod tests;
