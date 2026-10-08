//! Minimal RESP2/RESP3 codec shared by the E2E proxies and drivers.
//!
//! Every frame is returned with its exact wire bytes so a proxy can forward it
//! unmodified while still inspecting the parsed value. The parser is a superset
//! of the two Python helpers it replaces: it understands the RESP3 markers in
//! addition to the RESP2 subset the `redis` crate actually negotiates.

use std::io;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt};

#[derive(Clone, Debug, PartialEq)]
pub enum RespValue {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Int(i64),
    Double(Vec<u8>),
    Bool(Vec<u8>),
    Null,
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
    Push(Vec<RespValue>),
    Map(Vec<RespValue>),
}

impl RespValue {
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Simple(bytes) | Self::Bulk(Some(bytes)) => Some(bytes),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[RespValue]> {
        match self {
            Self::Array(Some(items)) | Self::Push(items) | Self::Map(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Frame {
    pub raw: Vec<u8>,
    pub value: RespValue,
}

pub async fn read_frame<R>(reader: &mut R) -> io::Result<Frame>
where
    R: AsyncBufRead + Unpin,
{
    let mut marker = [0_u8; 1];
    reader.read_exact(&mut marker).await?;
    let line = read_line(reader).await?;
    let value_line = &line[..line.len() - 2];
    let mut raw = Vec::with_capacity(1 + line.len());
    raw.push(marker[0]);
    raw.extend_from_slice(&line);

    let value = match marker[0] {
        b'+' => RespValue::Simple(value_line.to_vec()),
        b'-' => RespValue::Error(value_line.to_vec()),
        b':' => RespValue::Int(parse_int(value_line)?),
        b',' => RespValue::Double(value_line.to_vec()),
        b'#' => RespValue::Bool(value_line.to_vec()),
        b'_' => RespValue::Null,
        b'$' | b'!' | b'=' => {
            let length = parse_int(value_line)?;
            if length == -1 {
                RespValue::Bulk(None)
            } else {
                let body = read_exact_bytes(reader, as_length(length)? + 2).await?;
                if !body.ends_with(b"\r\n") {
                    return Err(invalid_data("invalid RESP bulk terminator"));
                }
                raw.extend_from_slice(&body);
                RespValue::Bulk(Some(body[..body.len() - 2].to_vec()))
            }
        }
        b'*' | b'~' | b'>' => {
            let count = parse_int(value_line)?;
            if count == -1 {
                RespValue::Array(None)
            } else {
                let mut items = Vec::with_capacity(as_length(count)?);
                for _ in 0..count {
                    let frame = Box::pin(read_frame(reader)).await?;
                    raw.extend_from_slice(&frame.raw);
                    items.push(frame.value);
                }
                if marker[0] == b'>' {
                    RespValue::Push(items)
                } else {
                    RespValue::Array(Some(items))
                }
            }
        }
        b'%' | b'|' => {
            let count = parse_int(value_line)?;
            let mut items = Vec::with_capacity(as_length(count)? * 2);
            for _ in 0..count * 2 {
                let frame = Box::pin(read_frame(reader)).await?;
                raw.extend_from_slice(&frame.raw);
                items.push(frame.value);
            }
            if marker[0] == b'|' {
                let frame = Box::pin(read_frame(reader)).await?;
                raw.extend_from_slice(&frame.raw);
                frame.value
            } else {
                RespValue::Map(items)
            }
        }
        other => return Err(invalid_data(format!("unsupported RESP marker {other:?}"))),
    };

    Ok(Frame { raw, value })
}

/// Reads one RESP command frame, requiring an array of bulk strings.
pub async fn read_command<R>(reader: &mut R) -> io::Result<(Vec<u8>, Vec<Vec<u8>>)>
where
    R: AsyncBufRead + Unpin,
{
    let frame = read_frame(reader).await?;
    let RespValue::Array(Some(items)) = &frame.value else {
        return Err(invalid_data("expected a RESP array command"));
    };
    let mut arguments = Vec::with_capacity(items.len());
    for item in items {
        match item {
            RespValue::Bulk(Some(bytes)) => arguments.push(bytes.clone()),
            _ => return Err(invalid_data("expected RESP bulk command arguments")),
        }
    }
    Ok((frame.raw, arguments))
}

pub fn encode_command(arguments: &[&[u8]]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(format!("*{}\r\n", arguments.len()).as_bytes());
    for argument in arguments {
        frame.extend_from_slice(format!("${}\r\n", argument.len()).as_bytes());
        frame.extend_from_slice(argument);
        frame.extend_from_slice(b"\r\n");
    }
    frame
}

async fn read_line<R>(reader: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    let read = reader.read_until(b'\n', &mut line).await?;
    if read == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "socket closed during RESP line",
        ));
    }
    if !line.ends_with(b"\r\n") {
        return Err(invalid_data("invalid RESP line terminator"));
    }
    Ok(line)
}

async fn read_exact_bytes<R>(reader: &mut R, length: usize) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}

fn as_length(value: i64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_data(format!("negative RESP length {value}")))
}

fn parse_int(bytes: &[u8]) -> io::Result<i64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| invalid_data(format!("invalid RESP integer {bytes:?}")))
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
