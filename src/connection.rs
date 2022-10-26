use crate::frame::{
    self, Frame, ParseFrameError, ARRAY_IDENT, BOOLEAN_IDENT, DOUBLE_IDENT, ERROR_IDENT,
    INTEGER_IDENT, MAP_IDENT, STRING_IDENT,
};
use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Represents connection option
#[derive(Debug)]
pub struct ConnectionOptions {
    host: String,
    port: u16,
}

#[derive(Debug)]
/// Represents a Segment connection
pub struct Connection {
    stream: TcpStream,
    buf: BytesMut,
}

#[derive(Debug, Error)]
/// Represents a connection error
pub enum ConnectionError {
    /// Represents a TCP connection error
    #[error(transparent)]
    TCPError(#[from] io::Error),

    /// Occurs when the connection is prematurely closed by the server
    #[error("server did not send any response")]
    Eof,

    /// Occurs when there is an error in parsing the frame
    #[error(transparent)]
    FrameError(#[from] ParseFrameError),
}

impl Connection {
    /// Creates a new connection from a TcpStream
    pub async fn connect(options: &ConnectionOptions) -> Result<Self, ConnectionError> {
        let stream = TcpStream::connect(format!("{}:{}", options.host(), options.port())).await?;
        Ok(Connection {
            stream,
            buf: BytesMut::with_capacity(4096),
        })
    }

    /// Reads a frame from the connection and parses it
    pub async fn read_frame(&mut self) -> Result<Frame, ConnectionError> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(frame);
            }

            if self.stream.read_buf(&mut self.buf).await? == 0 {
                return Err(ConnectionError::Eof);
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        let mut cursor = Cursor::new(&self.buf[..]);
        match frame::parse(&mut cursor) {
            Ok(frame) => {
                self.buf.advance(cursor.position() as usize);
                Ok(Some(frame))
            }
            Err(ParseFrameError::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Writes a frame to the connection
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        match frame {
            Frame::Array(array) => {
                self.stream.write_u8(ARRAY_IDENT).await?;
                self.stream
                    .write_all(format!("{}\r\n", array.len()).as_bytes())
                    .await?;
                for value in array {
                    self.write_value(value).await?;
                }
            }
            Frame::Map(map) => {
                self.stream.write_u8(MAP_IDENT).await?;
                self.stream
                    .write_all(format!("{}\r\n", map.len() / 2).as_bytes())
                    .await?;
                for value in map {
                    self.write_value(value).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }

        self.stream.flush().await?;
        Ok(())
    }

    async fn write_value(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        match frame {
            Frame::String(data) => {
                let len = data.len();
                self.stream.write_u8(STRING_IDENT).await?;
                self.stream
                    .write_all(format!("{}\r\n", len).as_bytes())
                    .await?;
                self.stream.write_all(data).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(data) => {
                self.stream.write_u8(INTEGER_IDENT).await?;
                self.stream
                    .write_all(format!("{}\r\n", data).as_bytes())
                    .await?;
            }
            Frame::Boolean(data) => {
                self.stream.write_u8(BOOLEAN_IDENT).await?;
                if *data {
                    self.stream
                        .write_all(format!("{}\r\n", 1).as_bytes())
                        .await?;
                } else {
                    self.stream
                        .write_all(format!("{}\r\n", 0).as_bytes())
                        .await?;
                }
            }
            Frame::Null => {
                self.stream.write_all(b"-\r\n").await?;
            }
            Frame::Double(data) => {
                self.stream.write_u8(DOUBLE_IDENT).await?;
                self.stream
                    .write_all(format!("{}\r\n", data).as_bytes())
                    .await?;
            }
            Frame::Error(data) => {
                let len = data.len();
                self.stream.write_u8(ERROR_IDENT).await?;
                self.stream
                    .write_all(format!("{}\r\n", len).as_bytes())
                    .await?;
                self.stream.write_all(data).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

impl ConnectionOptions {
    /// Creates a new connection option
    pub fn new(host: &str, port: u16) -> Self {
        ConnectionOptions {
            host: host.to_string(),
            port,
        }
    }

    /// Returns the connection host
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the connection port
    pub fn port(&self) -> u16 {
        self.port
    }
}
