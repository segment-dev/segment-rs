use crate::connection::{Connection, ConnectionError};
use crate::frame::Frame;
use bytes::Bytes;
use std::str::{self, Utf8Error};
use thiserror::Error;

/// Used to convert a value to a Segment frame
pub trait ToSegmentFrame {
    /// Creates a new Segment frame from value
    fn to_segment_frame(&self) -> Frame;
}

/// Used to create a value from a Segment frame
pub trait FromSegmentFrame: Sized {
    /// Creates a new value from Segment frame
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError>;
}

/// Specifies a Segment command
#[derive(Debug)]
pub struct Command {
    args: Vec<Frame>,
}

/// Represents a command error
#[derive(Debug, Error)]
pub enum CommandError {
    /// Occurs when the type returned by the server and the type requested are incompatible
    #[error("incompatible response type")]
    IncompatibleType,

    /// Represents a utf8 conversion error
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),

    /// Represents an error returned by the segment server
    #[error("segment: {0}")]
    QueryError(String),

    /// Represents a connection error
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
}

impl Command {
    /// Creates an empty command
    pub fn new() -> Self {
        Command { args: Vec::new() }
    }

    /// Pushes an arg to command's arg vec
    pub fn arg<T: ToSegmentFrame>(&mut self, arg: T) -> &mut Self {
        self.args.push(arg.to_segment_frame());
        self
    }

    /// Constructs a command from the args, executes it and returns the result
    pub async fn query<T: FromSegmentFrame>(
        self,
        connection: &mut Connection,
    ) -> Result<T, CommandError> {
        let cmd = Frame::Array(self.args);
        connection.write_frame(&cmd).await?;
        let response = connection.read_frame().await?;

        match response {
            Frame::Error(val) => Err(CommandError::QueryError(
                str::from_utf8(&val[..])?.to_string(),
            )),
            _ => T::from_segment_frame(&response),
        }
    }
}

impl Default for Command {
    fn default() -> Self {
        Self::new()
    }
}

impl ToSegmentFrame for u8 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for i8 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for u16 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for i16 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for u32 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for i32 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for u64 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for i64 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for usize {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for isize {
    fn to_segment_frame(&self) -> Frame {
        Frame::Integer(*self as i64)
    }
}

impl ToSegmentFrame for f32 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Double(*self as f64)
    }
}

impl ToSegmentFrame for f64 {
    fn to_segment_frame(&self) -> Frame {
        Frame::Double(*self as f64)
    }
}

impl ToSegmentFrame for bool {
    fn to_segment_frame(&self) -> Frame {
        Frame::Boolean(*self)
    }
}

impl ToSegmentFrame for String {
    fn to_segment_frame(&self) -> Frame {
        Frame::String(Bytes::from(self.clone()))
    }
}

impl ToSegmentFrame for &str {
    fn to_segment_frame(&self) -> Frame {
        Frame::String(Bytes::from(self.to_string()))
    }
}

impl ToSegmentFrame for Bytes {
    fn to_segment_frame(&self) -> Frame {
        Frame::String(self.clone())
    }
}

impl<T: ToSegmentFrame> ToSegmentFrame for Option<T> {
    fn to_segment_frame(&self) -> Frame {
        if let Some(val) = self {
            return T::to_segment_frame(val);
        }
        Frame::Null
    }
}

impl FromSegmentFrame for u8 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as u8),
            Frame::Double(val) => Ok(*val as u8),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for i8 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as i8),
            Frame::Double(val) => Ok(*val as i8),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for u16 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as u16),
            Frame::Double(val) => Ok(*val as u16),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for i16 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as i16),
            Frame::Double(val) => Ok(*val as i16),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for u32 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as u32),
            Frame::Double(val) => Ok(*val as u32),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for i32 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as i32),
            Frame::Double(val) => Ok(*val as i32),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for u64 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as u64),
            Frame::Double(val) => Ok(*val as u64),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for i64 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val),
            Frame::Double(val) => Ok(*val as i64),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for f32 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as f32),
            Frame::Double(val) => Ok(*val as f32),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for f64 {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Integer(val) => Ok(*val as f64),
            Frame::Double(val) => Ok(*val),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for bool {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Boolean(val) => Ok(*val),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for String {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::String(val) => Ok(str::from_utf8(&val[..])?.to_string()),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl FromSegmentFrame for Bytes {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::String(val) => Ok(val.clone()),
            _ => Err(CommandError::IncompatibleType),
        }
    }
}

impl<T: FromSegmentFrame> FromSegmentFrame for Option<T> {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Null => Ok(None),
            _ => Ok(Some(T::from_segment_frame(frame)?)),
        }
    }
}

impl<T: FromSegmentFrame> FromSegmentFrame for Vec<T> {
    fn from_segment_frame(frame: &Frame) -> Result<Self, CommandError> {
        let mut vec = Vec::new();
        match frame {
            Frame::Array(array) => {
                for v in array {
                    vec.push(T::from_segment_frame(v)?);
                }
                Ok(vec)
            }
            _ => Err(CommandError::IncompatibleType),
        }
    }
}
