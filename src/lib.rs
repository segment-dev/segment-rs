use thiserror::Error;

#[warn(missing_debug_implementations)]
#[warn(missing_docs)]
/// Contains functions for reading from and writing to a tcp connection
pub mod connection;

#[warn(missing_debug_implementations)]
#[warn(missing_docs)]
/// Contains functions for parsing Segment protocol
pub mod frame;

#[warn(missing_debug_implementations)]
#[warn(missing_docs)]
/// Contains functions for parsing Segment protocol
pub mod client;

#[warn(missing_debug_implementations)]
#[warn(missing_docs)]
/// Contains functions for parsing Segment protocol
pub mod command;

#[derive(Debug, Error)]
pub enum SegmentError {}

pub type SegmentResult<T> = Result<T, SegmentError>;
