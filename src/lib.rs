#[warn(missing_debug_implementations)]
#[warn(missing_docs)]
/// Contains functions for reading from and writing to a TCP connection
pub mod connection;

#[warn(missing_debug_implementations)]
#[warn(missing_docs)]
/// Contains functions for parsing Segment protocol
pub mod frame;

#[warn(missing_debug_implementations)]
#[warn(missing_docs)]
/// Contains the client logic
pub mod client;

#[warn(missing_debug_implementations)]
#[warn(missing_docs)]
/// Contains functions constructing and parsing commands
pub mod command;
