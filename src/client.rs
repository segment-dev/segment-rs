use crate::connection::{Connection, ConnectionError, ConnectionOptions};

#[derive(Debug)]
/// Segment client
pub struct Client {
    options: ConnectionOptions,
}

impl Client {
    /// Creates a new client, this does not create a new connection
    pub fn new(options: ConnectionOptions) -> Self {
        Client { options }
    }

    /// Creates a new connection
    pub async fn get_connection(&self) -> Result<Connection, ConnectionError> {
        Connection::connect(&self.options).await
    }
}
