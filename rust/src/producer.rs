use slog::Logger;

use crate::client;

struct Producer {
    client: client::Client,
}

impl Producer {
    pub async fn new<T>(logger: Logger, topics: T) -> Self
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let mut client = client::Client::new(logger);
        for _topic in topics.into_iter() {
            // client.subscribe(topic.as_ref()).await;
        }

        Producer { client }
    }

    pub fn start(&mut self) {}
}
