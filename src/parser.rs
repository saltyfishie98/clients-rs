use mqtt::AsyncClient;
use paho_mqtt as mqtt;

struct Parser {
    mqtt_client: mqtt::AsyncClient,
}

enum ParserBuilderError {
    NoClient,
}

struct ParserBuilder {
    mqtt_client: Option<mqtt::AsyncClient>,
}

impl ParserBuilder {
    fn new() -> Self {
        ParserBuilder { mqtt_client: None }
    }

    fn with_client(mut self, client: AsyncClient) -> Self {
        self.mqtt_client = Some(client);
        self
    }

    fn build(self) -> Result<Parser, ParserBuilderError> {
        match self.mqtt_client {
            Some(client) => Ok(Parser {
                mqtt_client: client,
            }),
            None => Err(ParserBuilderError::NoClient),
        }
    }
}
