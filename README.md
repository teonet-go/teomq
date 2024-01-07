# Teonet messages queue

The Teonet messages queue is a part of the Teonet network.

The Teonet messages queue contains three basic parts:

- Teonet messages broker
- Teonet messages consumer
- Teonet messages producer

The Teonet messages broker is responsible for sending and receiving messages
from the Teonet network. It wait connections from message consumers and messages
producers.

The message consumer connect to broker and waits for messages from broker.

The message producer sends messages to the broker. Broker add messages to queue
and send it to first available consumer.

The message consumer process the message and send answer to broker. The broker
remove message from queue and resend answer to producer.

The is one broker and several consumers and producers.

## License

[BSD](LICENSE)
