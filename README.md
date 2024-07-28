# Teonet Messages Queue

The Teonet Messages Queue is a part of the Teonet network.

A message queue is a form of asynchronous service-to-service communication used in serverless and microservices architectures. Messages are stored on the queue until they are processed and deleted. Each message is processed only once, by a single consumer. Message queues can be used to decouple heavyweight processing, to buffer or batch work, and to smooth spiky workloads.

## Basic teomq scheme

The basic teomq scheme works like load balancer between Producers and Consumers.

The Teonet Messages Queue contains three basic parts:

- Teonet messages broker
- Teonet messages consumer
- Teonet messages producer

![Basic teomq scheme](img/Basic-mq.drawio.png)

### Broker

The Teonet messages Broker is responsible for sending and receiving messages
from the Teonet network. It wait connections from message Consumers and messages
Producers.

### Consumer

The Consumer connect to Broker and waits for messages from Broker.

### Producer

The message Producer sends messages to the Broker. Broker add messages to queue
and send it to first available Consumer.

The message Consumer process the message and send answer to Broker. The Broker
remove message from queue and resend answer to Producer.

The is one Broker and several Consumers and Producers.

Many producers and consumers can use the Brokers queue, but each message is processed only once, by a single consumer. For this reason, this messaging pattern is often called one-to-one, or point-to-point, communications.

### Basic teomq exsample

Start three aplication in three terminals.

First start the Broker:

```bash
# Start broker
go run ./cmd/basic/broker/
```

When the Broker starts, it prints his teonet address in the console. This
address will be used in the next steps, when you start Consumers and Producers.

The string with address looks like this:

```bash
Connected to Teonet, this app address: og71X6Y8TU1Y2W4G9GkUsKmxnvvd9r2vXp2
```

Then start Consumer using Broker address:

```bash
# Start consumer
go run ./cmd/basic/consumer/ -broker=og71X6Y8TU1Y2W4G9GkUsKmxnvvd9r2vXp2
```

Then start Producer using Broker address:

```bash
# Start producer
go run ./cmd/basic/producer/ -broker=og71X6Y8TU1Y2W4G9GkUsKmxnvvd9r2vXp2
```

How this Basic teomq exsample works:

Producer sends messages to Broker. Broker prints messages in the console, save it to it's queue and resend it to the Consumer if Consumer is connected to Broker.

Consumer receives messages from Broker, prints them in the console and send it back to Broker. Broker resend this answers to Producer.

You can press Ctrl+C on Consumer terminal to stop the application. When Consumer will stoped, Broker will save messages to queue. When you start Consumer again, it will read messages from Brokers queue, process them and send answers.

## Command teomq scheme exsample

In command teomq scheme the Teonet Messages Queue consumers subscribes to specific commands (or events).

## Users_server exsample

In users_servers example the Teonet Messages Queue consumer subscribes to specific commands (or events).
The is two producers which send num_users and num_servers messages to Broker.
The Broker prints messages in the console and save it to it's queue.
The queue is processed by the consumer including the web consumer which connect to the Broker using Teoproxy package.

### Run users_servers example

Start Broker:

```bash
# Start broker
go run ./cmd/users_servers/broker/
```

When the Broker starts, it prints his teonet address in the console. This
address will be used in the next steps, when you start Consumers and Producers.

The string with address looks like this:

```bash
Connected to Teonet, this app address: og71X6Y8TU1Y2W4G9GkUsKmxnvvd9r2vXp2
```

Then start Consumer using Broker address:

```bash
# Start consumer
go run ./cmd/users_servers/consumer/ -broker=og71X6Y8TU1Y2W4G9GkUsKmxnvvd9r2vXp2
```

Then start Producers using Broker address:

```bash
# Start producers
go run ./cmd/users_servers/produser/ -broker=J4c0OciuN5R0cYfw652T9XkuvckAnUTJj5c -name=produser-1 -command=num_servers
```

```bash
# Start producers
go run ./cmd/users_servers/produser/ -broker=J4c0OciuN5R0cYfw652T9XkuvckAnUTJj5c -name=produser-2 -command=num_players
```

Start web consumer:

_Edit web consumer configs:_

- edit port number in the [main.go](./cmd/users_servers/consumer_web/main.go) or use default value :8094
- edit teoproxy url and broker address in the [client.js](./cmd/users_servers/consumer_web/client.js) or use default values

```bash
# Start web consumer http server
go run ./cmd/users_servers/consumer_web/
```

Run web consumer in your browser: [http://localhost:8094/cmd/users_servers/consumer_web/client.html](http://localhost:8094/cmd/users_servers/consumer_web/client.html)

## License

[BSD](LICENSE)
