# Users and Servers example

## Description

- Producer 1 generates number of users and send it to broker.
- Producer 2 generates number of servers and send it to broker.

- Broker gets messages form producers and resend it to subscriber (consumer).

- Consumer gets messages from broker and prints it to log.

## Execution example

Start broker and copy its app address:

```bash
go run ./cmd/users_servers/broker/
```

Start consumer, replace BROKER_ADDRESS with app address from previous step:

```bash
go run ./cmd/users_servers/consumer/ -broker=BROKER_ADDRESS
```

Start producer 1, replace BROKER_ADDRESS with broker app address:

```bash
go run ./cmd/users_servers/produser/ -broker=BROKER_ADDRESS -command=num_players
```

Start producer 2, replace BROKER_ADDRESS with broker app address:

```bash
go run ./cmd/users_servers/produser/ -broker=BROKER_ADDRESS -command=num_servers
```

## License

[BSD](LICENSE)
