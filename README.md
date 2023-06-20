# Redis and Golang

Playing around with Redis sorted sets as a way of retrieving a history of message.

By using a UnixNano timestamp as the Score, and the Member is a JSON payload a query could look like this: `1687272168555030500:+inf` where the Start is the last timestamp read by a user and `+inf` being the latest entry in the sorted set.

Running the go code will write a new member to a sorted set and then immediately read all members between unix timestamp _now_ and _a second ago_.

Rapidly running the code will yield as many messages as you can fit into 1 second.

```sh
docker compose up -d

go run cmd/broker/main.go
#   Message: Hello World, Time: 2023-06-20 16:55:15.147686966 +0200 CEST

go run cmd/broker/main.go
#   Message: Hello World, Time: 2023-06-20 16:55:15.147686966 +0200 CEST
#   Message: Hello World, Time: 2023-06-20 16:55:15.877785749 +0200 CEST
```
