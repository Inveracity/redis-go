package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	MsgChan = "messages"
)

type Message struct {
	Text      string
	UserName  string
	UserID    string
	ChannelID string
	Timestamp int64
}

// Write a message to Redis.
//
// Each message is stored in a sorted set with the key "messages:<channel_id>:<user_id>".
func (m *Message) Save(ctx context.Context, client *redis.Client) error {
	z := redis.ZAddArgs{
		Members: []redis.Z{
			{
				Score:  float64(m.Timestamp),
				Member: m.toJson(),
			},
		},
	}

	ret := client.ZAddArgs(ctx, m.zChannel(), z)

	if ret.Err() != nil {
		return ret.Err()
	}

	return nil
}

// LoadHistory expects a client to know the timestamp of the last message it received in order to retreive all unread messages.
// If the client has never received a message, it should pass 0 as the LastTimestamp.
func (m *Message) LoadHistory(ctx context.Context, client *redis.Client, LastTimestamp time.Time) ([]string, error) {
	// Construct a query to get all messages since LastTimestamp
	q := redis.ZRangeArgs{
		Key:     m.zChannel(),
		Start:   fmt.Sprintf("%d", LastTimestamp.UnixNano()),
		Stop:    "+inf",
		ByScore: true,
		Count:   100,
	}

	messages, err := client.ZRangeArgs(ctx, q).Result()
	if err != nil {
		return []string{}, err
	}

	return messages, nil
}

// Construct the channel name to look up messages for a given channel in redis.
func (m *Message) zChannel() string {
	return MsgChan + ":" + m.ChannelID
}

// Convert Message object to Json
func (m *Message) toJson() string {
	b, err := json.Marshal(m)
	if err != nil {
		log.Printf("error marshalling message: %v", err)
	}

	return string(b)
}

// Convert int64 to time
func int64ToTime(i int64) time.Time {
	return time.Unix(0, i)
}

// Convert Json To Message object
func JsonToMessage(s string) (Message, error) {
	var m Message
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return Message{}, err
	}

	return m, nil
}

func Run() {
	ctx := context.Background()
	r := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	message := Message{
		Text:      "Hello World",
		UserName:  "John Doe",
		UserID:    "1234",
		ChannelID: "5678",
		Timestamp: time.Now().UnixNano(),
	}

	message.Save(ctx, r)

	messages, err := message.LoadHistory(ctx, r, time.Now().Add(-1*time.Second))
	if err != nil {
		log.Printf("error loading history: %v", err)
	}

	for _, m := range messages {
		msg, err := JsonToMessage(m)
		if err != nil {
			log.Printf("error converting message: %v", err)
		}

		fmt.Printf("Message: %v, Time: %v\n", msg.Text, int64ToTime(msg.Timestamp))
	}
}
