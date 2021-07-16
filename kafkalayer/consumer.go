package kafkalayer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/isaacRevan24/go-kafka-cloud-event-poc/models"
	"github.com/segmentio/kafka-go"
)

func Consume(ctx context.Context, brokerAddress []string, topic string, groupId string) {

	reader := getKafkaReader(brokerAddress, topic, groupId)

	defer reader.Close()

	fmt.Println("start consuming ... !!")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalln(err)
		}
		var userKafka models.User
		json.Unmarshal(msg.Value, &userKafka)
		fmt.Println("received: ", string(msg.Value))
	}
}

func getKafkaReader(brokers []string, topic string, groupID string) *kafka.Reader {

	logger := log.New(os.Stdout, "kafka reader: ", 0)

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
		Logger:  logger,
	})

}
