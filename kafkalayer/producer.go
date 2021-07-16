package kafkalayer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/isaacRevan24/go-kafka-cloud-event-poc/models"
	"github.com/segmentio/kafka-go"
)

func Produce(ctx context.Context, broker string, topic string, groupid string, user models.User) {

	kafkaWriter := getKafkaWriter(broker, topic, groupid)

	userMessage, _ := json.Marshal(user)

	err := kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("address-%s", broker)),
		Value: userMessage,
	})
	if err != nil {
		log.Fatalln(err)
	}

}

func getKafkaWriter(broker string, topic string, groupID string) *kafka.Writer {

	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}
