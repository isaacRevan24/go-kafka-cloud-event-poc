package kafkalayer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	cloudevents "github.com/cloudevents/sdk-go/v2"
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

	go cloudEventHandler()

}

func getKafkaWriter(broker string, topic string, groupID string) *kafka.Writer {

	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func cloudEventHandler() {
	event := cloudevents.NewEvent()
	event.SetID("example-uuid-32943bac6fea")
	event.SetSource("example/uri")
	event.SetType("example.type")
	event.SetData(cloudevents.ApplicationJSON, map[string]string{"hello": "world"})
	bytes, err := json.Marshal(event)

	if err != nil {
		fmt.Println("malmal")
	}
	fmt.Println(bytes)
}
