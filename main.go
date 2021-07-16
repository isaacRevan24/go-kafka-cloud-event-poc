package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	gin "github.com/gin-gonic/gin"
	kafka "github.com/segmentio/kafka-go"
)

type User struct {
	Name     string `json:"name"`
	LastName string `json:"lastname"`
	Age      int    `json:"age"`
}

func main() {

	ctx := context.Background()
	r := gin.Default()
	broker := []string{"localhost:9092"}
	topic := "gamification.topic.consumer"
	groupid := "group_1"

	r.GET("/", func(c *gin.Context) {
		var user User
		user.Age = 23
		user.Name = "isaac"
		user.LastName = "revan"

		go produce(ctx, broker[0], topic, groupid, user)
	})
	go consume(ctx, broker, topic, groupid)

	r.Run()

}

func produce(ctx context.Context, broker string, topic string, groupid string, user User) {

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

func consume(ctx context.Context, brokerAddress []string, topic string, groupId string) {

	reader := getKafkaReader(brokerAddress, topic, groupId)

	defer reader.Close()

	fmt.Println("start consuming ... !!")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalln(err)
		}
		var userKafka User
		json.Unmarshal(msg.Value, &userKafka)
		fmt.Println("received: ", string(msg.Value))
	}
}

func getKafkaWriter(broker string, topic string, groupID string) *kafka.Writer {

	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
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
