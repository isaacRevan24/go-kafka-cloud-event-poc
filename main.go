package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topic         = "gamification.topic.consumer"
	brokerAddress = "localhost:9092"
)

type User struct {
	Name     string `json:"name"`
	LastName string `json:"lastname"`
	Age      int    `json:"age"`
}

func main() {

	ctx := context.Background()

	broker := []string{"localhost:9092"}
	topic := "gamification.topic.consumer"
	groupid := "group_1"

	go produce(ctx)
	consume(ctx, broker, topic, groupid)

}

func produce(ctx context.Context) {
	i := 0

	l := log.New(os.Stdout, "kafka writer: ", 0)

	w := kafka.NewWriter(kafka.WriterConfig{
		//Brokers: []string{brokerAddress},
		Brokers: []string{brokerAddress},
		Topic:   topic,

		// assign the logger to the writer
		Logger: l,
	})

	var user User
	user.Age = 23
	user.Name = "isaac"
	user.LastName = "revan"

	userMessage, _ := json.Marshal(user)

	for {
		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		// the message gets published on
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			Value: userMessage,
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes:", i)
		i++
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
		fmt.Println(userKafka.Age)
		fmt.Println(userKafka.LastName)
		fmt.Println(userKafka.Name)
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
