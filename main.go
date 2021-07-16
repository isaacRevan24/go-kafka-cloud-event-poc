package main

import (
	"context"

	gin "github.com/gin-gonic/gin"
	"github.com/isaacRevan24/go-kafka-cloud-event-poc/kafkalayer"
	"github.com/isaacRevan24/go-kafka-cloud-event-poc/models"
)

func main() {

	ctx := context.Background()
	r := gin.Default()
	broker := []string{"localhost:9092"}
	topic := "gamification.topic.consumer"
	groupid := "group_1"

	r.GET("/", func(c *gin.Context) {
		var user models.User
		user.Age = 23
		user.Name = "isaac"
		user.LastName = "revan"

		go kafkalayer.Produce(ctx, broker[0], topic, groupid, user)

	})

	go kafkalayer.Consume(ctx, broker, topic, groupid)

	r.Run()

}
