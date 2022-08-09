package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften"
	"github.com/shenqianjin/soften-client-go/soften/config"
)

func main() {
	client, err := soften.NewClient(config.ClientConfig{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       "topic-1",
		SubscriptionName:            "my-subscription",
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	messageHandle := func(msg pulsar.Message) (bool, error) {
		fmt.Printf("Received message  msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		return true, nil
	}
	err = listener.Start(context.Background(), messageHandle)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)
}
