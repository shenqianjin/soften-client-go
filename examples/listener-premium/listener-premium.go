package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

func main() {
	client, err := soften.NewClient(config.ClientConfig{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       "topic-1",
		SubscriptionName:            "my-subscription-premium",
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Concurrency:                 &config.ConcurrencyPolicy{CorePoolSize: 10},
	}, checker.PostHandleRetrying(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if err != nil {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	messageHandle := func(ctx context.Context, msg message.Message) handler.HandleStatus {
		fmt.Printf("Received message  msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		if _, ok := msg.Properties()["invalid-param"]; ok {
			return handler.StatusDead
		}
		return handler.StatusDone
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, messageHandle)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	cancel()
}
