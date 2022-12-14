package main

import (
	"context"
	"fmt"
	"log"
	"strings"
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

	lvlPolicy := &config.LevelPolicy{
		DegradeEnable:  config.True(),
		Degrade:        &config.ShiftPolicy{Level: message.B1},
		DeadEnable:     config.True(),
		RetryingEnable: config.True(),
	}
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       "topic-1",
		SubscriptionName:            "my-subscription",
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 lvlPolicy,
	}, checker.PrevHandleDegrade(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if "Junior" == msg.Properties()["UserLevel"] {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PostHandleDead(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if strings.Contains(err.Error(), "Bad Request") {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	messageHandle := func(ctx context.Context, msg message.Message) handler.HandleStatus {
		var err error
		fmt.Printf("Received message  msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		// here do you business logic
		if err != nil {
			if strings.Contains(err.Error(), "Internal Server Error") {
				return handler.StatusRetrying
			}
			return handler.StatusAuto
		}
		return handler.StatusDone
	}
	err = listener.StartPremium(context.Background(), messageHandle)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)
}
