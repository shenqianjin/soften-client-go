package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

func main() {
	client, err := soften.NewClient(config.ClientConfig{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:         "topic-1",
		UpgradeEnable: true,
		Upgrade:       &config.ShiftPolicy{Level: message.L2},
		DeadEnable:    true,
	}, checker.PrevSendUpgrade(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
		if "VIP" == msg.Properties["UserType"] {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevSendDead(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
		if "Disable" == msg.Properties["UserStatus"] {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		} else {
			log.Println("Published message: ", msgId)
		}
	}
}
