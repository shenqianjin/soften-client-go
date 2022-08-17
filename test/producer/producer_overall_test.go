package producer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestProduceOverall_Send3Msg_Transfer1MsgToL1(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	transferredTopic := topic + "-L2"
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)

	internal.CleanUpTopic(t, manager, topic)
	internal.CleanUpTopic(t, manager, transferredTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, topic)
		internal.CleanUpTopic(t, manager, transferredTopic)
	}()

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:          topic,
		TransferEnable: true,
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
	},
		checker.PrevSendTransfer(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "2" {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferredTopic})
			}
			return checker.CheckStatusRejected
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 1; i <= 3; i++ {
		msg := internal.GenerateProduceMessage(internal.Size64)
		msg.Properties["Index"] = strconv.Itoa(i)
		msgID, err := producer.Send(context.Background(), msg)
		assert.Nil(t, err)
		fmt.Println(msgID)
	}

	stats, err := manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, 2, stats.MsgInCounter)

	stats, err = manager.Stats(transferredTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)
}

func TestProduceOverall_SendAsync3Msg_Transfer1MsgToL1(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	transferredTopic := topic + "-L2"
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)

	internal.CleanUpTopic(t, manager, topic)
	internal.CleanUpTopic(t, manager, transferredTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, topic)
		internal.CleanUpTopic(t, manager, transferredTopic)
	}()

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:          topic,
		TransferEnable: true,
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
	},
		checker.PrevSendTransfer(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "2" {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferredTopic})
			}
			return checker.CheckStatusRejected
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	wg := sync.WaitGroup{}
	wg.Add(3)
	for i := 1; i <= 3; i++ {
		msg := internal.GenerateProduceMessage(internal.Size64)
		msg.Properties["Index"] = strconv.Itoa(i)
		producer.SendAsync(context.Background(), msg,
			func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				fmt.Println("sent async message: ", id)
				wg.Done()
			})
	}
	wg.Wait()

	stats, err := manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, 2, stats.MsgInCounter)

	stats, err = manager.Stats(transferredTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)
}
