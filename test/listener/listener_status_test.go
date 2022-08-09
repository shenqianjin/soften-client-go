package listener

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestListen_1Msg_Ready(t *testing.T) {
	testListenBySingleStatus(t, string(message.StatusReady))
}

func TestListen_1Msg_Retrying(t *testing.T) {
	testListenBySingleStatus(t, string(message.StatusRetrying))
}

func TestListen_1Msg_Pending(t *testing.T) {
	testListenBySingleStatus(t, string(message.StatusPending))
}

func TestListen_1Msg_Blocking(t *testing.T) {
	testListenBySingleStatus(t, string(message.StatusBlocking))
}

func testListenBySingleStatus(t *testing.T, status string) {
	topic := internal.GenerateTestTopic()
	storedTopic := topic
	if status != string(message.StatusReady) {
		storedTopic = topic + "-" + strings.ToUpper(status)
	}
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up topic
	internal.CleanUpTopic(t, manager, storedTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, storedTopic)
	}()
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:       topic,
		RouteEnable: true,
		Route:       &config.RoutePolicy{ConnectInSyncEnable: true},
	}, checker.PrevSendRoute(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
		if storedTopic, ok := msg.Properties["routeTopic"]; ok {
			return checker.CheckStatusPassed.WithRerouteTopic(storedTopic)
		}
		return checker.CheckStatusRejected
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	// send messages
	var msgID pulsar.MessageID
	if status == string(message.StatusReady) {
		msgID, err = producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
	} else {
		msgID, err = producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K, "routeTopic", storedTopic))
	}
	assert.Nil(t, err)
	fmt.Println("produced message: ", msgID)
	// check send stats
	stats, err := manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	// create listener
	listener := internal.CreateListener(client, config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.GenerateSubscribeNameByTopic(topic),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		RetryingEnable:              string(message.StatusRetrying) == status, // enable retrying if matches
		PendingEnable:               string(message.StatusPending) == status,  // enable pending if matches
		BlockingEnable:              string(message.StatusBlocking) == status, // enable blocking if matches
	})
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.Start(ctx, func(message pulsar.Message) (bool, error) {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(message.Payload()), message.Properties())
		return true, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(50 * time.Millisecond)
	// check stats
	stats, err = manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// stop listener
	cancel()
}

func TestListen_4Msg_Ready_Retrying_Pending_Blocking(t *testing.T) {
	topic := internal.GenerateTestTopic()
	// format topics
	statuses := []string{string(message.StatusReady), string(message.StatusRetrying), string(message.StatusPending), string(message.StatusBlocking)}
	storedTopics := make([]string, len(statuses))
	for index, status := range statuses {
		statusTopic := topic
		if status != string(message.StatusReady) {
			statusTopic = topic + "-" + strings.ToUpper(status)
		}
		storedTopics[index] = statusTopic
	}

	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up topic
	for _, storedTopic := range storedTopics {
		internal.CleanUpTopic(t, manager, storedTopic)
	}
	defer func() {
		for _, storedTopic := range storedTopics {
			internal.CleanUpTopic(t, manager, storedTopic)
		}
	}()
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:       topic,
		RouteEnable: true,
		Route:       &config.RoutePolicy{ConnectInSyncEnable: true},
	}, checker.PrevSendRoute(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
		if storedTopic, ok := msg.Properties["routeTopic"]; ok {
			return checker.CheckStatusPassed.WithRerouteTopic(storedTopic)
		}
		return checker.CheckStatusRejected
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	// send messages
	for _, storedTopic := range storedTopics {
		var msgID pulsar.MessageID
		if storedTopic == topic { // ready status
			msgID, err = producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
		} else {
			msgID, err = producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K, "routeTopic", storedTopic))
		}
		assert.Nil(t, err)
		fmt.Println("produced message: ", msgID)
		// check send stats
		stats, err := manager.Stats(storedTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
	}

	// ---------------

	// create listener
	listener := internal.CreateListener(client, config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.GenerateSubscribeNameByTopic(topic),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		RetryingEnable:              true, // enable retrying if matches
		PendingEnable:               true, // enable pending if matches
		BlockingEnable:              true, // enable blocking if matches
	})
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.Start(ctx, func(message pulsar.Message) (bool, error) {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(message.Payload()), message.Properties())
		return true, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(100 * time.Millisecond)
	// check stats
	for _, storedTopic := range storedTopics {
		stats, err := manager.Stats(storedTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgOutCounter)
		assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	}
	// stop listener
	cancel()
}
