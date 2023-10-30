package listener

import (
	"context"
	"fmt"
	"log"
	"strconv"
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

func TestConsumeCheck_Prev_Pending_Defer(t *testing.T) {
	// checker
	checkFunc := func(ctx context.Context, msg message.Message) checker.CheckStatus {
		indexInfo := "index=" + msg.Properties()["Index"]
		fmt.Println("*** checker1 started. " + indexInfo)
		status := checker.CheckStatusRejected
		if msg.Properties()["Index"] == "1" {
			status = checker.CheckStatusPassed
			// update index
			msg.Properties()["Index"] = "999"
		}
		status = status.WithHandledDefer(func() {
			defer fmt.Println("***----- checker1 defer executed. " + indexInfo)
		})
		fmt.Println("*** checker1 ended. " + indexInfo)
		return status
	}
	checkFunc2 := func(ctx context.Context, msg message.Message) checker.CheckStatus {
		indexInfo := "index=" + msg.Properties()["Index"]
		fmt.Println("*** checker2 started. " + indexInfo)
		status := checker.CheckStatusRejected
		status = status.WithHandledDefer(func() {
			defer fmt.Println("***----- checker2 defer executed. " + indexInfo)
		})
		fmt.Println("*** checker2 ended. " + indexInfo)
		return status

	}

	// init topics
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeCheck)
	storedTopic := topic
	transferredTopic := internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusPending.TopicSuffix())

	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	// clean up groundTopic
	internal.CleanUpTopic(t, manager, storedTopic)
	internal.CleanUpTopic(t, manager, transferredTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, storedTopic)
		internal.CleanUpTopic(t, manager, transferredTopic)
	}()
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, []string{storedTopic, transferredTopic}, 0)
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	// send messages
	count := 2
	for i := 1; i <= count; i++ {
		msgID, err := producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K, "Index", strconv.Itoa(i)))
		assert.Nil(t, err)
		fmt.Println("produced message: ", msgID)
	}

	// check send stats
	stats, err := manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 2, stats.MsgInCounter)

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: config.ToPointer(uint(1)),
	}
	leveledPolicy := &config.LevelPolicy{
		PendingEnable: config.True(),
		Pending:       testPolicy,
		DeadEnable:    config.False(),
	}
	// create listener
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
	}, checker.PrevHandlePending(checkFunc),
		checker.PrevHandlePending(checkFunc2))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.Start(ctx, func(ctx context.Context, msg message.Message) (bool, error) {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		//panic("mock panic ..... ")
		return true, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(200 * time.Millisecond)
	// check stats
	stats, err = manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 2, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// check transferred stats
	if transferredTopic != "" {
		// wait for decide the message
		time.Sleep(3000 * time.Millisecond)
		stats, err = manager.Stats(transferredTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, 1, stats.MsgOutCounter)
		for _, v := range stats.Subscriptions {
			assert.Equal(t, 0, v.MsgBacklog)
			break
		}
	}
	// stop listener
	cancel()
}
