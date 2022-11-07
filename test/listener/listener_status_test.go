package listener

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
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
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)

	pTopics := make([]string, 0)
	pTopics = append(pTopics, groundTopic)
	if status != message.StatusReady.String() {
		fTopics, err := util.FormatTopics(groundTopic, message.Levels{message.L1}, internal.FormatStatuses(status), internal.TestSubscriptionName())
		assert.Nil(t, err)
		pTopics = append(pTopics, fTopics...)
	}
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)

	// clean up testTopic
	internal.CleanUpTopics(t, manager, pTopics...)
	defer internal.CleanUpTopics(t, manager, pTopics...)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, pTopics, 0)

	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:          groundTopic,
		TransferEnable: config.True(),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
	}, checker.PrevSendTransfer(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
		if storedTopic, ok := msg.Properties["routeTopic"]; ok {
			return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: storedTopic})
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
		msgID, err = producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K, "routeTopic", pTopics[len(pTopics)-1]))
	}
	assert.Nil(t, err)
	fmt.Println("produced message: ", msgID)
	// check send stats
	stats, err := manager.Stats(pTopics[len(pTopics)-1])
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------
	leveledPolicy := &config.LevelPolicy{
		RetryingEnable: config.ToPointer(string(message.StatusRetrying) == status), // enable retrying if matches
		PendingEnable:  config.ToPointer(string(message.StatusPending) == status),  // enable pending if matches
		BlockingEnable: config.ToPointer(string(message.StatusBlocking) == status), // enable blocking if matches
		DeadEnable:     config.ToPointer(string(message.StatusDead) == status),
	}
	// create listener
	listener := internal.CreateListener(client, config.ConsumerConfig{
		Topic:                       groundTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
	})
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.Start(ctx, func(ctx context.Context, msg message.Message) (bool, error) {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		return true, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(50 * time.Millisecond)
	// check stats
	stats, err = manager.Stats(pTopics[len(pTopics)-1])
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// stop listener
	cancel()
}

func TestListen_4Msg_Ready_Retrying_Pending_Blocking(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	// format topics
	statuses := message.Statuses{message.StatusReady, message.StatusRetrying, message.StatusPending, message.StatusBlocking}
	pTopics, err := util.FormatTopics(groundTopic, message.Levels{message.L1}, statuses, internal.TestSubscriptionName())
	assert.Nil(t, err)
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)

	// clean up testTopic
	internal.CleanUpTopics(t, manager, pTopics...)
	defer internal.CleanUpTopics(t, manager, pTopics...)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, pTopics, 0)

	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:          groundTopic,
		TransferEnable: config.True(),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
	}, checker.PrevSendTransfer(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
		if storedTopic, ok := msg.Properties["routeTopic"]; ok {
			return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: storedTopic})
		}
		return checker.CheckStatusRejected
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	// send messages
	for _, storedTopic := range pTopics {
		var msgID pulsar.MessageID
		msgID, err = producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K, "routeTopic", storedTopic))
		assert.Nil(t, err)
		fmt.Println("produced message: ", msgID)
		// check send stats
		stats, err := manager.Stats(storedTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
	}

	// ---------------

	leveledPolicy := &config.LevelPolicy{
		RetryingEnable: config.True(), // enable retrying if matches
		PendingEnable:  config.True(), // enable pending if matches
		BlockingEnable: config.True(), // enable blocking if matches
		DeadEnable:     config.False(),
	}
	// create listener
	listener := internal.CreateListener(client, config.ConsumerConfig{
		Topic:                       groundTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
	})
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.Start(ctx, func(ctx context.Context, msg message.Message) (bool, error) {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		return true, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(100 * time.Millisecond)
	// check stats
	for _, storedTopic := range pTopics {
		stats, err := manager.Stats(storedTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgOutCounter)
		assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	}
	// stop listener
	cancel()
}
