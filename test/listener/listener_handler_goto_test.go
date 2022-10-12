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
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testListenHandleCase struct {
	topic                       string
	storedTopic                 string // produce to / consume from
	transferredTopic            string // Handle to
	expectedStoredOutCount      int    // should always 1
	expectedTransferredOutCount int    // 1 for pending, blocking, retrying; 0 for upgrade, degrade, transfer
	handleGoto                  string

	// extra for upgrade/degrade/shift
	shiftLevel string
}

func TestListenHandle_Done(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:       topic,
		storedTopic: topic,
		handleGoto:  decider.GotoDone.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Discard(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:       topic,
		storedTopic: topic,
		handleGoto:  decider.GotoDiscard.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Dead(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:            topic,
		storedTopic:      topic,
		transferredTopic: internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusDead.TopicSuffix()),
		handleGoto:       decider.GotoDead.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Pending(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:                       topic,
		storedTopic:                 topic,
		transferredTopic:            internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusPending.TopicSuffix()),
		handleGoto:                  decider.GotoPending.String(),
		expectedTransferredOutCount: 1, // transfer the msg to pending queue, and then reconsume it
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Blocking(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:                       topic,
		storedTopic:                 topic,
		transferredTopic:            internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusBlocking.TopicSuffix()),
		handleGoto:                  decider.GotoBlocking.String(),
		expectedTransferredOutCount: 1,
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Retrying(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:                       topic,
		storedTopic:                 topic,
		transferredTopic:            internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusRetrying.TopicSuffix()),
		handleGoto:                  decider.GotoRetrying.String(),
		expectedTransferredOutCount: 1,
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Upgrade(t *testing.T) {
	upgradeLevel := message.L2
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:            topic,
		storedTopic:      topic,
		transferredTopic: topic + upgradeLevel.TopicSuffix(),
		shiftLevel:       upgradeLevel.String(),
		handleGoto:       decider.GotoUpgrade.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Degrade(t *testing.T) {
	degradeLevel := message.B2
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:            topic,
		storedTopic:      topic,
		transferredTopic: topic + degradeLevel.TopicSuffix(),
		shiftLevel:       degradeLevel.String(),
		handleGoto:       decider.GotoDegrade.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Shift(t *testing.T) {
	shiftLevel := message.S1
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:            topic,
		storedTopic:      topic,
		transferredTopic: topic + shiftLevel.TopicSuffix(),
		shiftLevel:       shiftLevel.String(),
		handleGoto:       decider.GotoShift.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Transfer(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		topic:            topic,
		storedTopic:      topic,
		transferredTopic: topic + message.S2.TopicSuffix(),
		handleGoto:       decider.GotoTransfer.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func testListenHandleGoto(t *testing.T, handleCase testListenHandleCase) {
	topic := handleCase.topic
	storedTopic := handleCase.storedTopic
	TransferredTopic := handleCase.transferredTopic
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up groundTopic
	internal.CleanUpTopic(t, manager, storedTopic)
	internal.CleanUpTopic(t, manager, TransferredTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, storedTopic)
		internal.CleanUpTopic(t, manager, TransferredTopic)
	}()
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
	msgID, err := producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
	assert.Nil(t, err)
	fmt.Println("produced message: ", msgID)
	// Handle send stats
	stats, err := manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: 1,
	}
	// create listener
	shiftLevel, _ := message.LevelOf(handleCase.shiftLevel)
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoDiscard.String()),
		DeadEnable:     config.ToPointer(handleCase.handleGoto == decider.GotoDead.String()),
		PendingEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoPending.String()),
		Pending:        testPolicy,
		BlockingEnable: config.ToPointer(handleCase.handleGoto == decider.GotoBlocking.String()),
		Blocking:       testPolicy,
		RetryingEnable: config.ToPointer(handleCase.handleGoto == decider.GotoRetrying.String()),
		Retrying:       testPolicy,
		UpgradeEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoUpgrade.String()),
		Upgrade:        &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
		DegradeEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoDegrade.String()),
		Degrade:        &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
		ShiftEnable:    config.ToPointer(handleCase.handleGoto == decider.GotoShift.String()),
		Shift:          &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
		TransferEnable: config.ToPointer(handleCase.handleGoto == decider.GotoTransfer.String()),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: handleCase.handleGoto == decider.GotoTransfer.String()},
	}
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, func(ctx context.Context, msg message.Message) handler.HandleStatus {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		if handleGoto, err := decider.GotoOf(handleCase.handleGoto); err == nil {
			if handleGoto == decider.GotoTransfer {
				return handler.StatusTransfer.WithTopic(handleCase.transferredTopic)
			}
			if status, err := handler.StatusOf(handleCase.handleGoto); err == nil {
				return status
			}
		}
		return handler.StatusAuto

	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(100 * time.Millisecond)
	// Handle stats
	stats, err = manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// Handle transferred stats
	if TransferredTopic != "" {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		stats, err = manager.Stats(TransferredTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, handleCase.expectedTransferredOutCount, stats.MsgOutCounter)
		if handleCase.handleGoto == decider.GotoPending.String() ||
			handleCase.handleGoto == decider.GotoBlocking.String() ||
			handleCase.handleGoto == decider.GotoRetrying.String() {
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 1, v.MsgBacklog)
				break
			}
		}
	}
	// stop listener
	cancel()
}

func TestListenHandle_All(t *testing.T) {
	upgradeLevel := message.L2
	degradeLevel := message.B2
	shiftLevel := message.S1
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	statusTopicPrefix := internal.FormatStatusTopic(groundTopic, internal.TestSubscriptionName(), "", "")
	transferTopic := groundTopic + message.S2.TopicSuffix()
	decidedTopics := []string{
		"", // done
		"", // discard
		statusTopicPrefix + message.StatusDead.TopicSuffix(),     // dead
		statusTopicPrefix + message.StatusPending.TopicSuffix(),  // pending
		statusTopicPrefix + message.StatusBlocking.TopicSuffix(), // blocking
		statusTopicPrefix + message.StatusRetrying.TopicSuffix(), // retrying
		groundTopic + upgradeLevel.TopicSuffix(),                 // upgrade
		groundTopic + degradeLevel.TopicSuffix(),                 // degrade
		groundTopic + shiftLevel.TopicSuffix(),                   // shift
		transferTopic,                                            // transfer
	}
	midConsumedTopics := []string{
		decidedTopics[3],
		decidedTopics[4],
		decidedTopics[5],
	}
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up groundTopic
	internal.CleanUpTopic(t, manager, groundTopic)
	for _, decidedTopic := range decidedTopics {
		if decidedTopic != "" {
			internal.CleanUpTopic(t, manager, decidedTopic)
		}
	}
	defer func() {
		internal.CleanUpTopic(t, manager, groundTopic)
		for _, decidedTopic := range decidedTopics {
			if decidedTopic != "" {
				internal.CleanUpTopic(t, manager, decidedTopic)
			}
		}
	}()
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic: groundTopic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	// send messages
	for i := 0; i < len(decidedTopics); i++ {
		msg := internal.GenerateProduceMessage(internal.Size64, "Index", strconv.Itoa(i))
		mid, err := producer.Send(context.Background(), msg)
		assert.Nil(t, err)
		fmt.Println("sent message: ", mid)
	}
	// check send stats
	stats, err := manager.Stats(groundTopic)
	assert.Nil(t, err)
	assert.Equal(t, len(decidedTopics), stats.MsgInCounter)
	assert.Equal(t, 0, stats.MsgOutCounter)

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: 1,
	}
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:  config.True(),
		DeadEnable:     config.True(),
		PendingEnable:  config.True(),
		Pending:        testPolicy,
		BlockingEnable: config.True(),
		Blocking:       testPolicy,
		RetryingEnable: config.True(),
		Retrying:       testPolicy,
		UpgradeEnable:  config.True(),
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		DegradeEnable:  config.True(),
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		ShiftEnable:    config.True(),
		Shift:          &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
		TransferEnable: config.True(),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
	}
	// create listener
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       groundTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, func(ctx context.Context, msg message.Message) handler.HandleStatus {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		if index, ok := msg.Properties()["Index"]; ok {
			switch index {
			case "0":
				return handler.StatusDone
			case "1":
				return handler.StatusDiscard
			case "2":
				return handler.StatusDead
			case "3":
				return handler.StatusPending
			case "4":
				return handler.StatusBlocking
			case "5":
				return handler.StatusRetrying
			case "6":
				return handler.StatusUpgrade
			case "7":
				return handler.StatusDegrade
			case "8":
				return handler.StatusShift
			case "9":
				return handler.StatusTransfer.WithTopic(transferTopic)
			}
		}
		return handler.StatusDone
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message and wait for decide the message
	time.Sleep(500 * time.Millisecond)
	// check stats
	stats, err = manager.Stats(groundTopic)
	assert.Nil(t, err)
	assert.Equal(t, len(decidedTopics), stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// check decided stats
	for _, decidedTopic := range decidedTopics {
		if decidedTopic == "" {
			continue
		}
		stats, err = manager.Stats(decidedTopic)
		assert.Nil(t, err, "decided groundTopic: ", decidedTopic)
		assert.Equal(t, 1, stats.MsgInCounter, "decided groundTopic: ", decidedTopic)
		isMidConsumeTopic := false
		for _, midConsumedTopic := range midConsumedTopics {
			if decidedTopic == midConsumedTopic {
				isMidConsumeTopic = true
				break
			}
		}
		if isMidConsumeTopic {
			assert.Equal(t, 1, stats.MsgOutCounter)
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 1, v.MsgBacklog, "decided groundTopic: ", decidedTopic)
				break
			}
		} else {
			assert.Equal(t, 0, stats.MsgOutCounter)
		}
	}
	// stop listener
	cancel()
}
