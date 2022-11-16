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
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testListenHandleCase struct {
	groundTopic                 string
	consumeToLevel              string
	consumeToStatus             string
	expectedStoredOutCount      int // should always 1
	expectedTransferredOutCount int // 1 for pending, blocking, retrying; 0 for upgrade, degrade, transfer
	handleGoto                  string

	// extra for upgrade/degrade/shift/transfer
	shiftLevel      string
	upgradeLevel    string
	degradeLevel    string
	transferToTopic string
}

func TestListenHandle_Done(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic: topic,
		handleGoto:  decider.GotoDone.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Discard(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic: groundTopic,
		handleGoto:  decider.GotoDiscard.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Dead(t *testing.T) {
	status := message.StatusDead
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic:     groundTopic,
		consumeToStatus: status.String(),
		handleGoto:      decider.GotoDead.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Pending(t *testing.T) {
	status := message.StatusPending
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic:                 groundTopic,
		consumeToStatus:             status.String(),
		handleGoto:                  decider.GotoPending.String(),
		expectedTransferredOutCount: 1, // transfer the msg to pending queue, and then reconsume it
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Blocking(t *testing.T) {
	status := message.StatusBlocking
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic:                 groundTopic,
		consumeToStatus:             status.String(),
		handleGoto:                  decider.GotoBlocking.String(),
		expectedTransferredOutCount: 1,
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Retrying(t *testing.T) {
	status := message.StatusRetrying
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic:                 groundTopic,
		consumeToStatus:             status.String(),
		handleGoto:                  decider.GotoRetrying.String(),
		expectedTransferredOutCount: 1,
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Upgrade(t *testing.T) {
	upgradeLevel := message.L2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic:    groundTopic,
		consumeToLevel: upgradeLevel.String(),
		upgradeLevel:   upgradeLevel.String(),
		shiftLevel:     upgradeLevel.String(),
		handleGoto:     decider.GotoUpgrade.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Degrade(t *testing.T) {
	degradeLevel := message.B2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic:    groundTopic,
		consumeToLevel: degradeLevel.String(),
		degradeLevel:   degradeLevel.String(),
		shiftLevel:     degradeLevel.String(),
		handleGoto:     decider.GotoDegrade.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Shift(t *testing.T) {
	shiftLevel := message.S1
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenHandleCase{
		groundTopic:    groundTopic,
		consumeToLevel: shiftLevel.String(),
		shiftLevel:     shiftLevel.String(),
		handleGoto:     decider.GotoShift.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Transfer(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	transferToTopic := groundTopic + "-OTHER"
	HandleCase := testListenHandleCase{
		groundTopic:     groundTopic,
		transferToTopic: transferToTopic,
		handleGoto:      decider.GotoTransfer.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func testListenHandleGoto(t *testing.T, testCase testListenHandleCase) {
	groundTopic := testCase.groundTopic
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	// format topics
	pTopics := make([]string, 0)
	pTopics = append(pTopics, testCase.groundTopic)
	cTopics := make([]string, 0)
	if testCase.handleGoto == decider.GotoTransfer.String() {
		cTopics = append(cTopics, testCase.transferToTopic)
	} else if testCase.handleGoto == decider.GotoDiscard.String() {
		// do nothing
	} else if testCase.consumeToLevel != "" {
		fTopics, err := util.FormatTopics(testCase.groundTopic, internal.FormatLevels(testCase.consumeToLevel), message.Statuses{message.StatusReady}, "")
		assert.Nil(t, err)
		cTopics = append(cTopics, fTopics...)
	} else if testCase.consumeToStatus != "" {
		fTopics, err := util.FormatTopics(testCase.groundTopic, message.Levels{message.L1}, internal.FormatStatuses(testCase.consumeToStatus), internal.TestSubscriptionName())
		assert.Nil(t, err)
		cTopics = append(cTopics, fTopics...)
	}
	topics := append(pTopics, cTopics...)
	// clean up topic
	internal.CleanUpTopics(t, manager, topics...)
	defer internal.CleanUpTopics(t, manager, topics...)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, topics, 0)
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
	msgID, err := producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
	assert.Nil(t, err)
	fmt.Println("produced message: ", msgID)
	// Handle send stats
	stats, err := manager.Stats(pTopics[0])
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: config.ToPointer(uint(1)),
	}
	// create listener
	shiftLevel, _ := message.LevelOf(testCase.shiftLevel)
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:  config.ToPointer(testCase.handleGoto == decider.GotoDiscard.String()),
		Discard:        &config.DiscardPolicy{LogLevel: "debug"},
		DeadEnable:     config.ToPointer(testCase.handleGoto == decider.GotoDead.String()),
		PendingEnable:  config.ToPointer(testCase.handleGoto == decider.GotoPending.String()),
		Pending:        testPolicy,
		BlockingEnable: config.ToPointer(testCase.handleGoto == decider.GotoBlocking.String()),
		Blocking:       testPolicy,
		RetryingEnable: config.ToPointer(testCase.handleGoto == decider.GotoRetrying.String()),
		Retrying:       testPolicy,
		UpgradeEnable:  config.ToPointer(testCase.handleGoto == decider.GotoUpgrade.String()),
		Upgrade:        &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
		DegradeEnable:  config.ToPointer(testCase.handleGoto == decider.GotoDegrade.String()),
		Degrade:        &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
		ShiftEnable:    config.ToPointer(testCase.handleGoto == decider.GotoShift.String()),
		Shift:          &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
		TransferEnable: config.ToPointer(testCase.handleGoto == decider.GotoTransfer.String()),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: testCase.handleGoto == decider.GotoTransfer.String()},
	}
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
		if handleGoto, err := decider.GotoOf(testCase.handleGoto); err == nil {
			if handleGoto == decider.GotoTransfer {
				return handler.StatusTransfer.WithTopic(testCase.transferToTopic)
			}
			if status, err := handler.StatusOf(testCase.handleGoto); err == nil {
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
	stats, err = manager.Stats(pTopics[0])
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// Handle transferred stats
	if len(cTopics) > 0 {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		stats, err = manager.Stats(cTopics[0])
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, testCase.expectedTransferredOutCount, stats.MsgOutCounter)
		if testCase.handleGoto == decider.GotoPending.String() ||
			testCase.handleGoto == decider.GotoBlocking.String() ||
			testCase.handleGoto == decider.GotoRetrying.String() {
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
	transferTopic := groundTopic + "-OTHER"
	cTopics := []string{
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
		cTopics[3],
		cTopics[4],
		cTopics[5],
	}
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	// clean up topics
	topics := make([]string, 0)
	topics = append(topics, groundTopic)
	topics = append(topics, cTopics...)
	internal.CleanUpTopics(t, manager, topics...)
	defer internal.CleanUpTopics(t, manager, topics...)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, topics, 0)
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
	for i := 0; i < len(cTopics); i++ {
		msg := internal.GenerateProduceMessage(internal.Size64, "Index", strconv.Itoa(i))
		mid, err := producer.Send(context.Background(), msg)
		assert.Nil(t, err)
		fmt.Println("sent message: ", mid)
	}
	// check send stats
	stats, err := manager.Stats(groundTopic)
	assert.Nil(t, err)
	assert.Equal(t, len(cTopics), stats.MsgInCounter)
	assert.Equal(t, 0, stats.MsgOutCounter)

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: config.ToPointer(uint(1)),
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
	assert.Equal(t, len(cTopics), stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// check decided stats
	for _, decidedTopic := range cTopics {
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
