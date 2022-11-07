package listener

import (
	"context"
	"fmt"
	"log"
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

type testListenDecideCase struct {
	groundTopic              string
	consumeToLevel           string
	consumeToStatus          string
	expectedStoredOutCount   int // should always 1
	expectedTransferOutCount int // 1 for pending, blocking, retrying; 0 for upgrade, degrade, transfer
	handleGoto               string
	consumeTime              time.Time

	// extra for upgrade/degrade/transfer
	upgradeLevel    string
	degradeLevel    string
	transferToTopic string
}

func TestListenDecide_Pending(t *testing.T) {
	status := message.StatusPending
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		groundTopic:              topic,
		consumeToStatus:          status.String(),
		handleGoto:               decider.GotoPending.String(),
		expectedTransferOutCount: 1, // transfer the msg to pending queue, and then reconsume it
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Blocking(t *testing.T) {
	status := message.StatusBlocking
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		groundTopic:              topic,
		consumeToStatus:          status.String(),
		handleGoto:               decider.GotoBlocking.String(),
		expectedTransferOutCount: 1,
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Retrying(t *testing.T) {
	status := message.StatusRetrying
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		groundTopic:              groundTopic,
		consumeToStatus:          status.String(),
		handleGoto:               decider.GotoRetrying.String(),
		expectedTransferOutCount: 1,
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Upgrade(t *testing.T) {
	upgradeLevel := message.L2
	groudTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		groundTopic:              groudTopic,
		consumeToLevel:           upgradeLevel.String(),
		upgradeLevel:             upgradeLevel.String(),
		handleGoto:               decider.GotoUpgrade.String(),
		consumeTime:              time.Now().Add(time.Second * 5),
		expectedTransferOutCount: 1,
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Degrade(t *testing.T) {
	degradeLevel := message.B2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		groundTopic:              groundTopic,
		consumeToLevel:           degradeLevel.String(),
		degradeLevel:             degradeLevel.String(),
		handleGoto:               decider.GotoDegrade.String(),
		consumeTime:              time.Now().Add(time.Second * 5),
		expectedTransferOutCount: 1,
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Transfer(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	transferToTopic := groundTopic + "-OTHER"
	decideCase := testListenDecideCase{
		groundTopic:              groundTopic,
		transferToTopic:          transferToTopic,
		handleGoto:               decider.GotoTransfer.String(),
		consumeTime:              time.Now().Add(time.Second * 5),
		expectedTransferOutCount: 1,
	}
	testListenDecide(t, decideCase)
}

func testListenDecide(t *testing.T, testCase testListenDecideCase) {
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
	msg := internal.GenerateProduceMessage(internal.Size1K)
	msg.EventTime = time.Now().Add(-time.Minute)
	msgID, err := producer.Send(context.Background(), msg)
	assert.Nil(t, err)
	fmt.Println("produced message: ", msgID)
	// Handle send stats
	stats, err := manager.Stats(groundTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: config.ToPointer(uint(1)),
	}
	// create listener
	upgradeLevel, _ := message.LevelOf(testCase.upgradeLevel)
	degradeLevel, _ := message.LevelOf(testCase.degradeLevel)
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:  config.ToPointer(testCase.handleGoto == decider.GotoDiscard.String()),
		DeadEnable:     config.ToPointer(testCase.handleGoto == decider.GotoDead.String()),
		PendingEnable:  config.ToPointer(testCase.handleGoto == decider.GotoPending.String()),
		Pending:        testPolicy,
		BlockingEnable: config.ToPointer(testCase.handleGoto == decider.GotoBlocking.String()),
		Blocking:       testPolicy,
		RetryingEnable: config.ToPointer(testCase.handleGoto == decider.GotoRetrying.String()),
		Retrying:       testPolicy,
		UpgradeEnable:  config.ToPointer(testCase.handleGoto == decider.GotoUpgrade.String()),
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		DegradeEnable:  config.ToPointer(testCase.handleGoto == decider.GotoDegrade.String()),
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		TransferEnable: config.ToPointer(testCase.handleGoto == decider.GotoTransfer.String()),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: testCase.handleGoto == decider.GotoTransfer.String()},
	}
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       groundTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Type:                        pulsar.Shared,
		LevelPolicy:                 leveledPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// decided listener
	decidedListener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       cTopics[0],
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Type:                        pulsar.Shared,
		LevelPolicy:                 &config.LevelPolicy{DeadEnable: config.False()},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer decidedListener.Close()

	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, func(ctx context.Context, msg message.Message) handler.HandleStatus {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		if handleGoto, err1 := decider.GotoOf(testCase.handleGoto); err1 == nil {
			if handleGoto == decider.GotoTransfer {
				return handler.StatusTransfer.WithTopic(testCase.transferToTopic).WithConsumeTime(testCase.consumeTime)
			} else if handleGoto == decider.GotoUpgrade {
				return handler.StatusUpgrade.WithConsumeTime(testCase.consumeTime)
			} else if handleGoto == decider.GotoDegrade {
				return handler.StatusDegrade.WithConsumeTime(testCase.consumeTime)
			} else if handleGoto == decider.GotoRetrying {
				return handler.StatusRetrying.WithConsumeTime(testCase.consumeTime)
			} else if handleGoto == decider.GotoPending {
				return handler.StatusPending.WithConsumeTime(testCase.consumeTime)
			} else if handleGoto == decider.GotoBlocking {
				return handler.StatusBlocking.WithConsumeTime(testCase.consumeTime)
			}
		}
		return handler.StatusAuto
	})
	if err != nil {
		log.Fatal(err)
	}
	// decided listener starts
	err = decidedListener.Start(ctx, func(ctx context.Context, msg message.Message) (success bool, err error) {
		if !testCase.consumeTime.IsZero() {
			fmt.Println("********  now: ", time.Now(), testCase.consumeTime, msg.EventTime())
			fmt.Println("******** ", time.Now().Sub(testCase.consumeTime), msg.EventTime())
			assert.Equal(t, true, time.Now().Second() <= testCase.consumeTime.Second())
		}
		return true, nil
	})
	// wait for consuming the message
	time.Sleep(100 * time.Millisecond)
	// Handle stats
	stats, err = manager.Stats(groundTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// Handle transferred stats
	if cTopics[0] != "" {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		if !testCase.consumeTime.IsZero() {
			time.Sleep(testCase.consumeTime.Sub(time.Now()))
		}
		stats, err = manager.Stats(cTopics[0])
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, testCase.expectedTransferOutCount, stats.MsgOutCounter)
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
