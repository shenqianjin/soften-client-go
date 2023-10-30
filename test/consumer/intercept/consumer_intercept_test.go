package intercept

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/interceptor"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

type testListenInterceptCase struct {
	groundTopic     string
	requireLevels   []string // require levels for topic creation
	requireStatuses []string // require statuses for topic creation
	produceLevel    string   // produce level: default L1
	produceStatus   string   // produce status: default Ready
	consumeLevels   []string
	consumeStatuses []string
	requireGotos    []string // require gotos for consume enable

	handleGoto           string
	handleGotoFinalTopic string
	consumeMaxTimes      uint

	interceptFunc          func(ctx context.Context, message message.Message)
	expectedInterceptCount uint32

	// extra for upgrade/degrade/transfer
	upgradeLevel    string
	degradeLevel    string
	transferToTopic string
}

func TestConsumeIntercept_OnDecideToDead(t *testing.T) {
	handleStatus := message.StatusRetrying
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeIntercept)
	HandleCase := testListenInterceptCase{
		groundTopic:     topic,
		requireLevels:   []string{message.L1.String()},
		requireStatuses: []string{message.StatusReady.String(), handleStatus.String(), decider.GotoDead.String()},
		produceLevel:    message.L1.String(),
		produceStatus:   message.StatusReady.String(),
		consumeLevels:   []string{message.L1.String()},
		consumeStatuses: []string{message.StatusReady.String(), handleStatus.String()},
		requireGotos:    []string{decider.GotoRetrying.String(), decider.GotoDead.String()},
		handleGoto:      handleStatus.String(),

		consumeMaxTimes:        3,
		expectedInterceptCount: 1,
		interceptFunc: func(ctx context.Context, msg message.Message) {
			fmt.Printf("intercepted on decide to dead. msgID: %v, headers: %v\n", msg.ID(), msg.Properties())
		},
	}
	testListenIntercept(t, HandleCase)
}

func testListenIntercept(t *testing.T, testCase testListenInterceptCase) {
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	// format topics
	groundTopic := testCase.groundTopic
	requireLevels := internal.FormatLevels(testCase.requireLevels...)
	requireStatuses := internal.FormatStatuses(testCase.requireStatuses...)
	topics, err := util.FormatTopics(testCase.groundTopic, requireLevels, requireStatuses, internal.TestSubscriptionName())
	assert.Nil(t, err)
	// clean up topic
	internal.CleanUpTopics(t, manager, topics...)
	defer internal.CleanUpTopics(t, manager, topics...)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, topics, 0)

	// format produce topics
	pLevels := internal.FormatLevels(testCase.produceLevel)
	pStatuses := internal.FormatStatuses(testCase.produceStatus)
	pTopics, err := util.FormatTopics(testCase.groundTopic, pLevels, pStatuses, internal.TestSubscriptionName())
	assert.Nil(t, err)
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic: pTopics[0],
	})
	assert.Nil(t, err)
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
		BackoffDelays:   []string{"1s"},
		ReentrantDelay:  config.ToPointer(uint(1)),
		ConsumeMaxTimes: config.ToPointer(testCase.consumeMaxTimes),
	}
	var interceptorCount uint32
	consumeInterceptors := interceptor.ConsumeInterceptors{
		interceptor.NewDecideInterceptorBuilder().OnDecideToDead(func(ctx context.Context, msg message.Message) {
			testCase.interceptFunc(ctx, msg)
			atomic.AddUint32(&interceptorCount, 1)
		}).Build(),
	}
	// create listener
	upgradeLevel, _ := message.LevelOf(testCase.upgradeLevel)
	degradeLevel, _ := message.LevelOf(testCase.degradeLevel)
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:       config.ToPointer(slices.Contains(testCase.requireGotos, decider.GotoDiscard.String())),
		DeadEnable:          config.ToPointer(slices.Contains(testCase.requireGotos, decider.GotoDead.String())),
		PendingEnable:       config.ToPointer(slices.Contains(testCase.requireGotos, decider.GotoPending.String())),
		Pending:             testPolicy,
		BlockingEnable:      config.ToPointer(slices.Contains(testCase.requireGotos, decider.GotoBlocking.String())),
		Blocking:            testPolicy,
		RetryingEnable:      config.ToPointer(slices.Contains(testCase.requireGotos, decider.GotoRetrying.String())),
		Retrying:            testPolicy,
		UpgradeEnable:       config.ToPointer(slices.Contains(testCase.requireGotos, decider.GotoUpgrade.String())),
		Upgrade:             &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		DegradeEnable:       config.ToPointer(slices.Contains(testCase.requireGotos, decider.GotoDegrade.String())),
		Degrade:             &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		TransferEnable:      config.ToPointer(slices.Contains(testCase.requireGotos, decider.GotoTransfer.String())),
		Transfer:            &config.TransferPolicy{ConnectInSyncEnable: true},
		ConsumeInterceptors: consumeInterceptors,
	}
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       groundTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Type:                        pulsar.Shared,
		LevelPolicy:                 leveledPolicy,
		Levels:                      internal.FormatLevels(testCase.consumeLevels...),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, func(ctx context.Context, msg message.Message) handler.HandleStatus {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		if handleGoto, err1 := decider.GotoOf(testCase.handleGoto); err1 == nil {
			if handleGoto == decider.GotoTransfer {
				return handler.StatusTransfer.WithTopic(testCase.transferToTopic)
			} else if handleGoto == decider.GotoUpgrade {
				return handler.StatusUpgrade
			} else if handleGoto == decider.GotoDegrade {
				return handler.StatusDegrade
			} else if handleGoto == decider.GotoRetrying {
				return handler.StatusRetrying
			} else if handleGoto == decider.GotoPending {
				return handler.StatusPending
			} else if handleGoto == decider.GotoBlocking {
				return handler.StatusBlocking
			}
		}
		return handler.StatusAuto
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(4000 * time.Millisecond)
	// Handle stats
	stats, err = manager.Stats(pTopics[0])
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// Handle transferred stats
	finalTopic := testCase.handleGotoFinalTopic
	if finalTopic != "" {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		stats, err = manager.Stats(finalTopic)
		assert.Nil(t, err)
		assert.Equal(t, testCase.consumeMaxTimes, stats.MsgInCounter)
		assert.Equal(t, 0, stats.MsgOutCounter)
		for _, v := range stats.Subscriptions {
			assert.Equal(t, 1, v.MsgBacklog)
			break
		}
	}
	// stop listener
	assert.Equal(t, testCase.expectedInterceptCount, interceptorCount)
	cancel()
}
