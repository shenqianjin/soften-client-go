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
	consumeLevels   []string // consume level
	consumeStatuses []string // consume status
	requireGotos    []string // require gotos for consume enable

	handleStatus         handler.HandleStatus
	handleGotoFinalTopic string
	consumeMaxTimes      uint

	interceptGoto          string
	interceptFunc          func(ctx context.Context, message message.Message, decision decider.Decision)
	expectedInterceptCount uint32

	// extra for upgrade/degrade/transfer
	upgradeLevel    string
	degradeLevel    string
	transferToTopic string
}

func TestConsumeIntercept_OnDecideToDead(t *testing.T) {
	handleStatus := handler.StatusRetrying.WithErr(fmt.Errorf("internal server error"))
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeIntercept)
	HandleCase := testListenInterceptCase{
		groundTopic:     topic,
		requireLevels:   []string{message.L1.String()},
		requireStatuses: []string{message.StatusReady.String(), message.StatusRetrying.String(), message.StatusDead.String()},
		produceLevel:    message.L1.String(),
		produceStatus:   message.StatusReady.String(),
		consumeLevels:   []string{message.L1.String()},
		consumeStatuses: []string{message.StatusReady.String(), message.StatusRetrying.String()},
		requireGotos:    []string{message.StatusRetrying.String(), message.StatusDead.String()},
		handleStatus:    handleStatus.WithErr(fmt.Errorf("test error")),

		consumeMaxTimes:        3,
		expectedInterceptCount: 1,
		interceptGoto:          message.StatusDead.String(),
		interceptFunc: func(ctx context.Context, msg message.Message, decision decider.Decision) {
			fmt.Printf("intercepted on decide to dead. msgID: %v, headers: %v, goto: %v, err: %v\n",
				msg.ID(), msg.Properties(), decision.GetGoto(), decision.GetErr())
		},
	}
	testListenIntercept(t, HandleCase)
}

func TestConsumeIntercept_OnDecideToDiscard(t *testing.T) {
	handleStatus := handler.StatusDiscard
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeIntercept)
	HandleCase := testListenInterceptCase{
		groundTopic:     topic,
		requireLevels:   []string{message.L1.String()},
		requireStatuses: []string{message.StatusReady.String()},
		produceLevel:    message.L1.String(),
		produceStatus:   message.StatusReady.String(),
		consumeLevels:   []string{message.L1.String()},
		consumeStatuses: []string{message.StatusReady.String()},
		requireGotos:    []string{message.StatusDiscard.String()},
		handleStatus:    handleStatus.WithErr(fmt.Errorf("test goto discard error")),

		consumeMaxTimes:        3,
		expectedInterceptCount: 1,
		interceptGoto:          message.StatusDiscard.String(),
		interceptFunc: func(ctx context.Context, msg message.Message, decision decider.Decision) {
			fmt.Printf("intercepted on decide to dead. msgID: %v, headers: %v, goto: %v, err: %v\n",
				msg.ID(), msg.Properties(), decision.GetGoto(), decision.GetErr())
		},
	}
	testListenIntercept(t, HandleCase)
}

func TestConsumeIntercept_OnDecideToDone(t *testing.T) {
	handleStatus := handler.StatusDone
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeIntercept)
	HandleCase := testListenInterceptCase{
		groundTopic:     topic,
		requireLevels:   []string{message.L1.String()},
		requireStatuses: []string{message.StatusReady.String()},
		produceLevel:    message.L1.String(),
		produceStatus:   message.StatusReady.String(),
		consumeLevels:   []string{message.L1.String()},
		consumeStatuses: []string{message.StatusReady.String()},
		requireGotos:    []string{message.StatusDone.String()},
		handleStatus:    handleStatus,

		consumeMaxTimes:        3,
		expectedInterceptCount: 1,
		interceptGoto:          message.StatusDone.String(),
		interceptFunc: func(ctx context.Context, msg message.Message, decision decider.Decision) {
			fmt.Printf("intercepted on decide to dead. msgID: %v, headers: %v, goto: %v, err: %v\n",
				msg.ID(), msg.Properties(), decision.GetGoto(), decision.GetErr())
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
	var interceptorError error
	consumeInterceptors := interceptor.ConsumeInterceptors{
		interceptor.NewDecideInterceptor(func(ctx context.Context, msg message.Message, decision decider.Decision) {
			if decision.GetGoto().String() == testCase.interceptGoto {
				testCase.interceptFunc(ctx, msg, decision)
				atomic.AddUint32(&interceptorCount, 1)
				interceptorError = decision.GetErr()
			}
		}),
	}
	// create listener
	upgradeLevel, _ := message.LevelOf(testCase.upgradeLevel)
	degradeLevel, _ := message.LevelOf(testCase.degradeLevel)
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:       config.ToPointer(slices.Contains(testCase.requireGotos, handler.StatusDiscard.GetGoto().String())),
		DeadEnable:          config.ToPointer(slices.Contains(testCase.requireGotos, handler.StatusDead.GetGoto().String())),
		PendingEnable:       config.ToPointer(slices.Contains(testCase.requireGotos, handler.StatusPending.GetGoto().String())),
		Pending:             testPolicy,
		BlockingEnable:      config.ToPointer(slices.Contains(testCase.requireGotos, handler.StatusBlocking.GetGoto().String())),
		Blocking:            testPolicy,
		RetryingEnable:      config.ToPointer(slices.Contains(testCase.requireGotos, handler.StatusRetrying.GetGoto().String())),
		Retrying:            testPolicy,
		UpgradeEnable:       config.ToPointer(slices.Contains(testCase.requireGotos, handler.StatusUpgrade.GetGoto().String())),
		Upgrade:             &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		DegradeEnable:       config.ToPointer(slices.Contains(testCase.requireGotos, handler.StatusDegrade.GetGoto().String())),
		Degrade:             &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		TransferEnable:      config.ToPointer(slices.Contains(testCase.requireGotos, handler.StatusTransfer.GetGoto().String())),
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
		handleStatus := testCase.handleStatus
		if handleStatus.GetGoto() == handler.StatusTransfer.GetGoto() {
			return handler.StatusTransfer.WithTopic(testCase.transferToTopic)
		} else {
			return handleStatus
		}
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
	assert.Equal(t, testCase.handleStatus.GetErr(), interceptorError)
	cancel()
}
