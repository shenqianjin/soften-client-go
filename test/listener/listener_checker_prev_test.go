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
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testListenCheckCase struct {
	groundTopic               string
	storedTopic               string // produce to / consume from
	decidedTopic              string // check to
	checkpoint                checker.ConsumeCheckpoint
	expectedStoredOutCount    int // should always 1
	expectedTransferdOutCount int // 1 for pending, blocking, retrying; 0 for upgrade, degrade, transfer

	// extra for upgrade/degrade
	upgradeLevel string
	degradeLevel string
}

func TestListenCheck_Prev_Discard(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic: topic,
		storedTopic: topic,
		checkpoint: checker.PrevHandleDiscard(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Dead(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:  topic,
		storedTopic:  topic,
		decidedTopic: internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusDead.TopicSuffix()),
		checkpoint: checker.PrevHandleDead(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Pending(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:  topic,
		storedTopic:  topic,
		decidedTopic: internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusPending.TopicSuffix()),
		checkpoint: checker.PrevHandlePending(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedTransferdOutCount: 1, // transfer the msg to pending queue, and then reconsume it
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Blocking(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:  topic,
		storedTopic:  topic,
		decidedTopic: internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusBlocking.TopicSuffix()),
		checkpoint: checker.PrevHandleBlocking(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedTransferdOutCount: 1,
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Retrying(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:  topic,
		storedTopic:  topic,
		decidedTopic: internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusRetrying.TopicSuffix()),
		checkpoint: checker.PrevHandleRetrying(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedTransferdOutCount: 1,
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Upgrade(t *testing.T) {
	upgradeLevel := message.L2
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:  topic,
		storedTopic:  topic,
		decidedTopic: topic + upgradeLevel.TopicSuffix(),
		upgradeLevel: upgradeLevel.String(),
		checkpoint: checker.PrevHandleUpgrade(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Degrade(t *testing.T) {
	degradeLevel := message.B2
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:  topic,
		storedTopic:  topic,
		decidedTopic: topic + degradeLevel.TopicSuffix(),
		degradeLevel: degradeLevel.String(),
		checkpoint: checker.PrevHandleDegrade(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Shift(t *testing.T) {
	shiftLevel := message.B2
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:  topic,
		storedTopic:  topic,
		decidedTopic: topic + shiftLevel.TopicSuffix(),
		checkpoint: checker.PrevHandleShift(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Level: shiftLevel})
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Transfer(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	transferredTopic := topic + message.L2.TopicSuffix()
	checkCase := testListenCheckCase{
		groundTopic:  topic,
		storedTopic:  topic,
		decidedTopic: transferredTopic,
		checkpoint: checker.PrevHandleTransfer(func(ctx context.Context, msg message.Message) checker.CheckStatus {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferredTopic})
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func testListenPrevCheckHandle(t *testing.T, checkCase testListenCheckCase) {
	topic := checkCase.groundTopic
	storedTopic := checkCase.storedTopic
	transferredTopic := checkCase.decidedTopic
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up groundTopic
	internal.CleanUpTopic(t, manager, storedTopic)
	internal.CleanUpTopic(t, manager, transferredTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, storedTopic)
		internal.CleanUpTopic(t, manager, transferredTopic)
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
	// check send stats
	stats, err := manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: 1,
	}
	upgradeLevel, _ := message.LevelOf(checkCase.upgradeLevel)
	degradeLevel, _ := message.LevelOf(checkCase.degradeLevel)
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:  checkCase.checkpoint.CheckType == checker.CheckTypePrevDiscard,
		DeadEnable:     checkCase.checkpoint.CheckType == checker.CheckTypePrevDead,
		PendingEnable:  checkCase.checkpoint.CheckType == checker.CheckTypePrevPending,
		Pending:        testPolicy,
		BlockingEnable: checkCase.checkpoint.CheckType == checker.CheckTypePrevBlocking,
		Blocking:       testPolicy,
		RetryingEnable: checkCase.checkpoint.CheckType == checker.CheckTypePrevRetrying,
		Retrying:       testPolicy,
		UpgradeEnable:  checkCase.checkpoint.CheckType == checker.CheckTypePrevUpgrade,
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		DegradeEnable:  checkCase.checkpoint.CheckType == checker.CheckTypePrevDegrade,
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		ShiftEnable:    checkCase.checkpoint.CheckType == checker.CheckTypePrevShift,
		Shift:          &config.ShiftPolicy{ConnectInSyncEnable: true},
		TransferEnable: checkCase.checkpoint.CheckType == checker.CheckTypePrevTransfer,
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: checkCase.checkpoint.CheckType == checker.CheckTypePrevTransfer},
	}
	// create listener
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
	}, checkCase.checkpoint)
	if err != nil {
		log.Fatal(err)
	}
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
	stats, err = manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// check transferred stats
	if transferredTopic != "" {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		stats, err = manager.Stats(transferredTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, checkCase.expectedTransferdOutCount, stats.MsgOutCounter)
		if checkCase.checkpoint.CheckType == checker.CheckTypePrevPending ||
			checkCase.checkpoint.CheckType == checker.CheckTypePrevBlocking ||
			checkCase.checkpoint.CheckType == checker.CheckTypePrevRetrying {
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 1, v.MsgBacklog)
				break
			}
		}
	}
	// stop listener
	cancel()
}

func TestListenCheck_Prev_All(t *testing.T) {
	upgradeLevel := message.L2
	degradeLevel := message.B2
	shiftLevel := message.S1
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	statusTopicPrefix := internal.FormatStatusTopic(groundTopic, internal.TestSubscriptionName(), "", "")
	transferredTopic := groundTopic + "-S2"
	decidedTopics := []string{
		"", // done
		"", // discard
		statusTopicPrefix + message.StatusDead.TopicSuffix(),     // dead
		statusTopicPrefix + message.StatusPending.TopicSuffix(),  // pending
		statusTopicPrefix + message.StatusBlocking.TopicSuffix(), // blocking
		statusTopicPrefix + message.StatusRetrying.TopicSuffix(), // retrying
		groundTopic + upgradeLevel.TopicSuffix(),                 // degrade
		groundTopic + degradeLevel.TopicSuffix(),                 // upgrade
		groundTopic + shiftLevel.TopicSuffix(),                   // shift
		transferredTopic,                                         // transfer
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
		DiscardEnable:  true,
		DeadEnable:     true,
		PendingEnable:  true,
		Pending:        testPolicy,
		BlockingEnable: true,
		Blocking:       testPolicy,
		RetryingEnable: true,
		Retrying:       testPolicy,
		UpgradeEnable:  true,
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		DegradeEnable:  true,
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		ShiftEnable:    true,
		Shift:          &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
		TransferEnable: true,
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
	}
	// create listener
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       groundTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
	}, checker.PrevHandleDiscard(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "1" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleDead(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "2" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandlePending(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "3" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleBlocking(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "4" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleRetrying(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "5" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleUpgrade(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "6" {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleDegrade(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "7" {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleShift(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "8" {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleTransfer(func(ctx context.Context, msg message.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "9" {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferredTopic})
			}
		}
		return checker.CheckStatusRejected
	}))
	if err != nil {
		log.Fatal(err)
	}
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
