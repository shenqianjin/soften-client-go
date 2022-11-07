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
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestListenCheck_Post_Discard(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic: groundTopic,
		checkpoint: checker.PostHandleDiscard(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testListenCheckPostHandle(t, checkCase)
}

func TestListenCheck_Post_Dead(t *testing.T) {
	status := message.StatusDead
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:     groundTopic,
		consumeToStatus: status.String(),
		checkpoint: checker.PostHandleDead(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testListenCheckPostHandle(t, checkCase)
}

func TestListenCheck_Post_Pending(t *testing.T) {
	status := message.StatusPending
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:     groundTopic,
		consumeToStatus: status.String(),
		checkpoint: checker.PostHandlePending(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedTransferredOutCount: 1, // transfer the msg to pending queue, and then reconsume it
	}
	testListenCheckPostHandle(t, checkCase)
}

func TestListenCheck_Post_Blocking(t *testing.T) {
	status := message.StatusBlocking
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:     groundTopic,
		consumeToStatus: status.String(),
		checkpoint: checker.PostHandleBlocking(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedTransferredOutCount: 1,
	}
	testListenCheckPostHandle(t, checkCase)
}

func TestListenCheck_Post_Retrying(t *testing.T) {
	status := message.StatusRetrying
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:     groundTopic,
		consumeToStatus: status.String(),
		checkpoint: checker.PostHandleRetrying(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedTransferredOutCount: 1,
	}
	testListenCheckPostHandle(t, checkCase)
}

func TestListenCheck_Post_Upgrade(t *testing.T) {
	upgradeLevel := message.L2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:    groundTopic,
		consumeToLevel: upgradeLevel.String(),
		upgradeLevel:   upgradeLevel.String(),
		checkpoint: checker.PostHandleUpgrade(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenCheckPostHandle(t, checkCase)
}

func TestListenCheck_Post_Degrade(t *testing.T) {
	degradeLevel := message.B2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:    groundTopic,
		consumeToLevel: degradeLevel.String(),
		degradeLevel:   degradeLevel.String(),
		checkpoint: checker.PostHandleDegrade(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenCheckPostHandle(t, checkCase)
}

func TestListenCheck_Post_Shift(t *testing.T) {
	shiftLevel := message.L2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	checkCase := testListenCheckCase{
		groundTopic:    groundTopic,
		consumeToLevel: shiftLevel.String(),
		checkpoint: checker.PostHandleShift(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Level: shiftLevel})
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenCheckPostHandle(t, checkCase)
}

func TestListenCheck_Post_Transfer(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestListen)
	transferToTopic := groundTopic + "-OTHER"
	checkCase := testListenCheckCase{
		groundTopic:     groundTopic,
		transferToTopic: transferToTopic,
		checkpoint: checker.PostHandleTransfer(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferToTopic})
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenCheckPostHandle(t, checkCase)
}

func testListenCheckPostHandle(t *testing.T, testCase testListenCheckCase) {
	groundTopic := testCase.groundTopic
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	// format topics
	pTopics := make([]string, 0)
	pTopics = append(pTopics, testCase.groundTopic)
	cTopics := make([]string, 0)
	if testCase.checkpoint.CheckType == checker.CheckTypePostTransfer {
		cTopics = append(cTopics, testCase.transferToTopic)
	} else if testCase.checkpoint.CheckType == checker.CheckTypePrevDiscard {
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
	// check send stats
	stats, err := manager.Stats(pTopics[0])
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
		DiscardEnable:  config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostDiscard),
		DeadEnable:     config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostDead),
		PendingEnable:  config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostPending),
		Pending:        testPolicy,
		BlockingEnable: config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostBlocking),
		Blocking:       testPolicy,
		RetryingEnable: config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostRetrying),
		Retrying:       testPolicy,
		UpgradeEnable:  config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostUpgrade),
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		DegradeEnable:  config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostDegrade),
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		ShiftEnable:    config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostShift),
		Shift:          &config.ShiftPolicy{ConnectInSyncEnable: true},
		TransferEnable: config.ToPointer(testCase.checkpoint.CheckType == checker.CheckTypePostTransfer),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
	}
	// create listener
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       groundTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
	}, testCase.checkpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.Start(ctx, func(ctx context.Context, msg message.Message) (bool, error) {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		return false, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(100 * time.Millisecond)
	// check stats
	stats, err = manager.Stats(pTopics[0])
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// check transferred stats
	if len(cTopics) > 0 {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		stats, err = manager.Stats(cTopics[0])
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, testCase.expectedTransferredOutCount, stats.MsgOutCounter)
		if testCase.checkpoint.CheckType == checker.CheckTypePostPending ||
			testCase.checkpoint.CheckType == checker.CheckTypePostBlocking ||
			testCase.checkpoint.CheckType == checker.CheckTypePostRetrying {
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 1, v.MsgBacklog)
				break
			}
		}
	}
	// stop listener
	cancel()
}

func TestListenCheck_Post_All(t *testing.T) {
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
		groundTopic + upgradeLevel.TopicSuffix(),                 // degrade
		groundTopic + degradeLevel.TopicSuffix(),                 // upgrade
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
	}, checker.PostHandleDiscard(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "1" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PostHandleDead(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "2" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PostHandlePending(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "3" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PostHandleBlocking(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "4" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PostHandleRetrying(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "5" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PostHandleUpgrade(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "6" {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
		}
		return checker.CheckStatusRejected
	}), checker.PostHandleDegrade(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "7" {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
		}
		return checker.CheckStatusRejected
	}), checker.PostHandleShift(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "8" {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed
			}
		}
		return checker.CheckStatusRejected
	}), checker.PostHandleTransfer(func(ctx context.Context, msg message.Message, err error) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "9" {
			if msg.Status() == message.StatusReady {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferTopic})
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
		if index, ok := msg.Properties()["Index"]; ok && index != "0" {
			return false, nil
		}
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
