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
	topiclevel "github.com/shenqianjin/soften-client-go/soften/topic"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testListenCheckCase struct {
	topic                  string
	storedTopic            string // produce to / consume from
	routedTopic            string // check to
	checkpoint             checker.ConsumeCheckpoint
	expectedStoredOutCount int // should always 1
	expectedRoutedOutCount int // 1 for pending, blocking, retrying; 0 for upgrade, degrade, reroute

	// extra for upgrade/degrade
	upgradeLevel string
	degradeLevel string
}

func TestListenCheck_Prev_Discard(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testListenCheckCase{
		topic:       topic,
		storedTopic: topic,
		checkpoint: checker.PrevHandleDiscard(func(msg pulsar.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Dead(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testListenCheckCase{
		topic:       topic,
		storedTopic: topic,
		routedTopic: topic + message.StatusDead.TopicSuffix(),
		checkpoint: checker.PrevHandleDead(func(msg pulsar.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Pending(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testListenCheckCase{
		topic:       topic,
		storedTopic: topic,
		routedTopic: topic + message.StatusPending.TopicSuffix(),
		checkpoint: checker.PrevHandlePending(func(msg pulsar.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedRoutedOutCount: 1, // reroute the msg to pending queue, and then reconsume it
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Blocking(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testListenCheckCase{
		topic:       topic,
		storedTopic: topic,
		routedTopic: topic + message.StatusBlocking.TopicSuffix(),
		checkpoint: checker.PrevHandleBlocking(func(msg pulsar.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedRoutedOutCount: 1,
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Retrying(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testListenCheckCase{
		topic:       topic,
		storedTopic: topic,
		routedTopic: topic + message.StatusRetrying.TopicSuffix(),
		checkpoint: checker.PrevHandleRetrying(func(msg pulsar.Message) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
		expectedRoutedOutCount: 1,
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Upgrade(t *testing.T) {
	upgradeLevel := topiclevel.L2
	topic := internal.GenerateTestTopic()
	checkCase := testListenCheckCase{
		topic:        topic,
		storedTopic:  topic,
		routedTopic:  topic + upgradeLevel.TopicSuffix(),
		upgradeLevel: upgradeLevel.String(),
		checkpoint: checker.PrevHandleUpgrade(func(msg pulsar.Message) checker.CheckStatus {
			if consumerMsg, ok := msg.(pulsar.ConsumerMessage); ok && message.Parser.GetCurrentStatus(consumerMsg) == message.StatusReady {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Degrade(t *testing.T) {
	degradeLevel := topiclevel.B2
	topic := internal.GenerateTestTopic()
	checkCase := testListenCheckCase{
		topic:        topic,
		storedTopic:  topic,
		routedTopic:  topic + degradeLevel.TopicSuffix(),
		degradeLevel: degradeLevel.String(),
		checkpoint: checker.PrevHandleDegrade(func(msg pulsar.Message) checker.CheckStatus {
			if consumerMsg, ok := msg.(pulsar.ConsumerMessage); ok && message.Parser.GetCurrentStatus(consumerMsg) == message.StatusReady {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func TestListenCheck_Prev_Reroute(t *testing.T) {
	topic := internal.GenerateTestTopic()
	reroutedTopic := topic + topiclevel.L2.TopicSuffix()
	checkCase := testListenCheckCase{
		topic:       topic,
		storedTopic: topic,
		routedTopic: reroutedTopic,
		checkpoint: checker.PrevHandleReroute(func(msg pulsar.Message) checker.CheckStatus {
			if consumerMsg, ok := msg.(pulsar.ConsumerMessage); ok && message.Parser.GetCurrentStatus(consumerMsg) == message.StatusReady {
				return checker.CheckStatusPassed.WithRerouteTopic(reroutedTopic)
			}
			return checker.CheckStatusRejected
		}),
	}
	testListenPrevCheckHandle(t, checkCase)
}

func testListenPrevCheckHandle(t *testing.T, checkCase testListenCheckCase) {
	topic := checkCase.topic
	storedTopic := checkCase.storedTopic
	routedTopic := checkCase.routedTopic
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up topic
	internal.CleanUpTopic(t, manager, storedTopic)
	internal.CleanUpTopic(t, manager, routedTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, storedTopic)
		internal.CleanUpTopic(t, manager, routedTopic)
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

	// create listener
	upgradeLevel, _ := topiclevel.LevelOf(checkCase.upgradeLevel)
	degradeLevel, _ := topiclevel.LevelOf(checkCase.degradeLevel)
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.GenerateSubscribeNameByTopic(topic),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		DiscardEnable:               checkCase.checkpoint.CheckType == checker.CheckTypePrevDiscard,
		DeadEnable:                  checkCase.checkpoint.CheckType == checker.CheckTypePrevDead,
		PendingEnable:               checkCase.checkpoint.CheckType == checker.CheckTypePrevPending,
		BlockingEnable:              checkCase.checkpoint.CheckType == checker.CheckTypePrevBlocking,
		RetryingEnable:              checkCase.checkpoint.CheckType == checker.CheckTypePrevRetrying,
		UpgradeEnable:               checkCase.checkpoint.CheckType == checker.CheckTypePrevUpgrade,
		DegradeEnable:               checkCase.checkpoint.CheckType == checker.CheckTypePrevDegrade,
		RerouteEnable:               checkCase.checkpoint.CheckType == checker.CheckTypePrevReroute,
		UpgradeTopicLevel:           upgradeLevel,
		DegradeTopicLevel:           degradeLevel,
		Reroute: &config.ReroutePolicy{
			ConnectInSyncEnable: checkCase.checkpoint.CheckType == checker.CheckTypePrevReroute},
	}, checkCase.checkpoint)
	if err != nil {
		log.Fatal(err)
	}
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
	stats, err = manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// check rerouted stats
	if routedTopic != "" {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		stats, err = manager.Stats(routedTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, checkCase.expectedRoutedOutCount, stats.MsgOutCounter)
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
	upgradeLevel := topiclevel.L2
	degradeLevel := topiclevel.B2
	topic := internal.GenerateTestTopic()
	reroutedTopic := topic + "-S1"
	decidedTopics := []string{
		"",                                       // done
		"",                                       // discard
		topic + message.StatusDead.TopicSuffix(), // dead
		topic + message.StatusPending.TopicSuffix(),  // pending
		topic + message.StatusBlocking.TopicSuffix(), // blocking
		topic + message.StatusRetrying.TopicSuffix(), // retrying
		topic + upgradeLevel.TopicSuffix(),           // degrade
		topic + degradeLevel.TopicSuffix(),           // upgrade
		reroutedTopic,                                // reroute
	}
	midConsumedTopics := []string{
		decidedTopics[3],
		decidedTopics[4],
		decidedTopics[5],
	}
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up topic
	internal.CleanUpTopic(t, manager, topic)
	for _, decidedTopic := range decidedTopics {
		if decidedTopic != "" {
			internal.CleanUpTopic(t, manager, decidedTopic)
		}
	}
	defer func() {
		internal.CleanUpTopic(t, manager, topic)
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
		Topic: topic,
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
	stats, err := manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, len(decidedTopics), stats.MsgInCounter)
	assert.Equal(t, 0, stats.MsgOutCounter)

	// ---------------

	// create listener
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.GenerateSubscribeNameByTopic(topic),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		DiscardEnable:               true,
		DeadEnable:                  true,
		PendingEnable:               true,
		BlockingEnable:              true,
		RetryingEnable:              true,
		UpgradeEnable:               true,
		DegradeEnable:               true,
		RerouteEnable:               true,
		UpgradeTopicLevel:           upgradeLevel,
		DegradeTopicLevel:           degradeLevel,
		Reroute: &config.ReroutePolicy{
			ConnectInSyncEnable: true},
	}, checker.PrevHandleDiscard(func(msg pulsar.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "1" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleDead(func(msg pulsar.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "2" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandlePending(func(msg pulsar.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "3" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleBlocking(func(msg pulsar.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "4" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleRetrying(func(msg pulsar.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "5" {
			return checker.CheckStatusPassed
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleUpgrade(func(msg pulsar.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "6" {
			if consumerMsg, ok := msg.(pulsar.ConsumerMessage); ok && message.Parser.GetCurrentStatus(consumerMsg) == message.StatusReady {
				return checker.CheckStatusPassed
			}
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleDegrade(func(msg pulsar.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "7" {
			if consumerMsg, ok := msg.(pulsar.ConsumerMessage); ok && message.Parser.GetCurrentStatus(consumerMsg) == message.StatusReady {
				return checker.CheckStatusPassed
			}
		}
		return checker.CheckStatusRejected
	}), checker.PrevHandleReroute(func(msg pulsar.Message) checker.CheckStatus {
		if index, ok := msg.Properties()["Index"]; ok && index == "8" {
			if consumerMsg, ok := msg.(pulsar.ConsumerMessage); ok && message.Parser.GetCurrentStatus(consumerMsg) == message.StatusReady {
				return checker.CheckStatusPassed.WithRerouteTopic(reroutedTopic)
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
	err = listener.Start(ctx, func(message pulsar.Message) (bool, error) {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(message.Payload()), message.Properties())
		return true, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message and wait for decide the message
	time.Sleep(500 * time.Millisecond)
	// check stats
	stats, err = manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, len(decidedTopics), stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// check decided stats
	for _, decidedTopic := range decidedTopics {
		if decidedTopic == "" {
			continue
		}
		stats, err = manager.Stats(decidedTopic)
		assert.Nil(t, err, "decided topic: ", decidedTopic)
		assert.Equal(t, 1, stats.MsgInCounter, "decided topic: ", decidedTopic)
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
				assert.Equal(t, 1, v.MsgBacklog, "decided topic: ", decidedTopic)
				break
			}
		} else {
			assert.Equal(t, 0, stats.MsgOutCounter)
		}
	}
	// stop listener
	cancel()
}
