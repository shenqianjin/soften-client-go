package producer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
	topiclevel "github.com/shenqianjin/soften-client-go/soften/topic"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testProduceCheckCase struct {
	topic               string
	storedTopic         string // for check stats
	checkpoint          checker.ProduceCheckpoint
	expectedStoredCount int

	// extra for upgrade/degrade
	upgradeLevel string
	degradeLevel string
}

func TestProduceCheck_Discard_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testProduceCheckCase{
		topic:               topic,
		expectedStoredCount: 0,
		checkpoint: checker.PrevSendDiscard(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Dead_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + message.StatusDead.TopicSuffix(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendDead(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Pending_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + message.StatusPending.TopicSuffix(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendPending(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Blocking_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + message.StatusBlocking.TopicSuffix(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendBlocking(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Retrying_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic()
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + message.StatusRetrying.TopicSuffix(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendRetrying(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Upgrade_BySend(t *testing.T) {
	upgradeLevel := topiclevel.L2
	topic := internal.GenerateTestTopic()
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + upgradeLevel.TopicSuffix(),
		upgradeLevel:        string(upgradeLevel),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendUpgrade(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Degrade_BySend(t *testing.T) {
	degradeLevel := topiclevel.B2
	topic := internal.GenerateTestTopic()
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + degradeLevel.TopicSuffix(),
		degradeLevel:        string(degradeLevel),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendDegrade(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_RouteToL2_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic()
	routedTopic := topic + "-L2"
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         routedTopic,
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendRoute(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed.WithRerouteTopic(routedTopic)
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func testProduceCheckBySend(t *testing.T, checkCase testProduceCheckCase) {
	if checkCase.storedTopic == "" {
		checkCase.storedTopic = checkCase.topic
	}
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)

	internal.CleanUpTopic(t, manager, checkCase.storedTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, checkCase.storedTopic)
	}()

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	upgradeLevel, _ := topiclevel.LevelOf(checkCase.upgradeLevel)
	degradeLevel, _ := topiclevel.LevelOf(checkCase.degradeLevel)
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:             checkCase.topic,
		RouteEnable:       checkCase.checkpoint.CheckType == checker.ProduceCheckTypeRoute,
		DiscardEnable:     checkCase.checkpoint.CheckType == checker.ProduceCheckTypeDiscard,
		DeadEnable:        checkCase.checkpoint.CheckType == checker.ProduceCheckTypeDead,
		PendingEnable:     checkCase.checkpoint.CheckType == checker.ProduceCheckTypePending,
		BlockingEnable:    checkCase.checkpoint.CheckType == checker.ProduceCheckTypeBlocking,
		RetryingEnable:    checkCase.checkpoint.CheckType == checker.ProduceCheckTypeRetrying,
		UpgradeEnable:     checkCase.checkpoint.CheckType == checker.ProduceCheckTypeUpgrade,
		DegradeEnable:     checkCase.checkpoint.CheckType == checker.ProduceCheckTypeDegrade,
		Route:             &config.RoutePolicy{ConnectInSyncEnable: true},
		UpgradeTopicLevel: upgradeLevel,
		DegradeTopicLevel: degradeLevel,
	}, checkCase.checkpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	msg := internal.GenerateProduceMessage(internal.Size64)
	msgID, err := producer.Send(context.Background(), msg)
	assert.Nil(t, err)
	fmt.Println(msgID)

	stats, err := manager.Stats(checkCase.storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, checkCase.expectedStoredCount, stats.MsgInCounter)
}

func TestProduceCheck_All_BySend(t *testing.T) {
	upgradeLevel := topiclevel.L2
	degradeLevel := topiclevel.B2
	topic := internal.GenerateTestTopic()
	routedTopic := topic + "-S1"
	storedTopics := []string{
		topic, // ready
		"",    // discard
		topic + message.StatusDead.TopicSuffix(),
		topic + message.StatusPending.TopicSuffix(),
		topic + message.StatusBlocking.TopicSuffix(),
		topic + message.StatusRetrying.TopicSuffix(),
		topic + upgradeLevel.TopicSuffix(),
		topic + degradeLevel.TopicSuffix(),
		routedTopic,
	}
	checkpoints := []checker.ProduceCheckpoint{
		// 0 为 ready
		checker.PrevSendDiscard(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "1" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		checker.PrevSendDead(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "2" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		checker.PrevSendPending(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "3" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		checker.PrevSendBlocking(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "4" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		checker.PrevSendRetrying(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "5" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		checker.PrevSendUpgrade(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "6" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		checker.PrevSendDegrade(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "7" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		checker.PrevSendRoute(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "8" {
				return checker.CheckStatusPassed.WithRerouteTopic(routedTopic)
			}
			return checker.CheckStatusRejected
		}),
	}
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)

	for _, storedTopic := range storedTopics {
		if storedTopic == "" { // discard
			continue
		}
		internal.CleanUpTopic(t, manager, storedTopic)
	}
	defer func() {
		for _, storedTopic := range storedTopics {
			if storedTopic == "" { // discard
				continue
			}
			internal.CleanUpTopic(t, manager, storedTopic)
		}
	}()

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:             topic,
		RouteEnable:       true,
		DiscardEnable:     true,
		DeadEnable:        true,
		PendingEnable:     true,
		BlockingEnable:    true,
		RetryingEnable:    true,
		UpgradeEnable:     true,
		DegradeEnable:     true,
		Route:             &config.RoutePolicy{ConnectInSyncEnable: true},
		UpgradeTopicLevel: upgradeLevel,
		DegradeTopicLevel: degradeLevel,
	}, checkpoints...)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 9; i++ {
		msg := internal.GenerateProduceMessage(internal.Size64, "Index", strconv.Itoa(i))
		mid, err := producer.Send(context.Background(), msg)
		assert.Nil(t, err)
		fmt.Println("sent message: ", mid)
	}

	for index, storedTopic := range storedTopics {
		expected := 1
		if storedTopic == "" { // discard
			expected = 1 // 发送discard消息之前, 发了一个ready消息, 所以这里依然expected 1
			storedTopic = topic
		}
		stats, err := manager.Stats(storedTopic)
		assert.Nil(t, err)
		assert.Equal(t, expected, stats.MsgInCounter, fmt.Sprintf("failed to validate stats for the %v topic: %v", index, storedTopic))
	}
}
