package producer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"

	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/message"
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
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		topic:               topic,
		expectedStoredCount: 0,
		checkpoint: checker.PrevSendDiscard(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Dead_BySend(t *testing.T) {
	deadLevel := message.D1
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + deadLevel.TopicSuffix(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendDead(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Level: deadLevel})
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

/*func TestProduceCheck_Pending_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + message.StatusPending.TopicSuffix(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendPending(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Blocking_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + message.StatusBlocking.TopicSuffix(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendBlocking(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Retrying_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + message.StatusRetrying.TopicSuffix(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendRetrying(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}*/

func TestProduceCheck_Upgrade_BySend(t *testing.T) {
	upgradeLevel := message.L2
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + upgradeLevel.TopicSuffix(),
		upgradeLevel:        string(upgradeLevel),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendUpgrade(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Degrade_BySend(t *testing.T) {
	degradeLevel := message.B2
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         topic + degradeLevel.TopicSuffix(),
		degradeLevel:        string(degradeLevel),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendDegrade(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Shift_BySend(t *testing.T) {
	shiftLevel := message.B2
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	transferredTopic := topic + shiftLevel.TopicSuffix()
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         transferredTopic,
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendShift(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Level: shiftLevel})
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_TransferToL2_BySend(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	transferredTopic := topic + "-L2"
	checkCase := testProduceCheckCase{
		topic:               topic,
		storedTopic:         transferredTopic,
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendTransfer(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferredTopic})
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
	upgradeLevel, _ := message.LevelOf(checkCase.upgradeLevel)
	degradeLevel, _ := message.LevelOf(checkCase.degradeLevel)
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:          checkCase.topic,
		DiscardEnable:  config.ToPointer(checkCase.checkpoint.CheckType == checker.ProduceCheckTypeDiscard),
		DeadEnable:     config.ToPointer(checkCase.checkpoint.CheckType == checker.ProduceCheckTypeDead),
		UpgradeEnable:  config.ToPointer(checkCase.checkpoint.CheckType == checker.ProduceCheckTypeUpgrade),
		DegradeEnable:  config.ToPointer(checkCase.checkpoint.CheckType == checker.ProduceCheckTypeDegrade),
		ShiftEnable:    config.ToPointer(checkCase.checkpoint.CheckType == checker.ProduceCheckTypeShift),
		TransferEnable: config.ToPointer(checkCase.checkpoint.CheckType == checker.ProduceCheckTypeTransfer),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		Shift:          &config.ShiftPolicy{ConnectInSyncEnable: true},
		Dead:           &config.ShiftPolicy{ConnectInSyncEnable: true},
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
	upgradeLevel := message.L2
	degradeLevel := message.B2
	shiftLevel := message.L3
	deadLevel := message.D1
	topic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	transferredTopic := topic + "-S1"
	storedTopics := []string{
		topic,                              // ready
		"",                                 // discard
		topic + deadLevel.TopicSuffix(),    // dead
		topic + upgradeLevel.TopicSuffix(), // upgrade
		topic + degradeLevel.TopicSuffix(), // degrade
		topic + shiftLevel.TopicSuffix(),   // shift
		transferredTopic,                   //transfer
	}
	checkpoints := []checker.ProduceCheckpoint{
		// 0 为 ready
		// discard
		checker.PrevSendDiscard(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "1" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		// dead
		checker.PrevSendDead(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "2" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		// upgrade
		checker.PrevSendUpgrade(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "3" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		// degrade
		checker.PrevSendDegrade(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "4" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		// shift
		checker.PrevSendShift(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "5" {
				return checker.CheckStatusPassed
			}
			return checker.CheckStatusRejected
		}),
		// transfer
		checker.PrevSendTransfer(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			if index, ok := msg.Properties["Index"]; ok && index == "6" {
				return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferredTopic})
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
		Topic:          topic,
		TransferEnable: config.True(),
		DiscardEnable:  config.True(),
		DeadEnable:     config.True(),
		UpgradeEnable:  config.True(),
		DegradeEnable:  config.True(),
		ShiftEnable:    config.True(),
		Dead:           &config.ShiftPolicy{Level: deadLevel, ConnectInSyncEnable: true},
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		Shift:          &config.ShiftPolicy{Level: shiftLevel, ConnectInSyncEnable: true},
	}, checkpoints...)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < len(storedTopics); i++ {
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
		fmt.Println(index, "---------------")
		assert.Equal(t, expected, stats.MsgInCounter, fmt.Sprintf("failed to validate stats for the %v topic: %v", index, storedTopic))
	}
}
