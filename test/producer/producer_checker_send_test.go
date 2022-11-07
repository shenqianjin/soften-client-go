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
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testProduceCheckCase struct {
	groundTopic         string
	produceToLevel      string
	checkpoint          checker.ProduceCheckpoint
	expectedStoredCount int

	// extra for upgrade/degrade/transfer
	upgradeLevel    string
	degradeLevel    string
	transferToTopic string
}

func TestProduceCheck_Discard_BySend(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		groundTopic:         groundTopic,
		expectedStoredCount: 0,
		checkpoint: checker.PrevSendDiscard(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Dead_BySend(t *testing.T) {
	deadLevel := message.D1
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		groundTopic:         groundTopic,
		produceToLevel:      deadLevel.String(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendDead(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Level: deadLevel})
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Upgrade_BySend(t *testing.T) {
	upgradeLevel := message.L2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		groundTopic:         groundTopic,
		produceToLevel:      upgradeLevel.String(),
		upgradeLevel:        upgradeLevel.String(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendUpgrade(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Degrade_BySend(t *testing.T) {
	degradeLevel := message.B2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		groundTopic:         groundTopic,
		produceToLevel:      degradeLevel.String(),
		degradeLevel:        degradeLevel.String(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendDegrade(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_Shift_BySend(t *testing.T) {
	shiftLevel := message.B2
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	checkCase := testProduceCheckCase{
		groundTopic:         groundTopic,
		produceToLevel:      shiftLevel.String(),
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendShift(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Level: shiftLevel})
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func TestProduceCheck_TransferToL2_BySend(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	transferToTopic := groundTopic + "-OTHER"
	checkCase := testProduceCheckCase{
		groundTopic:         groundTopic,
		transferToTopic:     transferToTopic,
		expectedStoredCount: 1,
		checkpoint: checker.PrevSendTransfer(func(ctx context.Context, msg *message.ProducerMessage) checker.CheckStatus {
			return checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{Topic: transferToTopic})
		}),
	}
	testProduceCheckBySend(t, checkCase)
}

func testProduceCheckBySend(t *testing.T, testCase testProduceCheckCase) {
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	// format topics
	pTopics := make([]string, 0)
	pTopics = append(pTopics, testCase.groundTopic)
	if testCase.checkpoint.CheckType == checker.ProduceCheckTypeTransfer {
		pTopics = append(pTopics, testCase.transferToTopic)
	} else if testCase.checkpoint.CheckType == checker.ProduceCheckTypeDiscard {
		// do nothing
	} else {
		fTopics, err := util.FormatTopics(testCase.groundTopic, internal.FormatLevels(testCase.produceToLevel), message.Statuses{message.StatusReady}, "")
		assert.Nil(t, err)
		pTopics = append(pTopics, fTopics...)
	}
	// clean up topic
	internal.CleanUpTopics(t, manager, pTopics...)
	defer internal.CleanUpTopics(t, manager, pTopics...)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, pTopics, 0)

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	upgradeLevel, _ := message.LevelOf(testCase.upgradeLevel)
	degradeLevel, _ := message.LevelOf(testCase.degradeLevel)
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:          testCase.groundTopic,
		DiscardEnable:  config.ToPointer(testCase.checkpoint.CheckType == checker.ProduceCheckTypeDiscard),
		DeadEnable:     config.ToPointer(testCase.checkpoint.CheckType == checker.ProduceCheckTypeDead),
		UpgradeEnable:  config.ToPointer(testCase.checkpoint.CheckType == checker.ProduceCheckTypeUpgrade),
		DegradeEnable:  config.ToPointer(testCase.checkpoint.CheckType == checker.ProduceCheckTypeDegrade),
		ShiftEnable:    config.ToPointer(testCase.checkpoint.CheckType == checker.ProduceCheckTypeShift),
		TransferEnable: config.ToPointer(testCase.checkpoint.CheckType == checker.ProduceCheckTypeTransfer),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: true},
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		Shift:          &config.ShiftPolicy{ConnectInSyncEnable: true},
		Dead:           &config.ShiftPolicy{ConnectInSyncEnable: true},
	}, testCase.checkpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	msg := internal.GenerateProduceMessage(internal.Size64)
	msgID, err := producer.Send(context.Background(), msg)
	assert.Nil(t, err)
	fmt.Println(msgID)

	stats, err := manager.Stats(pTopics[len(pTopics)-1])
	assert.Nil(t, err)
	assert.Equal(t, testCase.expectedStoredCount, stats.MsgInCounter)
}

func TestProduceCheck_All_BySend(t *testing.T) {
	upgradeLevel := message.L2
	degradeLevel := message.B2
	shiftLevel := message.L3
	deadLevel := message.D1
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	transferredTopic := groundTopic + "-OTHER"
	pTopics := []string{
		groundTopic,                              // ready
		"",                                       // discard
		groundTopic + deadLevel.TopicSuffix(),    // dead
		groundTopic + upgradeLevel.TopicSuffix(), // upgrade
		groundTopic + degradeLevel.TopicSuffix(), // degrade
		groundTopic + shiftLevel.TopicSuffix(),   // shift
		transferredTopic,                         //transfer
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
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)

	// clean up topic
	internal.CleanUpTopics(t, manager, pTopics...)
	defer internal.CleanUpTopics(t, manager, pTopics...)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, pTopics, 0)

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:          groundTopic,
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

	for i := 0; i < len(pTopics); i++ {
		msg := internal.GenerateProduceMessage(internal.Size64, "Index", strconv.Itoa(i))
		mid, err := producer.Send(context.Background(), msg)
		assert.Nil(t, err)
		fmt.Println("sent message: ", mid)
	}

	for index, storedTopic := range pTopics {
		expected := 1
		if storedTopic == "" { // discard
			expected = 1 // 发送discard消息之前, 发了一个ready消息, 所以这里依然expected 1
			storedTopic = groundTopic
		}
		stats, err := manager.Stats(storedTopic)
		assert.Nil(t, err, "index = "+strconv.Itoa(index))
		assert.Equal(t, expected, stats.MsgInCounter, fmt.Sprintf("failed to validate stats for the %v topic: %v", index, storedTopic))
	}
}
