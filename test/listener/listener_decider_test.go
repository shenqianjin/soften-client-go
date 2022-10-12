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
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testListenDecideCase struct {
	topic                     string
	storedTopic               string // produce to / consume from
	decidedTopic              string // Handle to
	expectedStoredOutCount    int    // should always 1
	expectedTransferdOutCount int    // 1 for pending, blocking, retrying; 0 for upgrade, degrade, transfer
	handleGoto                string
	consumeTime               time.Time

	// extra for upgrade/degrade
	upgradeLevel string
	degradeLevel string
}

func TestListenDecide_Pending(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		topic:                     topic,
		storedTopic:               topic,
		decidedTopic:              internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusPending.TopicSuffix()),
		handleGoto:                decider.GotoPending.String(),
		expectedTransferdOutCount: 1, // transfer the msg to pending queue, and then reconsume it
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Blocking(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		topic:                     topic,
		storedTopic:               topic,
		decidedTopic:              internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusBlocking.TopicSuffix()),
		handleGoto:                decider.GotoBlocking.String(),
		expectedTransferdOutCount: 1,
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Retrying(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		topic:                     topic,
		storedTopic:               topic,
		decidedTopic:              internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusRetrying.TopicSuffix()),
		handleGoto:                decider.GotoRetrying.String(),
		expectedTransferdOutCount: 1,
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Upgrade(t *testing.T) {
	upgradeLevel := message.L2
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		topic:                     topic,
		storedTopic:               topic,
		decidedTopic:              topic + upgradeLevel.TopicSuffix(),
		upgradeLevel:              upgradeLevel.String(),
		handleGoto:                decider.GotoUpgrade.String(),
		consumeTime:               time.Now().Add(time.Second * 5),
		expectedTransferdOutCount: 1,
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Degrade(t *testing.T) {
	degradeLevel := message.B2
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	HandleCase := testListenDecideCase{
		topic:                     topic,
		storedTopic:               topic,
		decidedTopic:              topic + degradeLevel.TopicSuffix(),
		degradeLevel:              degradeLevel.String(),
		handleGoto:                decider.GotoDegrade.String(),
		consumeTime:               time.Now().Add(time.Second * 5),
		expectedTransferdOutCount: 1,
	}
	testListenDecide(t, HandleCase)
}

func TestListenDecide_Transfer(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestListen)
	decideCase := testListenDecideCase{
		topic:                     topic,
		storedTopic:               topic,
		decidedTopic:              topic + message.L2.TopicSuffix(),
		handleGoto:                decider.GotoTransfer.String(),
		consumeTime:               time.Now().Add(time.Second * 5),
		expectedTransferdOutCount: 1,
	}
	testListenDecide(t, decideCase)
}

func testListenDecide(t *testing.T, handleCase testListenDecideCase) {
	topic := handleCase.topic
	storedTopic := handleCase.storedTopic
	transferredTopic := handleCase.decidedTopic
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up topic
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
	msg := internal.GenerateProduceMessage(internal.Size1K)
	msg.EventTime = time.Now().Add(-time.Minute)
	msgID, err := producer.Send(context.Background(), msg)
	assert.Nil(t, err)
	fmt.Println("produced message: ", msgID)
	// Handle send stats
	stats, err := manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: 1,
	}
	// create listener
	upgradeLevel, _ := message.LevelOf(handleCase.upgradeLevel)
	degradeLevel, _ := message.LevelOf(handleCase.degradeLevel)
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoDiscard.String()),
		DeadEnable:     config.ToPointer(handleCase.handleGoto == decider.GotoDead.String()),
		PendingEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoPending.String()),
		Pending:        testPolicy,
		BlockingEnable: config.ToPointer(handleCase.handleGoto == decider.GotoBlocking.String()),
		Blocking:       testPolicy,
		RetryingEnable: config.ToPointer(handleCase.handleGoto == decider.GotoRetrying.String()),
		Retrying:       testPolicy,
		UpgradeEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoUpgrade.String()),
		Upgrade:        &config.ShiftPolicy{Level: upgradeLevel, ConnectInSyncEnable: true},
		DegradeEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoDegrade.String()),
		Degrade:        &config.ShiftPolicy{Level: degradeLevel, ConnectInSyncEnable: true},
		TransferEnable: config.ToPointer(handleCase.handleGoto == decider.GotoTransfer.String()),
		Transfer:       &config.TransferPolicy{ConnectInSyncEnable: handleCase.handleGoto == decider.GotoTransfer.String()},
	}
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
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
		Topic:                       transferredTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Type:                        pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer decidedListener.Close()

	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, func(ctx context.Context, msg message.Message) handler.HandleStatus {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		if handleGoto, err1 := decider.GotoOf(handleCase.handleGoto); err1 == nil {
			if handleGoto == decider.GotoTransfer {
				return handler.StatusTransfer.WithTopic(handleCase.decidedTopic).WithConsumeTime(handleCase.consumeTime)
			} else if handleGoto == decider.GotoUpgrade {
				return handler.StatusUpgrade.WithConsumeTime(handleCase.consumeTime)
			} else if handleGoto == decider.GotoDegrade {
				return handler.StatusDegrade.WithConsumeTime(handleCase.consumeTime)
			} else if handleGoto == decider.GotoRetrying {
				return handler.StatusRetrying.WithConsumeTime(handleCase.consumeTime)
			} else if handleGoto == decider.GotoPending {
				return handler.StatusPending.WithConsumeTime(handleCase.consumeTime)
			} else if handleGoto == decider.GotoBlocking {
				return handler.StatusBlocking.WithConsumeTime(handleCase.consumeTime)
			}
		}
		return handler.StatusAuto
	})
	if err != nil {
		log.Fatal(err)
	}
	// decided listener starts
	err = decidedListener.Start(ctx, func(ctx context.Context, msg message.Message) (success bool, err error) {
		if !handleCase.consumeTime.IsZero() {
			fmt.Println("********  now: ", time.Now(), handleCase.consumeTime, msg.EventTime())
			fmt.Println("******** ", time.Now().Sub(handleCase.consumeTime), msg.EventTime())
			assert.Equal(t, true, time.Now().Second() <= handleCase.consumeTime.Second())
		}
		return true, nil
	})
	// wait for consuming the message
	time.Sleep(100 * time.Millisecond)
	// Handle stats
	stats, err = manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// Handle transferred stats
	if transferredTopic != "" {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		if !handleCase.consumeTime.IsZero() {
			time.Sleep(handleCase.consumeTime.Sub(time.Now()))
		}
		stats, err = manager.Stats(transferredTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, handleCase.expectedTransferdOutCount, stats.MsgOutCounter)
		if handleCase.handleGoto == decider.GotoPending.String() ||
			handleCase.handleGoto == decider.GotoBlocking.String() ||
			handleCase.handleGoto == decider.GotoRetrying.String() {
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 1, v.MsgBacklog)
				break
			}
		}
	}
	// stop listener
	cancel()
}
