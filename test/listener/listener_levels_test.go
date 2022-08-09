package listener

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/topic"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestListen_2Msg_L2(t *testing.T) {
	testListenByMultiLevels(t, topic.Levels{topic.L2})
}

func TestListen_2Msg_L1_L2(t *testing.T) {
	testListenByMultiLevels(t, topic.Levels{topic.L1, topic.L2})
}

func TestListen_1Msg_B1(t *testing.T) {
	testListenByMultiLevels(t, topic.Levels{topic.B1})
}

func TestListen_2Msg_L1_B1(t *testing.T) {
	testListenByMultiLevels(t, topic.Levels{topic.L1, topic.B1})
}

func TestListen_1Msg_S1(t *testing.T) {
	testListenByMultiLevels(t, topic.Levels{topic.S1})
}

func TestListen_2Msg_L1_S1(t *testing.T) {
	testListenByMultiLevels(t, topic.Levels{topic.L1, topic.S1})
}

func TestListen_4Msg_L1_L2_B1_S1(t *testing.T) {
	testListenByMultiLevels(t, topic.Levels{topic.L1, topic.L2, topic.B1, topic.S1})
}

func TestListen_7Msg_AllLevels(t *testing.T) {
	testListenByMultiLevels(t, topic.Levels{
		topic.S2, topic.S1,
		topic.L3, topic.L2, topic.L1,
		topic.B1, topic.B2,
	})
}

func testListenByMultiLevels(t *testing.T, levels topic.Levels) {
	testTopic := internal.GenerateTestTopic()
	// format topics
	storedTopics := make([]string, len(levels))
	for index, level := range levels {
		storedTopic := testTopic
		if level != topic.L1 {
			storedTopic = testTopic + "-" + strings.ToUpper(level.String())
		}
		storedTopics[index] = storedTopic
	}

	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up testTopic
	for _, storedTopic := range storedTopics {
		internal.CleanUpTopic(t, manager, storedTopic)
	}
	defer func() {
		for _, storedTopic := range storedTopics {
			internal.CleanUpTopic(t, manager, storedTopic)
		}
	}()
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:       testTopic,
		RouteEnable: true,
		Route:       &config.RoutePolicy{ConnectInSyncEnable: true},
	}, checker.PrevSendRoute(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
		if storedTopic, ok := msg.Properties["routeTopic"]; ok {
			return checker.CheckStatusPassed.WithRerouteTopic(storedTopic)
		}
		return checker.CheckStatusRejected
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	// send messages
	for index, level := range levels {
		storedTopic := storedTopics[index]
		var msgID pulsar.MessageID
		if level == topic.L1 { // ready level
			msgID, err = producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
		} else {
			msgID, err = producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K, "routeTopic", storedTopic))
		}
		assert.Nil(t, err)
		fmt.Println("produced message: ", msgID)
		// check send stats
		stats, err := manager.Stats(storedTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
	}

	// ---------------

	// create listener
	listener := internal.CreateListener(client, config.ConsumerConfig{
		Topic:                       testTopic,
		SubscriptionName:            internal.GenerateSubscribeNameByTopic(testTopic),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		//RetryingEnable:              true, // enable retrying if matches
		//PendingEnable:               true, // enable pending if matches
		//BlockingEnable:              true, // enable blocking if matches
		Levels: levels,
	})
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
	for _, storedTopic := range storedTopics {
		stats, err := manager.Stats(storedTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgOutCounter)
		assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	}
	// stop listener
	cancel()
}
