package consumer

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestConsumerBalanceLevel_L1_B1(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.L1.String(), message.B1.String()},
		weights:      []int{50, 50},
		totals:       []int{500, 500},
	}
	testConsumerBalanceLevel(t, testCase)
}

func TestConsumerBalanceLevel_L1_L2(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.L1.String(), message.B1.String()},
		weights:      []int{50, 80},
		totals:       []int{500, 800},
	}
	testConsumerBalanceLevel(t, testCase)
}

func TestConsumerBalanceLevel_L1_S1(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.L1.String(), message.B1.String()},
		weights:      []int{50, 100},
		totals:       []int{500, 1000},
	}
	testConsumerBalanceLevel(t, testCase)
}

func TestConsumerBalanceLevel_L1_B1_L2_S1(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.L1.String(), message.B1.String(), message.L2.String(), message.S1.String()},
		weights:      []int{50, 25, 60, 100},
		totals:       []int{500, 250, 600, 1000},
	}
	testConsumerBalanceLevel(t, testCase)
}

func TestConsumerBalanceLevel_All(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.S2.String(), message.S1.String(),
			message.L3.String(), message.L2.String(), message.L1.String(),
			message.B1.String(), message.B2.String()},
		weights: []int{
			200, 100,
			70, 60, 50,
			30, 25,
		},
		totals: []int{
			2000, 1000,
			700, 600, 500,
			300, 250,
		},
	}
	testConsumerBalanceLevel(t, testCase)
}

func testConsumerBalanceLevel(t *testing.T, testCase consumerBalanceCase) {
	assert.True(t, len(testCase.consumeTypes) > 0)
	assert.Equal(t, len(testCase.consumeTypes), len(testCase.weights))
	assert.Equal(t, len(testCase.consumeTypes), len(testCase.totals))
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsume)
	consumeTypes := testCase.consumeTypes
	totals := testCase.totals
	expectedTotal := 0
	expectedWeights := make(map[string]int, len(testCase.consumeTypes))
	for index, lvl := range testCase.consumeTypes {
		expectedWeights[lvl] = testCase.weights[index]
	}
	levels := make(message.Levels, len(consumeTypes))
	for index, lvlStr := range consumeTypes {
		lvl, err := message.LevelOf(lvlStr)
		assert.Nil(t, err)
		expectedTotal += totals[index]
		levels[index] = lvl
	}

	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	pTopics, err := util.FormatTopics(groundTopic, testCase.consumeTypes, []string{message.StatusReady.String()}, internal.TestSubscriptionName())
	assert.Nil(t, err)
	// clean up tpc
	internal.CleanUpTopics(t, manager, pTopics...)
	defer internal.CleanUpTopics(t, manager, pTopics...)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, pTopics, 0)
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producers := make([]soften.Producer, len(pTopics))
	for index, producedTopic := range pTopics {
		producer, err := client.CreateProducer(config.ProducerConfig{
			Topic: producedTopic,
		})
		if err != nil {
			log.Fatal(err)
		}
		producers[index] = producer
	}
	defer func() {
		for _, producer := range producers {
			producer.Close()
		}
	}()

	// send messages
	wg := sync.WaitGroup{}
	for index, total := range totals {
		assert.True(t, total >= 0)
		wg.Add(1)
		go func(index int, total int) {
			for count := 0; count < total; count++ {
				msg := internal.GenerateProduceMessage(internal.Size64, "ConsumeType", consumeTypes[index])
				mid, err := producers[index].Send(context.Background(), msg)
				assert.Nil(t, err)
				assert.True(t, mid.LedgerID() >= 0)
			}
			wg.Done()
		}(index, total)
	}
	wg.Wait()
	for index, total := range totals {
		// check send stats
		stats, err := manager.Stats(pTopics[index])
		assert.Nil(t, err)
		assert.Equal(t, total, stats.MsgInCounter)
		assert.Equal(t, 0, stats.MsgOutCounter)
	}

	// ---------------

	// create listener
	consumerConf := config.ConsumerConfig{
		Topic:                       groundTopic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		Concurrency: &config.ConcurrencyPolicy{
			CorePoolSize: 4,
		},
		Levels:        levels,
		LevelPolicies: make(config.LevelPolicies, 0),
	}
	for _, lvl := range levels {
		if w, ok := expectedWeights[lvl.String()]; ok {
			consumerConf.LevelPolicies[lvl] = &config.LevelPolicy{ConsumeWeight: uint(w), DeadEnable: config.False()}
		}
	}
	listener, err := client.CreateListener(consumerConf)

	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	consumedCh := make(chan string, 100)
	doneCh := make(chan struct{}, 1)
	err = listener.Start(ctx, func(ctx context.Context, msg message.Message) (bool, error) {
		time.Sleep(time.Millisecond * 20)
		consumedCh <- msg.Properties()["ConsumeType"]
		return true, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	monitorOptions := consumeMonitorOptions{expectedTotal: expectedTotal, doneCh: doneCh, expectedWeights: expectedWeights}
	go consumeMonitor(t, ctx, consumedCh, monitorOptions)
	// wait for consuming the message and wait for decide the message
	<-doneCh
	time.Sleep(100 * time.Millisecond)
	// check stats
	for index, producedTopic := range pTopics {
		stats, err := manager.Stats(producedTopic)
		assert.Nil(t, err, "produced tpc: ", index)
		assert.Equal(t, testCase.totals[index], stats.MsgInCounter, "produced tpc: ", producedTopic)
		assert.Equal(t, testCase.totals[index], stats.MsgOutCounter)
		for _, v := range stats.Subscriptions {
			assert.Equal(t, 0, v.MsgBacklog, "decided tpc: ", producedTopic)
			break
		}
	}
	// stop listener
	cancel()
}
