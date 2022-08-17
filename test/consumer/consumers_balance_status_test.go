package consumer

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type consumeMonitorOptions struct {
	expectedTotal   int
	doneCh          chan<- struct{}
	expectedWeights map[string]int
}

type consumerBalanceCase struct {
	consumeTypes []string
	weights      []int
	totals       []int
}

func TestConsumerBalanceStatus_Ready_Retrying(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.StatusReady.String(), message.StatusRetrying.String()},
		weights:      []int{50, 50},
		totals:       []int{500, 500},
	}
	testConsumerBalanceStatus(t, testCase)
}

func TestConsumerBalanceStatus_Ready_Pending(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.StatusReady.String(), message.StatusPending.String()},
		weights:      []int{50, 40},
		totals:       []int{500, 400},
	}
	testConsumerBalanceStatus(t, testCase)
}

func TestConsumerBalanceStatus_Ready_Blocking(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.StatusReady.String(), message.StatusBlocking.String()},
		weights:      []int{50, 30},
		totals:       []int{500, 300},
	}
	testConsumerBalanceStatus(t, testCase)
}

func TestConsumerBalanceStatus_All(t *testing.T) {
	testCase := consumerBalanceCase{
		consumeTypes: []string{message.StatusReady.String(), message.StatusRetrying.String(),
			message.StatusPending.String(), message.StatusBlocking.String()},
		weights: []int{50, 40, 30, 20},
		totals:  []int{500, 400, 300, 200},
	}
	testConsumerBalanceStatus(t, testCase)
}

func testConsumerBalanceStatus(t *testing.T, testCase consumerBalanceCase) {
	assert.True(t, len(testCase.consumeTypes) > 0)
	assert.Equal(t, len(testCase.consumeTypes), len(testCase.weights))
	assert.Equal(t, len(testCase.consumeTypes), len(testCase.totals))
	topic := internal.GenerateTestTopic(internal.PrefixTestConsume)
	consumeTypes := testCase.consumeTypes
	totals := testCase.totals
	expectedTotal := 0
	expectedWeights := make(map[string]int, len(testCase.consumeTypes))
	for index, status := range testCase.consumeTypes {
		expectedWeights[status] = testCase.weights[index]
	}
	producedTopics := make([]string, len(consumeTypes))
	for index, status := range consumeTypes {
		st, err := message.StatusOf(status)
		assert.Nil(t, err)
		if st == message.StatusReady {
			producedTopics[index] = topic + st.TopicSuffix()
		} else {
			producedTopics[index] = topic + "-" + internal.TestSubscriptionName() + st.TopicSuffix()
		}
		expectedTotal += totals[index]
	}

	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up topic
	for _, producedTopic := range producedTopics {
		if producedTopic != "" {
			internal.CleanUpTopic(t, manager, producedTopic)
		}
	}
	defer func() {
		for _, producedTopic := range producedTopics {
			if producedTopic != "" {
				internal.CleanUpTopic(t, manager, producedTopic)
			}
		}
	}()
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()
	// create producer
	producers := make([]soften.Producer, len(producedTopics))
	for index, producedTopic := range producedTopics {
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
		stats, err := manager.Stats(producedTopics[index])
		assert.Nil(t, err)
		assert.Equal(t, total, stats.MsgInCounter)
		assert.Equal(t, 0, stats.MsgOutCounter)
	}

	// ---------------

	testPolicy := &config.StatusPolicy{
		BackoffDelays:  []string{"1s"},
		ReentrantDelay: 1,
	}
	leveledPolicy := &config.LevelPolicy{
		PendingEnable:  true,
		BlockingEnable: true,
		RetryingEnable: true,
	}
	// create listener
	consumerConf := config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
		Concurrency: &config.ConcurrencyPolicy{
			CorePoolSize: 4,
		},
	}
	if w, ok := expectedWeights[message.StatusReady.String()]; ok {
		consumerConf.Ready = &config.StatusPolicy{ConsumeWeight: uint(w)}
	}
	if w, ok := expectedWeights[message.StatusPending.String()]; ok {
		consumerConf.Pending = &config.StatusPolicy{ConsumeWeight: uint(w), BackoffDelays: testPolicy.BackoffDelays, ReentrantDelay: testPolicy.ReentrantDelay}
	}
	if w, ok := expectedWeights[message.StatusRetrying.String()]; ok {
		consumerConf.Retrying = &config.StatusPolicy{ConsumeWeight: uint(w), BackoffDelays: testPolicy.BackoffDelays, ReentrantDelay: testPolicy.ReentrantDelay}
	}
	if w, ok := expectedWeights[message.StatusBlocking.String()]; ok {
		consumerConf.Blocking = &config.StatusPolicy{ConsumeWeight: uint(w), BackoffDelays: testPolicy.BackoffDelays, ReentrantDelay: testPolicy.ReentrantDelay}
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
		time.Sleep(time.Millisecond * 50)
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
	for index, producedTopic := range producedTopics {
		stats, err := manager.Stats(producedTopic)
		assert.Nil(t, err, "produced topic: ", index)
		assert.Equal(t, testCase.totals[index], stats.MsgInCounter, "produced topic: ", producedTopic)
		assert.Equal(t, testCase.totals[index], stats.MsgOutCounter)
		for _, v := range stats.Subscriptions {
			assert.Equal(t, 0, v.MsgBacklog, "decided topic: ", producedTopic)
			break
		}
	}
	// stop listener
	cancel()
}

func consumeMonitor(t *testing.T, ctx context.Context, consumedCh <-chan string, options consumeMonitorOptions) {
	interval := 2
	intervalConsumedCount := 0
	total := 0
	tickerCount := 0
	consumedStatsKeys := make([]string, 0)
	consumedStats := make(map[string]int, 0)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	totalWeight := 0
	for _, weight := range options.expectedWeights {
		totalWeight += weight
	}
	defer ticker.Stop()
	for {
		needLog := false
		select {
		case <-ctx.Done():
			return
		case consumedType := <-consumedCh:
			total++
			intervalConsumedCount++
			if _, ok := consumedStats[consumedType]; ok {
				consumedStats[consumedType]++
			} else {
				consumedStatsKeys = append(consumedStatsKeys, consumedType)
				consumedStats[consumedType] = 1
			}
		case <-ticker.C:
			tickerCount++
			needLog = true

		}
		if total == options.expectedTotal {
			needLog = true
			options.doneCh <- struct{}{}
		}
		if needLog {
			b := &bytes.Buffer{}
			roundMsg := intervalConsumedCount
			roundRate := float64(intervalConsumedCount) / float64(interval)
			_, _ = fmt.Fprintf(b, `>>>>>>>> Summary - Consumed total: %6d msg, rate: %6.1f msg/s
						- Current               Round: %6d msg, rate: %6.1f msg/s`,
				total, float64(total)/float64(tickerCount*interval), roundMsg, roundRate)
			for _, stat := range consumedStatsKeys {
				count := consumedStats[stat]
				expectedCount := int(float64(roundMsg) * float64(options.expectedWeights[stat]) / float64(totalWeight))
				diffCount := count - expectedCount
				statRate := float64(count) / float64(interval)
				expectedStatRate := roundRate * float64(options.expectedWeights[stat]) / float64(totalWeight)
				diffRate := statRate - expectedStatRate
				_, _ = fmt.Fprintf(b, `
						- Consume %8s count(msg): %6d msg, expected: %6d, diff: %6d;  rate(msg/s): %6.1f, expected: %6.1f, diff: %6.1f`,
					stat, count, expectedCount, diffCount, statRate, expectedStatRate, diffRate)
				if roundMsg > 100 {
					assert.True(t, count > 0)
					assert.True(t, math.Abs(float64(diffCount)) < float64(roundMsg)*0.2)
				}
				consumedStats[stat] = 0
			}
			log.Println(b.String())
			intervalConsumedCount = 0
		}
	}
}
