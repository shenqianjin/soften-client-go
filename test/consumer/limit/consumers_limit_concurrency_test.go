package limit

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type testConsumeLimitCase struct {
	groundTopic      string
	levels           message.Levels
	statuses         message.Statuses
	leveledPolicies  config.LevelPolicies
	consumerLimit    *config.LimitPolicy
	MsgCountPerTopic int
	MaxConcurrency   int
	MaxOPS           int
}

func TestConsumeLimit_Concurrency_Ready(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			Ready:      &config.ReadyPolicy{ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))}},
			DeadEnable: config.False(),
			Done:       &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_Concurrency_Pending(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			Ready:         &config.ReadyPolicy{ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))}},
			PendingEnable: config.ToPointer(true),
			Pending:       &config.StatusPolicy{ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))}},
			DeadEnable:    config.False(),
			Done:          &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady, message.StatusPending},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_Concurrency_Blocking(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			Ready:          &config.ReadyPolicy{ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))}},
			BlockingEnable: config.ToPointer(true),
			Blocking:       &config.StatusPolicy{ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))}},
			DeadEnable:     config.False(),
			Done:           &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady, message.StatusBlocking},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_Concurrency_Retrying(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			Ready:          &config.ReadyPolicy{ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))}},
			RetryingEnable: config.ToPointer(true),
			Retrying:       &config.StatusPolicy{ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))}},
			DeadEnable:     config.False(),
			Done:           &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady, message.StatusRetrying},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_Concurrency_L1(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))},
			DeadEnable:   config.False(),
			Done:         &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_Concurrency_L2(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L2: &config.LevelPolicy{
			ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))},
			DeadEnable:   config.False(),
			Done:         &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L2},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_Concurrency_MultiLevels(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L2: &config.LevelPolicy{
			ConsumeLimit: &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))},
			DeadEnable:   config.False(),
			Done:         &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L2, message.L3, message.B2},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_Concurrency_ConsumerLevel_L2(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L2: &config.LevelPolicy{
			DeadEnable: config.False(),
			Done:       &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L2},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		consumerLimit:    &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))},
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_Concurrency_ConsumerLevel_MultiLevels(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L2: &config.LevelPolicy{
			DeadEnable: config.False(),
			Done:       &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L2, message.L3, message.B2},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		consumerLimit:    &config.LimitPolicy{MaxConcurrency: config.ToPointer(uint(10))},
		MsgCountPerTopic: 300,
		MaxConcurrency:   10,
	}
	testConsumeLimit(t, testCase)
}

func testConsumeLimit(t *testing.T, handleCase testConsumeLimitCase) {
	pTopics, err := util.FormatTopics(handleCase.groundTopic, handleCase.levels, handleCase.statuses, internal.TestSubscriptionName())
	assert.Nil(t, err)

	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	topics := pTopics
	// clean up groundTopic
	internal.CleanUpTopics(t, manager, topics...)
	defer func() {
		internal.CleanUpTopics(t, manager, topics...)
	}()
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, topics, 0)
	// create client
	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	for _, topic := range pTopics {
		// create producer
		producer, err := client.CreateProducer(config.ProducerConfig{
			Topic: topic,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer producer.Close()
		// send messages
		for i := 0; i < handleCase.MsgCountPerTopic; i++ {
			_, err := producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
			assert.Nil(t, err)
		}
		// Handle send stats
		stats, err := manager.Stats(pTopics[0])
		assert.Nil(t, err)
		assert.Equal(t, handleCase.MsgCountPerTopic, stats.MsgInCounter)
	}

	// ---------------

	// create listener
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       handleCase.groundTopic,
		Levels:                      handleCase.levels,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicies:               handleCase.leveledPolicies,
		ConsumerLimit:               handleCase.consumerLimit,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	concurrencyMap := map[string]int{}
	concurrencyMapLock := sync.RWMutex{}
	countMap := map[string]int{}
	countMapLock := sync.RWMutex{}
	// listener starts
	err = listener.StartPremium(context.Background(), func(ctx context.Context, msg message.Message) handler.HandleStatus {
		//fmt.Printf("mid: %v\n", msg.ID())
		// stats concurrency
		concurrencyMapLock.Lock()
		if v, ok := concurrencyMap[msg.Topic()]; ok {
			concurrencyMap[msg.Topic()] = v + 1
		} else {
			concurrencyMap[msg.Topic()] = 1
		}
		concurrencyMapLock.Unlock()
		// mock biz logic
		time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
		// stats count
		countMapLock.Lock()
		if v, ok := countMap[msg.Topic()]; ok {
			countMap[msg.Topic()] = v + 1
		} else {
			countMap[msg.Topic()] = 1
		}
		countMapLock.Unlock()
		// release concurrency
		concurrencyMapLock.Lock()
		if v, ok := concurrencyMap[msg.Topic()]; ok {
			concurrencyMap[msg.Topic()] = v - 1
		}
		concurrencyMapLock.Unlock()
		return handler.StatusDone
	})
	if err != nil {
		log.Fatal(err)
	}
	closeCh := make(chan struct{}, 1)
	// validate limit
	go func() {
		prevCountMap := map[string]int{}
		for c := 0; ; c++ {
			time.Sleep(10 * time.Millisecond)
			closed := false
			select {
			case <-closeCh:
				closed = true
				break
			default:
			}
			if closed {
				break
			}
			// rate
			if c%100 == 0 {
				// rate
				curCountMap := map[string]int{}
				countMapLock.RLock()
				for k, v := range countMap {
					curCountMap[k] = v
				}
				countMapLock.RUnlock()
				for k, v := range curCountMap {
					pv := 0
					if pvt, ok := prevCountMap[k]; ok {
						pv = pvt
					}
					ops := v - pv
					fmt.Printf("======> OPS of %s = %d\n", k, ops)
					if handleCase.MaxOPS <= 0 {
						continue
					}
					assert.True(t, ops <= handleCase.MaxOPS+5, // 由于计算可能存在一定的误差,增加5的容忍
						fmt.Sprintf("%s: current OPS: %d is larger than max: %d", k, ops, handleCase.MaxOPS))
				}
				// record prev count map
				for k, v := range curCountMap {
					prevCountMap[k] = v
				}
				// concurrency
				concurrencyMapLock.RLock()
				for k, v := range concurrencyMap {
					fmt.Printf("======> Concurrency of %s = %d\n", k, v)
					if handleCase.MaxConcurrency <= 0 {
						continue
					}
					assert.True(t, v <= handleCase.MaxConcurrency, fmt.Sprintf("%s: current concurrency: %d is larger than max: %d",
						k, v, handleCase.MaxConcurrency))
				}
				concurrencyMapLock.RUnlock()
			}
			// validate concurrency
			concurrencyMapLock.RLock()
			for k, v := range concurrencyMap {
				if handleCase.MaxConcurrency <= 0 {
					continue
				}
				assert.True(t, v <= handleCase.MaxConcurrency,
					fmt.Sprintf("%s: current concurrency: %d is larger than max: %d", k, v, handleCase.MaxConcurrency))
			}
			concurrencyMapLock.RUnlock()
		}
	}()
	// wait for consuming the message
	for {
		time.Sleep(8 * time.Millisecond)
		handled := 0
		countMapLock.RLock()
		for _, v := range countMap {
			handled += v
		}
		countMapLock.RUnlock()
		if handled == handleCase.MsgCountPerTopic*len(pTopics) {
			listener.Close()
			closeCh <- struct{}{}
			break
		}
	}
	// Handle stats
	for _, topic := range pTopics {
		stats, err1 := manager.Stats(topic)
		assert.Nil(t, err1)
		assert.Equal(t, handleCase.MsgCountPerTopic, stats.MsgOutCounter)
		assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	}
}
