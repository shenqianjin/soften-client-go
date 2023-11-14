package times

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testConsumeLimitCase struct {
	groundTopic string
	level       string

	consumeToDeadFinal  bool
	consumeToStatus     string
	consumeHandler      handler.PremiumHandleFunc
	consumeHandleGoto   string
	consumeStatusPolicy *config.StatusPolicy

	waitTime                     time.Duration
	expectedStatusReentrantTimes int
	expectedDeadMsgCount         int
}

func TestConsumeLimitL2_Retrying_Success(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeTimes)
	level := message.L2

	testPolicy := &config.StatusPolicy{
		// reentrant times:        1     +1    +2    +4         = 8
		// real delay:             2s    2s    4s    8s         = 16s
		BackoffDelays:   []string{"1s", "2s", "3s", "7s", "3s"}, // 3s   3s    3s
		ReentrantDelay:  config.ToPointer(uint(2)),
		ConsumeMaxTimes: config.ToPointer(uint(8)),
	}
	successConsumeTimes := 5
	expectedStatusReentrantTimes := 8
	waitTime := (16 + 1) * time.Second

	HandleCase := testConsumeLimitCase{
		groundTopic:                  topic,
		level:                        level.String(),
		consumeToDeadFinal:           true,
		consumeToStatus:              message.StatusRetrying.String(),
		consumeHandleGoto:            message.StatusRetrying.String(),
		consumeStatusPolicy:          testPolicy,
		expectedStatusReentrantTimes: expectedStatusReentrantTimes,
		expectedDeadMsgCount:         0,
		waitTime:                     waitTime,
		consumeHandler: func(ctx context.Context, msg message.Message) handler.HandleStatus {
			tpc := msg.Topic()
			index := strings.LastIndexAny(tpc, "/")
			msgCounter := message.Parser.GetMessageCounter(msg)
			fmt.Printf("%v: topic: %v, msgId: %v, message counter: %v, current redelivery count: %v\n",
				time.Now().Format(time.RFC3339Nano), msg.Topic()[index:], msg.ID(), msgCounter, msg.RedeliveryCount())
			if msgCounter.ConsumeTimes == successConsumeTimes {
				return handler.StatusDone
			}
			return handler.StatusRetrying
		},
	}
	testConsumeLimitGoto(t, HandleCase)
}

func TestConsumeLimitL2_Retrying(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeTimes)
	level := message.L2

	testPolicy := &config.StatusPolicy{
		// reentrant times:        1     +1    +2    +4    +2    [+2,  +2     +2] = 16
		// real delay:             2s    2s    4s    8s    4s     4s    4s     4s = 32s
		BackoffDelays:   []string{"1s", "2s", "3s", "7s", "3s"}, // 3s   3s    3s -> DLQ
		ReentrantDelay:  config.ToPointer(uint(2)),
		ConsumeMaxTimes: config.ToPointer(uint(8)),
	}
	expectedStatusReentrantTimes := 16
	waitTime := (32 + 1) * time.Second

	HandleCase := testConsumeLimitCase{
		groundTopic:                  topic,
		level:                        level.String(),
		consumeToDeadFinal:           true,
		consumeToStatus:              message.StatusRetrying.String(),
		consumeHandleGoto:            message.StatusRetrying.String(),
		consumeStatusPolicy:          testPolicy,
		expectedStatusReentrantTimes: expectedStatusReentrantTimes,
		expectedDeadMsgCount:         1,
		waitTime:                     waitTime,
		consumeHandler: func(ctx context.Context, msg message.Message) handler.HandleStatus {
			tpc := msg.Topic()
			index := strings.LastIndexAny(tpc, "/")
			msgCounter := message.Parser.GetMessageCounter(msg)
			statusMsgCounter := message.Parser.GetStatusMessageCounter(message.StatusRetrying, msg)
			fmt.Printf("%v: topic: %v, msgId: %v, message counter: %v, statu message counter: %v,"+
				" current redelivery count: %v\n",
				time.Now().Format(time.RFC3339Nano), msg.Topic()[index:], msg.ID(), msgCounter.ToString(),
				statusMsgCounter.ToString(), msg.RedeliveryCount())
			return handler.StatusRetrying
		},
	}
	testConsumeLimitGoto(t, HandleCase)
}

func TestConsumeLimitL2_Pending(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeTimes)
	level := message.L2

	testPolicy := &config.StatusPolicy{
		// reentrant times:        1     +1    +2    +4    +2     = 16
		// real delay:             2s    2s    4s    8s    4s     = 32s
		BackoffDelays:   []string{"1s", "2s", "3s", "7s", "3s"}, // 3s   3s    3s -> DLQ
		ReentrantDelay:  config.ToPointer(uint(2)),
		ConsumeMaxTimes: config.ToPointer(uint(8)),
	}
	expectedStatusReentrantTimes := 16
	waitTime := (32 + 1) * time.Second

	HandleCase := testConsumeLimitCase{
		groundTopic:                  topic,
		level:                        level.String(),
		consumeToDeadFinal:           true,
		consumeToStatus:              message.StatusPending.String(),
		consumeHandleGoto:            handler.StatusPending.GetGoto().String(),
		consumeStatusPolicy:          testPolicy,
		expectedStatusReentrantTimes: expectedStatusReentrantTimes,
		expectedDeadMsgCount:         1,
		waitTime:                     waitTime,
		consumeHandler: func(ctx context.Context, msg message.Message) handler.HandleStatus {
			tpc := msg.Topic()
			index := strings.LastIndexAny(tpc, "/")
			msgCounter := message.Parser.GetMessageCounter(msg)
			fmt.Printf("%v: topic: %v, msgId: %v, message counter: %v, current redelivery count: %v\n",
				time.Now().Format(time.RFC3339Nano), msg.Topic()[index:], msg.ID(), msgCounter.ToString(), msg.RedeliveryCount())
			return handler.StatusPending
		},
	}
	testConsumeLimitGoto(t, HandleCase)
}
func TestConsumeLimitL2_Blocking(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeTimes)
	level := message.L2

	testPolicy := &config.StatusPolicy{
		// reentrant times:        1     +1    +2    +4    +2    [+2,  +2     +2] = 16
		// real delay:             2s    2s    4s    8s    4s     4s    4s     4s = 32s
		BackoffDelays:   []string{"1s", "2s", "3s", "7s", "3s"}, // 3s   3s    3s -> DLQ
		ReentrantDelay:  config.ToPointer(uint(2)),
		ConsumeMaxTimes: config.ToPointer(uint(8)),
	}
	expectedStatusReentrantTimes := 16
	waitTime := (32 + 1) * time.Second

	HandleCase := testConsumeLimitCase{
		groundTopic:                  topic,
		level:                        level.String(),
		consumeToDeadFinal:           true,
		consumeToStatus:              message.StatusBlocking.String(),
		consumeHandleGoto:            handler.StatusBlocking.GetGoto().String(),
		consumeStatusPolicy:          testPolicy,
		expectedStatusReentrantTimes: expectedStatusReentrantTimes,
		expectedDeadMsgCount:         1,
		waitTime:                     waitTime,
		consumeHandler: func(ctx context.Context, msg message.Message) handler.HandleStatus {
			tpc := msg.Topic()
			index := strings.LastIndexAny(tpc, "/")
			msgCounter := message.Parser.GetMessageCounter(msg)
			fmt.Printf("%v: topic: %v, msgId: %v, message counter: %v, current redelivery count: %v\n",
				time.Now().Format(time.RFC3339Nano), msg.Topic()[index:], msg.ID(), msgCounter, msg.RedeliveryCount())
			return handler.StatusBlocking
		},
	}
	testConsumeLimitGoto(t, HandleCase)
}

func testConsumeLimitGoto(t *testing.T, handleCase testConsumeLimitCase) {
	groundTopic := handleCase.groundTopic
	level := message.L1
	if handleCase.level != "" {
		if lvl, err := message.LevelOf(handleCase.level); err != nil {
			panic(err)
		} else {
			level = lvl
		}
	}
	pTopics, err := util.FormatTopics(handleCase.groundTopic, message.Levels{level}, message.Statuses{message.StatusReady}, "")
	assert.Nil(t, err)
	cTopics, err := util.FormatTopics(handleCase.groundTopic, message.Levels{level}, internal.FormatStatuses(handleCase.consumeToStatus), internal.TestSubscriptionName())
	assert.Nil(t, err)
	if handleCase.consumeToDeadFinal {
		dTopic, err1 := util.FormatDeadTopic(handleCase.groundTopic, internal.TestSubscriptionName())
		assert.Nil(t, err1)
		cTopics = append(cTopics, dTopic)
	}
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)

	topics := append(pTopics, cTopics...)
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
	// create producer
	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic: groundTopic,
		Level: level,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	// send messages
	msgID, err := producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
	assert.Nil(t, err)
	fmt.Println("produced message: ", msgID)
	// Handle send stats
	stats, err := manager.Stats(pTopics[0])
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	// create listener
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:  config.ToPointer(handleCase.consumeHandleGoto == handler.StatusDiscard.GetGoto().String()),
		DeadEnable:     config.ToPointer(handleCase.consumeToDeadFinal),
		PendingEnable:  config.ToPointer(handleCase.consumeHandleGoto == handler.StatusPending.GetGoto().String()),
		Pending:        handleCase.consumeStatusPolicy,
		BlockingEnable: config.ToPointer(handleCase.consumeHandleGoto == handler.StatusBlocking.GetGoto().String()),
		Blocking:       handleCase.consumeStatusPolicy,
		RetryingEnable: config.ToPointer(handleCase.consumeHandleGoto == handler.StatusRetrying.GetGoto().String()),
		Retrying:       handleCase.consumeStatusPolicy,
	}
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       groundTopic,
		Level:                       level,
		Levels:                      message.Levels{level},
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		LevelPolicy:                 leveledPolicy,
		//NackRedeliveryDelay:         6,
		//Type: pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, handleCase.consumeHandler)
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(handleCase.waitTime)
	// Handle stats
	stats, err = manager.Stats(pTopics[0])
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// Handle transferred stats
	if len(cTopics) == 2 {
		// wait for decide the message
		time.Sleep(10000 * time.Millisecond)
		stats, err = manager.Stats(cTopics[0])
		assert.Nil(t, err)
		assert.Equal(t, handleCase.expectedStatusReentrantTimes, stats.MsgInCounter)
		assert.Equal(t, stats.MsgInCounter, stats.MsgOutCounter)
		if handleCase.consumeHandleGoto == handler.StatusPending.GetGoto().String() ||
			handleCase.consumeHandleGoto == handler.StatusBlocking.GetGoto().String() ||
			handleCase.consumeHandleGoto == handler.StatusRetrying.GetGoto().String() {
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 0, v.MsgBacklog)
				break
			}
		}
		stats, err = manager.Stats(cTopics[1])
		assert.Nil(t, err)
		assert.Equal(t, handleCase.expectedDeadMsgCount, stats.MsgInCounter)
		assert.Equal(t, 0, stats.MsgOutCounter)
	}
	// stop listener
	cancel()
}
