package limit

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
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testConsumeLimitCase struct {
	groundTopic                  string
	level                        string
	storedTopic                  string // produce to / consume from
	transferredTopic             string // Handle to
	deadTopic                    string //
	handler                      handler.PremiumHandleFunc
	handleGoto                   string
	statusPolicy                 *config.StatusPolicy
	waitTime                     time.Duration
	expectedStatusReentrantTimes int
	expectedDeadMsgCount         int
}

func TestConsumeLimitL2_Retrying_Success(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	level := message.L2

	testPolicy := &config.StatusPolicy{
		// reentrant times:        1     +1    +2    +4         = 8
		// real delay:             2s    2s    4s    8s         = 16s
		BackoffDelays:   []string{"1s", "2s", "3s", "7s", "3s"}, // 3s   3s    3s
		ReentrantDelay:  2,
		ConsumeMaxTimes: 8,
	}
	successConsumeTimes := 5
	expectedStatusReentrantTimes := 8
	waitTime := (16 + 1) * time.Second

	HandleCase := testConsumeLimitCase{
		groundTopic:                  topic,
		level:                        level.String(),
		storedTopic:                  topic + level.TopicSuffix(),
		transferredTopic:             internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), level.TopicSuffix(), message.StatusRetrying.TopicSuffix()),
		deadTopic:                    internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusDead.TopicSuffix()),
		statusPolicy:                 testPolicy,
		expectedStatusReentrantTimes: expectedStatusReentrantTimes,
		expectedDeadMsgCount:         0,
		waitTime:                     waitTime,
		handler: func(ctx context.Context, msg message.Message) handler.HandleStatus {
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
		handleGoto: message.StatusRetrying.String(),
	}
	testConsumeLimitGoto(t, HandleCase)
}

func TestConsumeLimitL2_Retrying(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	level := message.L2

	testPolicy := &config.StatusPolicy{
		// reentrant times:        1     +1    +2    +4    +2    [+2,  +2     +2] = 16
		// real delay:             2s    2s    4s    8s    4s     4s    4s     4s = 32s
		BackoffDelays:   []string{"1s", "2s", "3s", "7s", "3s"}, // 3s   3s    3s -> DLQ
		ReentrantDelay:  2,
		ConsumeMaxTimes: 8,
	}
	expectedStatusReentrantTimes := 16
	waitTime := (32 + 1) * time.Second

	HandleCase := testConsumeLimitCase{
		groundTopic:                  topic,
		level:                        level.String(),
		storedTopic:                  topic + level.TopicSuffix(),
		transferredTopic:             internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), level.TopicSuffix(), message.StatusRetrying.TopicSuffix()),
		deadTopic:                    internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusDead.TopicSuffix()),
		statusPolicy:                 testPolicy,
		expectedStatusReentrantTimes: expectedStatusReentrantTimes,
		expectedDeadMsgCount:         1,
		waitTime:                     waitTime,
		handler: func(ctx context.Context, msg message.Message) handler.HandleStatus {
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
		handleGoto: message.StatusRetrying.String(),
	}
	testConsumeLimitGoto(t, HandleCase)
}

func TestConsumeLimitL2_Pending(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	level := message.L2

	testPolicy := &config.StatusPolicy{
		// reentrant times:        1     +1    +2    +4    +2     = 16
		// real delay:             2s    2s    4s    8s    4s     = 32s
		BackoffDelays:   []string{"1s", "2s", "3s", "7s", "3s"}, // 3s   3s    3s -> DLQ
		ReentrantDelay:  2,
		ConsumeMaxTimes: 8,
	}
	expectedStatusReentrantTimes := 16
	waitTime := (32 + 1) * time.Second

	HandleCase := testConsumeLimitCase{
		groundTopic:                  topic,
		level:                        level.String(),
		storedTopic:                  topic + level.TopicSuffix(),
		transferredTopic:             internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), level.TopicSuffix(), message.StatusPending.TopicSuffix()),
		deadTopic:                    internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusDead.TopicSuffix()),
		statusPolicy:                 testPolicy,
		expectedStatusReentrantTimes: expectedStatusReentrantTimes,
		expectedDeadMsgCount:         1,
		waitTime:                     waitTime,
		handler: func(ctx context.Context, msg message.Message) handler.HandleStatus {
			tpc := msg.Topic()
			index := strings.LastIndexAny(tpc, "/")
			msgCounter := message.Parser.GetMessageCounter(msg)
			fmt.Printf("%v: topic: %v, msgId: %v, message counter: %v, current redelivery count: %v\n",
				time.Now().Format(time.RFC3339Nano), msg.Topic()[index:], msg.ID(), msgCounter.ToString(), msg.RedeliveryCount())
			return handler.StatusPending
		},
		handleGoto: message.StatusPending.String(),
	}
	testConsumeLimitGoto(t, HandleCase)
}
func TestConsumeLimitL2_Blocking(t *testing.T) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	level := message.L2

	testPolicy := &config.StatusPolicy{
		// reentrant times:        1     +1    +2    +4    +2    [+2,  +2     +2] = 16
		// real delay:             2s    2s    4s    8s    4s     4s    4s     4s = 32s
		BackoffDelays:   []string{"1s", "2s", "3s", "7s", "3s"}, // 3s   3s    3s -> DLQ
		ReentrantDelay:  2,
		ConsumeMaxTimes: 8,
	}
	expectedStatusReentrantTimes := 16
	waitTime := (32 + 1) * time.Second

	HandleCase := testConsumeLimitCase{
		groundTopic:                  topic,
		level:                        level.String(),
		storedTopic:                  topic + level.TopicSuffix(),
		transferredTopic:             internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), level.TopicSuffix(), message.StatusBlocking.TopicSuffix()),
		deadTopic:                    internal.FormatStatusTopic(topic, internal.TestSubscriptionName(), "", message.StatusDead.TopicSuffix()),
		statusPolicy:                 testPolicy,
		expectedStatusReentrantTimes: expectedStatusReentrantTimes,
		expectedDeadMsgCount:         1,
		waitTime:                     waitTime,
		handler: func(ctx context.Context, msg message.Message) handler.HandleStatus {
			tpc := msg.Topic()
			index := strings.LastIndexAny(tpc, "/")
			msgCounter := message.Parser.GetMessageCounter(msg)
			fmt.Printf("%v: topic: %v, msgId: %v, message counter: %v, current redelivery count: %v\n",
				time.Now().Format(time.RFC3339Nano), msg.Topic()[index:], msg.ID(), msgCounter, msg.RedeliveryCount())
			return handler.StatusBlocking
		},
		handleGoto: message.StatusBlocking.String(),
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
	storedTopic := handleCase.storedTopic
	transferredTopic := handleCase.transferredTopic
	deadTopic := handleCase.deadTopic
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up groundTopic
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
	stats, err := manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	// create listener
	leveledPolicy := &config.LevelPolicy{
		DiscardEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoDiscard.String()),
		DeadEnable:     config.True(), // always set as true
		PendingEnable:  config.ToPointer(handleCase.handleGoto == decider.GotoPending.String()),
		Pending:        handleCase.statusPolicy,
		BlockingEnable: config.ToPointer(handleCase.handleGoto == decider.GotoBlocking.String()),
		Blocking:       handleCase.statusPolicy,
		RetryingEnable: config.ToPointer(handleCase.handleGoto == decider.GotoRetrying.String()),
		Retrying:       handleCase.statusPolicy,
	}
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       groundTopic,
		Level:                       message.L1,
		Levels:                      message.Levels{message.L1, level},
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
	err = listener.StartPremium(ctx, handleCase.handler)
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(handleCase.waitTime)
	// Handle stats
	stats, err = manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// Handle transferred stats
	if transferredTopic != "" {
		// wait for decide the message
		time.Sleep(10000 * time.Millisecond)
		stats, err = manager.Stats(transferredTopic)
		assert.Nil(t, err)
		assert.Equal(t, handleCase.expectedStatusReentrantTimes, stats.MsgInCounter)
		assert.Equal(t, stats.MsgInCounter, stats.MsgOutCounter)
		if handleCase.handleGoto == decider.GotoPending.String() ||
			handleCase.handleGoto == decider.GotoBlocking.String() ||
			handleCase.handleGoto == decider.GotoRetrying.String() {
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 0, v.MsgBacklog)
				break
			}
		}
		stats, err = manager.Stats(deadTopic)
		assert.Nil(t, err)
		assert.Equal(t, handleCase.expectedDeadMsgCount, stats.MsgInCounter)
		assert.Equal(t, 0, stats.MsgOutCounter)
	}
	// stop listener
	cancel()
}
