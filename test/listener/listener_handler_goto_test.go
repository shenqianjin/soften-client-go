package listener

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
	topiclevel "github.com/shenqianjin/soften-client-go/soften/topic"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

type testListenHandleCase struct {
	topic                  string
	storedTopic            string // produce to / consume from
	routedTopic            string // Handle to
	expectedStoredOutCount int    // should always 1
	expectedRoutedOutCount int    // 1 for pending, blocking, retrying; 0 for upgrade, degrade, reroute
	handleGoto             string

	// extra for upgrade/degrade
	upgradeLevel string
	degradeLevel string
}

func TestListenHandle_Done(t *testing.T) {
	topic := internal.GenerateTestTopic()
	HandleCase := testListenHandleCase{
		topic:       topic,
		storedTopic: topic,
		handleGoto:  handler.GotoDone.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Discard(t *testing.T) {
	topic := internal.GenerateTestTopic()
	HandleCase := testListenHandleCase{
		topic:       topic,
		storedTopic: topic,
		handleGoto:  handler.GotoDiscard.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Dead(t *testing.T) {
	topic := internal.GenerateTestTopic()
	HandleCase := testListenHandleCase{
		topic:       topic,
		storedTopic: topic,
		routedTopic: topic + message.StatusDead.TopicSuffix(),
		handleGoto:  handler.GotoDead.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Pending(t *testing.T) {
	topic := internal.GenerateTestTopic()
	HandleCase := testListenHandleCase{
		topic:                  topic,
		storedTopic:            topic,
		routedTopic:            topic + message.StatusPending.TopicSuffix(),
		handleGoto:             handler.GotoPending.String(),
		expectedRoutedOutCount: 1, // reroute the msg to pending queue, and then reconsume it
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Blocking(t *testing.T) {
	topic := internal.GenerateTestTopic()
	HandleCase := testListenHandleCase{
		topic:                  topic,
		storedTopic:            topic,
		routedTopic:            topic + message.StatusBlocking.TopicSuffix(),
		handleGoto:             handler.GotoBlocking.String(),
		expectedRoutedOutCount: 1,
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Retrying(t *testing.T) {
	topic := internal.GenerateTestTopic()
	HandleCase := testListenHandleCase{
		topic:                  topic,
		storedTopic:            topic,
		routedTopic:            topic + message.StatusRetrying.TopicSuffix(),
		handleGoto:             handler.GotoRetrying.String(),
		expectedRoutedOutCount: 1,
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Degrade(t *testing.T) {
	degradeLevel := topiclevel.B2
	topic := internal.GenerateTestTopic()
	HandleCase := testListenHandleCase{
		topic:        topic,
		storedTopic:  topic,
		routedTopic:  topic + degradeLevel.TopicSuffix(),
		degradeLevel: degradeLevel.String(),
		handleGoto:   handler.GotoDegrade.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func TestListenHandle_Upgrade(t *testing.T) {
	upgradeLevel := topiclevel.L2
	topic := internal.GenerateTestTopic()
	HandleCase := testListenHandleCase{
		topic:        topic,
		storedTopic:  topic,
		routedTopic:  topic + upgradeLevel.TopicSuffix(),
		upgradeLevel: upgradeLevel.String(),
		handleGoto:   handler.GotoUpgrade.String(),
	}
	testListenHandleGoto(t, HandleCase)
}

func testListenHandleGoto(t *testing.T, handleCase testListenHandleCase) {
	topic := handleCase.topic
	storedTopic := handleCase.storedTopic
	routedTopic := handleCase.routedTopic
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up topic
	internal.CleanUpTopic(t, manager, storedTopic)
	internal.CleanUpTopic(t, manager, routedTopic)
	defer func() {
		internal.CleanUpTopic(t, manager, storedTopic)
		internal.CleanUpTopic(t, manager, routedTopic)
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
	msgID, err := producer.Send(context.Background(), internal.GenerateProduceMessage(internal.Size1K))
	assert.Nil(t, err)
	fmt.Println("produced message: ", msgID)
	// Handle send stats
	stats, err := manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgInCounter)

	// ---------------

	// create listener
	upgradeLevel, _ := topiclevel.LevelOf(handleCase.upgradeLevel)
	degradeLevel, _ := topiclevel.LevelOf(handleCase.degradeLevel)
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.GenerateSubscribeNameByTopic(topic),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		DiscardEnable:               handleCase.handleGoto == handler.GotoDiscard.String(),
		DeadEnable:                  handleCase.handleGoto == handler.GotoDead.String(),
		PendingEnable:               handleCase.handleGoto == handler.GotoPending.String(),
		BlockingEnable:              handleCase.handleGoto == handler.GotoBlocking.String(),
		RetryingEnable:              handleCase.handleGoto == handler.GotoRetrying.String(),
		UpgradeEnable:               handleCase.handleGoto == handler.GotoUpgrade.String(),
		DegradeEnable:               handleCase.handleGoto == handler.GotoDegrade.String(),
		//RerouteEnable:               handleCase.handleGoto == message.GotoReroute.String(),
		UpgradeTopicLevel: upgradeLevel,
		DegradeTopicLevel: degradeLevel,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, func(msg pulsar.Message) handler.HandleStatus {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		if handleGoto, err := handler.GotoOf(handleCase.handleGoto); err == nil {
			return handler.HandleStatusBuilder().Goto(handleGoto).Build()
		} else {
			return handler.HandleStatusAuto
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message
	time.Sleep(100 * time.Millisecond)
	// Handle stats
	stats, err = manager.Stats(storedTopic)
	assert.Nil(t, err)
	assert.Equal(t, 1, stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// Handle rerouted stats
	if routedTopic != "" {
		// wait for decide the message
		time.Sleep(100 * time.Millisecond)
		stats, err = manager.Stats(routedTopic)
		assert.Nil(t, err)
		assert.Equal(t, 1, stats.MsgInCounter)
		assert.Equal(t, handleCase.expectedRoutedOutCount, stats.MsgOutCounter)
		if handleCase.handleGoto == handler.GotoPending.String() ||
			handleCase.handleGoto == handler.GotoBlocking.String() ||
			handleCase.handleGoto == handler.GotoRetrying.String() {
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 1, v.MsgBacklog)
				break
			}
		}
	}
	// stop listener
	cancel()
}

func TestListenHandle_All(t *testing.T) {
	upgradeLevel := topiclevel.L2
	degradeLevel := topiclevel.B2
	topic := internal.GenerateTestTopic()
	//reroutedTopic := topic + "-S1"
	decidedTopics := []string{
		"",                                       // done
		"",                                       // discard
		topic + message.StatusDead.TopicSuffix(), // dead
		topic + message.StatusPending.TopicSuffix(),  // pending
		topic + message.StatusBlocking.TopicSuffix(), // blocking
		topic + message.StatusRetrying.TopicSuffix(), // retrying
		topic + upgradeLevel.TopicSuffix(),           // upgrade
		topic + degradeLevel.TopicSuffix(),           // degrade
		//reroutedTopic,                                // reroute
	}
	midConsumedTopics := []string{
		decidedTopics[3],
		decidedTopics[4],
		decidedTopics[5],
	}
	manager := admin.NewAdminManager(internal.DefaultPulsarHttpUrl)
	// clean up topic
	internal.CleanUpTopic(t, manager, topic)
	for _, decidedTopic := range decidedTopics {
		if decidedTopic != "" {
			internal.CleanUpTopic(t, manager, decidedTopic)
		}
	}
	defer func() {
		internal.CleanUpTopic(t, manager, topic)
		for _, decidedTopic := range decidedTopics {
			if decidedTopic != "" {
				internal.CleanUpTopic(t, manager, decidedTopic)
			}
		}
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
	for i := 0; i < len(decidedTopics); i++ {
		msg := internal.GenerateProduceMessage(internal.Size64, "Index", strconv.Itoa(i))
		mid, err := producer.Send(context.Background(), msg)
		assert.Nil(t, err)
		fmt.Println("sent message: ", mid)
	}
	// check send stats
	stats, err := manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, len(decidedTopics), stats.MsgInCounter)
	assert.Equal(t, 0, stats.MsgOutCounter)

	// ---------------

	// create listener
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.GenerateSubscribeNameByTopic(topic),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		DiscardEnable:               true,
		DeadEnable:                  true,
		PendingEnable:               true,
		BlockingEnable:              true,
		RetryingEnable:              true,
		UpgradeEnable:               true,
		DegradeEnable:               true,
		RerouteEnable:               true,
		UpgradeTopicLevel:           upgradeLevel,
		DegradeTopicLevel:           degradeLevel,
		Reroute: &config.ReroutePolicy{
			ConnectInSyncEnable: true},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	// listener starts
	ctx, cancel := context.WithCancel(context.Background())
	err = listener.StartPremium(ctx, func(msg pulsar.Message) handler.HandleStatus {
		fmt.Printf("consumed message size: %v, headers: %v\n", len(msg.Payload()), msg.Properties())
		if index, ok := msg.Properties()["Index"]; ok {
			switch index {
			case "0":
				return handler.HandleStatusBuilder().Goto(handler.GotoDone).Build()
			case "1":
				return handler.HandleStatusBuilder().Goto(handler.GotoDiscard).Build()
			case "2":
				return handler.HandleStatusBuilder().Goto(handler.GotoDead).Build()
			case "3":
				return handler.HandleStatusBuilder().Goto(handler.GotoPending).Build()
			case "4":
				return handler.HandleStatusBuilder().Goto(handler.GotoBlocking).Build()
			case "5":
				return handler.HandleStatusBuilder().Goto(handler.GotoRetrying).Build()
			case "6":
				if statusMsg, ok := msg.(soften.StatusMessage); ok && statusMsg.Status() == message.StatusReady {
					return handler.HandleStatusBuilder().Goto(handler.GotoUpgrade).Build()
				}
			case "7":
				if statusMsg, ok := msg.(soften.StatusMessage); ok && statusMsg.Status() == message.StatusReady {
					return handler.HandleStatusBuilder().Goto(handler.GotoDegrade).Build()
				}
			case "8": // no reroute decider for handle goto
			}
		}
		return handler.HandleStatusOk
	})
	if err != nil {
		log.Fatal(err)
	}
	// wait for consuming the message and wait for decide the message
	time.Sleep(500 * time.Millisecond)
	// check stats
	stats, err = manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, len(decidedTopics), stats.MsgOutCounter)
	assert.Equal(t, stats.MsgOutCounter, stats.MsgInCounter)
	// check decided stats
	for _, decidedTopic := range decidedTopics {
		if decidedTopic == "" {
			continue
		}
		stats, err = manager.Stats(decidedTopic)
		assert.Nil(t, err, "decided topic: ", decidedTopic)
		assert.Equal(t, 1, stats.MsgInCounter, "decided topic: ", decidedTopic)
		isMidConsumeTopic := false
		for _, midConsumedTopic := range midConsumedTopics {
			if decidedTopic == midConsumedTopic {
				isMidConsumeTopic = true
				break
			}
		}
		if isMidConsumeTopic {
			assert.Equal(t, 1, stats.MsgOutCounter)
			for _, v := range stats.Subscriptions {
				assert.Equal(t, 1, v.MsgBacklog, "decided topic: ", decidedTopic)
				break
			}
		} else {
			assert.Equal(t, 0, stats.MsgOutCounter)
		}
	}
	// stop listener
	cancel()
}
