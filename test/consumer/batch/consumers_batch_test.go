package batch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestConsumeBatch(t *testing.T) {
	testConsumeBatch(t, 10)
}

func testConsumeBatch(t *testing.T, count int) {
	topic := internal.GenerateTestTopic(internal.PrefixTestConsumeBatch)
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)
	internal.CleanUpTopic(t, manager, topic)
	defer func() {
		internal.CleanUpTopic(t, manager, topic)
	}()
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, []string{topic}, 0)

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	wg := sync.WaitGroup{}
	for i := 1; i <= count; i++ {
		msg := internal.GenerateProduceMessage(internal.Size64)
		msg.Properties["Index"] = strconv.Itoa(i)
		wg.Add(1)
		func(i int) {
			/*mid, err := producer.Send(context.Background(), msg)
			fmt.Printf("sync message %v, msgId: %v\n", i, mid)
			assert.Nil(t, err)
			wg.Done()*/
			producer.SendAsync(context.Background(), msg, func(id pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
				fmt.Printf("sync message %v, msgId: %v\n", i, id)
				assert.Nil(t, err)
				wg.Done()
			})
		}(i)
	}
	wg.Wait()

	stats, err := manager.Stats(topic)
	assert.Nil(t, err)
	assert.Equal(t, count, stats.MsgInCounter)

	// consumer
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:                       topic,
		SubscriptionName:            internal.TestSubscriptionName(),
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		NackRedeliveryDelay:         1,
		Type:                        pulsar.Shared,
		EscapeHandleType:            config.EscapeAsNack,
		LevelPolicy:                 &config.LevelPolicy{DeadEnable: config.False()},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// start to listen
	nackLimit := uint32(5)
	var nackCount atomic.Uint32
	wg2 := sync.WaitGroup{}
	wg2.Add(count)
	listener.Start(context.Background(), func(ctx context.Context, msg message.Message) (success bool, err error) {
		msgCounter := message.Parser.GetMessageCounter(msg)
		index := msg.Properties()["Index"]
		fmt.Printf("%v: index: %v, msgId: %v [%v], message counter: %v, current redelivery count: %v\n",
			time.Now().Format(time.RFC3339Nano), index, msg.ID(), msg.ID().BatchIdx(), msgCounter, msg.RedeliveryCount())
		if index == strconv.Itoa(count/2) {
			if nackCount.Load() <= nackLimit {
				nackCount.Add(1)
				time.Sleep(3 * time.Second)
				return false, errors.New("test nack error")
			}
		}
		//wg2.Done()
		return true, nil
	})
	//wg2.Wait()
	fmt.Println("-------")
	time.Sleep(3000 * time.Millisecond)
}
