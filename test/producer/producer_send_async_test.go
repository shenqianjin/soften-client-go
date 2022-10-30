package producer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestProduceSendAsync_Single(t *testing.T) {
	testProduceSendAsync(t, 1)
}

func TestProduceSendAsync_Batch100(t *testing.T) {
	testProduceSendAsync(t, 100)
}

func testProduceSendAsync(t *testing.T, batch int) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduce)
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)

	// clean up topics
	internal.CleanUpTopics(t, manager, groundTopic)
	defer internal.CleanUpTopics(t, manager, groundTopic)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, []string{groundTopic}, 0)

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic:               groundTopic,
		BatchingMaxMessages: 40,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	wg := sync.WaitGroup{}
	wg.Add(batch)
	for i := 1; i <= batch; i++ {
		msg := internal.GenerateProduceMessage(internal.Size64)
		msg.Properties["Index"] = strconv.Itoa(i)
		func(index int) {
			producer.SendAsync(context.Background(), msg, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
				fmt.Printf("async message %v, msgId: %v\n", index, id)
				assert.Nil(t, err)
				wg.Done()
			})
		}(i)
	}
	wg.Wait()

	stats, err := manager.Stats(groundTopic)
	assert.Nil(t, err)
	assert.Equal(t, batch, stats.MsgInCounter)
}
