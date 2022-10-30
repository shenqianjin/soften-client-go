package batch

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

func TestProduceBatch(t *testing.T) {
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestProduceBatch)
	testProduceBatch(t, groundTopic, 1000)
}

func testProduceBatch(t *testing.T, groundTopic string, count int) {
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)

	// clean up topics
	internal.CleanUpTopics(t, manager, groundTopic)
	defer internal.CleanUpTopics(t, manager, groundTopic)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, []string{groundTopic}, 0)

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic: groundTopic,
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

	stats, err := manager.Stats(groundTopic)
	assert.Nil(t, err)
	assert.Equal(t, count, stats.MsgInCounter)
}
