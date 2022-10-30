package producer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"

	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

func TestProduceSendSync_Single(t *testing.T) {
	testProduceSendSync(t, 1)
}

func TestProduceSendSync_Batch10(t *testing.T) {
	testProduceSendSync(t, 10)
}

func testProduceSendSync(t *testing.T, count int) {
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
		Topic: groundTopic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 1; i <= count; i++ {
		msg := internal.GenerateProduceMessage(internal.Size64)
		msg.Properties["Index"] = strconv.Itoa(i)
		mid, err := producer.Send(context.Background(), msg)
		fmt.Printf("sync message %v, msgId: %v\n", i, mid)
		assert.Nil(t, err)
	}

	stats, err := manager.Stats(groundTopic)
	assert.Nil(t, err)
	assert.Equal(t, count, stats.MsgInCounter)
}
