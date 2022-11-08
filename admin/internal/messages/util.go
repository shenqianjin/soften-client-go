package messages

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
)

func parseTimeString(timeStr string, optionName string) time.Time {
	t := time.Time{}
	if timeStr == "" {
	} else if timeStr == "now" || timeStr == "now()" || timeStr == "time.Now()" {
		t = time.Now()
	} else if timeStr != "" {
		var err error
		t, err = time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			logrus.Fatalf("invalid %v option: %v\n", optionName, timeStr)
		}
	}
	return t
}

func parseAndCompileConditions(condition string) []*vm.Program {
	programs := make([]*vm.Program, 0)
	conditions := strings.Split(condition, "\\n")
	for _, c := range conditions {
		if program, err := expr.Compile(c); err != nil {
			logrus.Fatalf("invalid condition: %v, err: %v\n", c, err)
		} else {
			programs = append(programs, program)
		}
	}
	return programs
}

// ------ helper ------

type matchOptions struct {
	conditions       []*vm.Program
	startPublishTime time.Time
	startEventTime   time.Time
}

func matched(msg pulsar.Message, options matchOptions) bool {
	// old publish time
	if !options.startPublishTime.IsZero() {
		if msg.PublishTime().Before(options.startPublishTime) {
			return false
		}
	}
	// old event time
	if !options.startEventTime.IsZero() && !msg.EventTime().IsZero() {
		if msg.EventTime().Before(options.startEventTime) {
			return false
		}
	}
	// unmarshal payload
	var payloadAsMap map[string]interface{}
	if err := json.Unmarshal(msg.Payload(), &payloadAsMap); err != nil {
		logrus.Fatalf("failed unmarshal payload to json. payload: %v, err: %v", string(msg.Payload()), err)
	}
	// check conditions
	for conditionIndex, program := range options.conditions {
		output, err1 := expr.Run(program, payloadAsMap)
		if err1 != nil {
			logrus.Fatalf("failed to check condition: %v, err: %v\n"+
				"payload: %v\n", conditionIndex, err1, string(msg.Payload()))
		}
		if output == true {
			return true
		}
	}
	return false
}

// ------ publish ------

func publish(producer pulsar.Producer, producerMsg *pulsar.ProducerMessage, publishMaxTimes uint64) (pulsar.MessageID, error) {
	var mid pulsar.MessageID
	err := errors.New("dummy error")
	if publishMaxTimes <= 0 {
		// 无限重试
		for err != nil {
			mid, err = producer.Send(context.Background(), producerMsg)
		}
	} else {
		// 指定次数重试
		for i := uint64(1); err != nil && i <= publishMaxTimes; i++ {
			mid, err = producer.Send(context.Background(), producerMsg)
		}
	}
	return mid, err
}

func publishAsync(producer pulsar.Producer, producerMsg *pulsar.ProducerMessage,
	callback func(producerMid pulsar.MessageID, message *pulsar.ProducerMessage, err error),
	publishMaxTimes uint64) {
	callbackNew := func(producerMid pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
		if publishMaxTimes <= 0 {
			for err != nil {
				producerMid, err = producer.Send(context.Background(), producerMsg)
			}
		} else {
			for i := uint64(2); err != nil && i <= publishMaxTimes; i++ {
				producerMid, err = producer.Send(context.Background(), producerMsg)
			}
		}
		callback(producerMid, message, err)
	}
	producer.SendAsync(context.Background(), producerMsg, callbackNew)
}

func querySinglePartitionTopics(webUrl string, topic string) []string {
	topics := make([]string, 0)
	nonPartitionedManager := admin.NewNonPartitionedTopicManager(webUrl)
	_, err := nonPartitionedManager.Stats(topic)
	if err == nil {
		topics = append(topics, topic)
	} else if strings.Contains(err.Error(), admin.Err404NotFound) {
		partitionedManager := admin.NewPartitionedTopicManager(webUrl)
		stat2, err2 := partitionedManager.Stats(topic)
		if err2 != nil || stat2.Metadata.Partitions <= 0 {
			logrus.Fatal("failed to stats to get partitions metadata. err: %v", err)
		}
		for i := 0; i < stat2.Metadata.Partitions; i++ {
			topics = append(topics, topic+"-partition-"+strconv.Itoa(i))
		}
	} else {
		logrus.Fatal(err)
	}
	return topics
}

// ------ helpers ------

const (
	SampleConditionAgeLessEqualThan10                     = `age != nil && age <= 10`
	SampleConditionUidRangeAndNameStartsWithNo12          = `uid > 100 && uid < 200 && name startsWith "No12"`
	SampleConditionSpouseAgeLessThan40                    = `spouse != nil && spouse.age != nil && spouse.age < 40`
	SampleConditionFriendsHasOneOfAgeLessEqualThan10      = `friends != nil && any(friends, {#.age != nil && #.age <= 10})`
	SampleConditionAgeLessEqualThan10OrNameStartsWithNo12 = `age != nil && age <= 10 \n name startsWith "No12"`
)
