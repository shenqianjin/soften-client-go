package messages

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

func formatMessage4Print(msg pulsar.Message) string {
	if msg == nil {
		return "unknown"
	}
	return formatMessage(msg).String()
}

func formatMessage(msg pulsar.Message) *prettyMessage {
	pm := &prettyMessage{
		mid:         msg.ID(),
		payload:     string(msg.Payload()),
		properties:  msg.Properties(),
		publishTime: msg.PublishTime(),
		eventTime:   msg.EventTime(),
	}
	return pm
}

type prettyMessage struct {
	mid         pulsar.MessageID
	payload     string
	properties  map[string]string
	publishTime time.Time
	eventTime   time.Time
}

func (msg *prettyMessage) String() string {
	if bytes, err := json.Marshal(msg); err != nil {
		logrus.Fatal(err)
		return ""
	} else {
		return string(bytes)
	}
}

func parseTimeString(timeStr string, optionName string) time.Time {
	t := time.Time{}
	if timeStr != "" {
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
	conditions := strings.Split(condition, ",")
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
