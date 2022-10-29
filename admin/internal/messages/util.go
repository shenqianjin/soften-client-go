package messages

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

func formatMessageAsString(msg pulsar.Message) string {
	if msg == nil {
		return ""
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
		panic(err)
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
