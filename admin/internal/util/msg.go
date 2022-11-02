package util

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

func FormatMessage4Print(msg pulsar.Message) string {
	if msg == nil {
		return "unknown"
	}
	return formatMessage(msg).String()
}

func FormatProducerMessage4Print(msg *pulsar.ProducerMessage, mid pulsar.MessageID) string {
	if msg == nil {
		return "unknown"
	}
	return formatProducerMessage(msg, mid).String()
}

func formatMessage(msg pulsar.Message) *prettyMessage {
	pm := &prettyMessage{
		Mid:         fmt.Sprintf("%v", msg.ID()),
		Payload:     string(msg.Payload()),
		Properties:  msg.Properties(),
		PublishTime: msg.PublishTime(),
		EventTime:   msg.EventTime(),
	}
	return pm
}
func formatProducerMessage(msg *pulsar.ProducerMessage, mid pulsar.MessageID) *prettyMessage {
	pm := &prettyMessage{
		Mid:        fmt.Sprintf("%v", mid),
		Payload:    string(msg.Payload),
		Properties: msg.Properties,
		EventTime:  msg.EventTime,
	}
	return pm
}

type prettyMessage struct {
	Mid         string            `json:"mid,omitempty"`
	Payload     string            `json:"payload,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
	PublishTime time.Time         `json:"publishTime,omitempty"`
	EventTime   time.Time         `json:"eventTime,omitempty"`
}

func (msg *prettyMessage) String() string {
	if bytes, err := json.Marshal(msg); err != nil {
		logrus.Fatal(err)
		return ""
	} else {
		return string(bytes)
	}
}
