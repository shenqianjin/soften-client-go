package message

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ message updater ------

var Helper = &messageHelper{}

type messageHelper struct {
}

func (h *messageHelper) InjectOriginTopic(msg pulsar.Message, props map[string]string) {
	if _, ok := props[XPropertyOriginTopic]; !ok {
		props[XPropertyOriginTopic] = msg.Topic()
	}
}

func (h *messageHelper) InjectOriginLevel(msg pulsar.Message, props map[string]string) {
	if _, ok := props[XPropertyOriginLevel]; !ok {
		if leveledMsg, ok2 := msg.(internal.LeveledMessage); ok2 {
			props[XPropertyOriginLevel] = leveledMsg.Level().String()
			return
		}
	}
}

func (h *messageHelper) InjectOriginStatus(msg pulsar.Message, props map[string]string) {
	if _, ok := props[XPropertyOriginStatus]; !ok {
		if statusMsg, ok2 := msg.(internal.StatusMessage); ok2 {
			props[XPropertyOriginStatus] = statusMsg.Status().String()
			return
		}
	}
}

func (h *messageHelper) InjectOriginMessageId(msg pulsar.Message, props map[string]string) {
	if _, ok := props[XPropertyOriginMessageID]; !ok {
		props[XPropertyOriginMessageID] = Parser.GetMessageId(msg)
	}
}

func (h *messageHelper) InjectOriginPublishTime(msg pulsar.Message, props map[string]string) {
	if _, ok := props[XPropertyOriginPublishTime]; !ok {
		props[XPropertyOriginPublishTime] = msg.PublishTime().UTC().Format(internal.RFC3339TimeInSecondPattern)
	}
}

func (h *messageHelper) InjectPreviousStatus(msg pulsar.Message, props map[string]string) {
	previousStatus := Parser.GetPreviousStatus(msg)
	currentStatus := Parser.GetCurrentStatus(msg)
	if previousStatus != "" && currentStatus != previousStatus {
		props[XPropertyPreviousMessageStatus] = string(currentStatus)
	}
}

func (h *messageHelper) InjectPreviousErrorMessage(props map[string]string, err error) {
	if err != nil {
		errMsg := err.Error()
		if len(errMsg) > 128 {
			errMsg = errMsg[:128]
		}
		if len(errMsg) > 0 {
			props[XPropertyPreviousErrorMessage] = errMsg
		}
	}
}

func (h *messageHelper) InjectPreviousLevel(msg pulsar.Message, props map[string]string) {
	previousLevel := Parser.GetPreviousLevel(msg)
	currentLevel := Parser.GetCurrentLevel(msg)
	if currentLevel != "" && currentLevel != previousLevel {
		props[XPropertyPreviousMessageLevel] = string(currentLevel)
	}
}

func (h *messageHelper) InjectConsumeTime(props map[string]string, consumeTime time.Time) {
	if !consumeTime.IsZero() {
		props[XPropertyConsumeTime] = consumeTime.UTC().Format(internal.RFC3339TimeInSecondPattern)
	}
}

func (h *messageHelper) InjectReentrantTime(props map[string]string, reentrant time.Time) {
	if !reentrant.IsZero() {
		props[XPropertyReentrantTime] = reentrant.UTC().Format(internal.RFC3339TimeInSecondPattern)
	}
}

func (h *messageHelper) InjectMessageCounter(props map[string]string, msgCounter internal.MessageCounter) {
	props[XPropertyMessageCounter] = msgCounter.ToString()
}

func (h *messageHelper) InjectStatusMessageCounter(props map[string]string, status internal.MessageStatus, msgCounter internal.MessageCounter) {
	statusMessageCounterHeader, ok := statusMessageCounterMap[status]
	if !ok {
		panic(fmt.Sprintf("invalid status for statusConsumeTimes: %s", status))
	}
	props[statusMessageCounterHeader] = msgCounter.ToString()
}

func (h *messageHelper) ClearMessageCounter(props map[string]string) {
	delete(props, XPropertyMessageCounter)
}

func (h *messageHelper) ClearStatusMessageCounters(props map[string]string) {
	for _, header := range statusMessageCounterMap {
		delete(props, header)
	}
}
