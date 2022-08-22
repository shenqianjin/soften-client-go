package message

import (
	"fmt"
	"strconv"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ message updater ------

var Helper = &messageHelper{}

type messageHelper struct {
}

func (h *messageHelper) InjectOriginTopic(msg pulsar.ConsumerMessage, properties *map[string]string) {
	props := *properties
	if _, ok := props[XPropertyOriginTopic]; !ok {
		props[XPropertyOriginTopic] = msg.Message.Topic()
	}
}

func (h *messageHelper) InjectOriginLevel(msg pulsar.ConsumerMessage, properties *map[string]string) {
	props := *properties
	if _, ok := props[XPropertyOriginLevel]; !ok {
		if leveledMsg, ok2 := msg.Message.(internal.LeveledMessage); ok2 {
			props[XPropertyOriginLevel] = leveledMsg.Level().String()
			return
		}
	}
}

func (h *messageHelper) InjectOriginStatus(msg pulsar.ConsumerMessage, properties *map[string]string) {
	props := *properties
	if _, ok := props[XPropertyOriginStatus]; !ok {
		if statusMsg, ok2 := msg.Message.(internal.StatusMessage); ok2 {
			props[XPropertyOriginStatus] = statusMsg.Status().String()
			return
		}
	}
}

func (h *messageHelper) InjectOriginMessageId(msg pulsar.ConsumerMessage, properties *map[string]string) {
	props := *properties
	if _, ok := props[XPropertyOriginMessageID]; !ok {
		props[XPropertyOriginMessageID] = Parser.GetMessageId(msg)
	}
}

func (h *messageHelper) InjectOriginPublishTime(msg pulsar.ConsumerMessage, properties *map[string]string) {
	props := *properties
	if _, ok := props[XPropertyOriginPublishTime]; !ok {
		props[XPropertyOriginPublishTime] = msg.PublishTime().UTC().Format(internal.RFC3339TimeInSecondPattern)
	}
}

func (h *messageHelper) InjectPreviousStatus(msg pulsar.ConsumerMessage, properties *map[string]string) {
	props := *properties
	previousStatus := Parser.GetPreviousStatus(msg)
	currentStatus := Parser.GetCurrentStatus(msg)
	if previousStatus != "" && currentStatus != previousStatus {
		props[XPropertyPreviousMessageStatus] = string(currentStatus)
	}
}

func (h *messageHelper) InjectPreviousLevel(msg pulsar.ConsumerMessage, properties *map[string]string) {
	props := *properties
	previousLevel := Parser.GetPreviousLevel(msg)
	currentLevel := Parser.GetCurrentLevel(msg)
	if currentLevel != "" && currentLevel != previousLevel {
		props[XPropertyPreviousMessageLevel] = string(currentLevel)
	}
}

func (h *messageHelper) InjectStatusReconsumeTimes(status internal.MessageStatus, statusReconsumeTimes int, properties *map[string]string) {
	statusReconsumeTimesHeader, ok := statusConsumeTimesMap[status]
	if !ok {
		panic(fmt.Sprintf("invalid status for statusConsumeTimes: %s", status))
	}
	props := *properties
	props[statusReconsumeTimesHeader] = strconv.Itoa(statusReconsumeTimes)
}

func (h *messageHelper) InjectStatusReentrantTimes(status internal.MessageStatus, statusReentrantTimes int, properties *map[string]string) {
	statusReentrantTimesHeader, ok := statusReentrantTimesMap[status]
	if !ok {
		panic(fmt.Sprintf("invalid status for statusConsumeTimes: %s", status))
	}
	props := *properties
	props[statusReentrantTimesHeader] = strconv.Itoa(statusReentrantTimes)
}
