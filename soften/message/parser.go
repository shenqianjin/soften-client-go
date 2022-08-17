package message

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ message parser ------

var Parser = &messageParser{}

type messageParser struct {
}

func (p *messageParser) GetCurrentLevel(msg pulsar.Message) internal.TopicLevel {
	if leveledMsg, ok := msg.(internal.LeveledMessage); ok {
		return leveledMsg.Level()
	}
	return L1
}

func (p *messageParser) GetPreviousLevel(msg pulsar.Message) internal.TopicLevel {
	properties := msg.Properties()
	if level, ok := properties[XPropertyPreviousMessageLevel]; ok {
		if messageLevel, err := LevelOf(level); err == nil {
			return messageLevel
		}
	}
	return L1
}

func (p *messageParser) GetCurrentStatus(msg pulsar.Message) internal.MessageStatus {
	if statusMsg, ok := msg.(internal.StatusMessage); ok {
		return statusMsg.Status()
	}
	return StatusReady
}

func (p *messageParser) GetPreviousStatus(msg pulsar.Message) internal.MessageStatus {
	properties := msg.Properties()
	if status, ok := properties[XPropertyPreviousMessageStatus]; ok {
		if messageStatus, err := StatusOf(status); err == nil {
			return messageStatus
		}
	}
	return ""
}

func (p *messageParser) GetReconsumeTimes(msg pulsar.Message) int {
	properties := msg.Properties()
	if timesStr, ok := properties[XPropertyReconsumeTimes]; ok {
		if times, err := strconv.Atoi(timesStr); err == nil {
			return times
		}
	}
	return 0
}

func (p *messageParser) GetReentrantStartRedeliveryCount(msg pulsar.Message) uint32 {
	properties := msg.Properties()
	if timesStr, ok := properties[XPropertyReentrantStartRedeliveryCount]; ok {
		if times, err := strconv.ParseUint(timesStr, 10, 32); err == nil {
			return uint32(times)
		}
	}
	return 0
}

func (p *messageParser) GetStatusConsumeTimes(status internal.MessageStatus, msg pulsar.Message) int {
	statusConsumeTimesHeader, ok := statusConsumeTimesMap[status]
	if !ok {
		panic(fmt.Sprintf("invalid status for statusConsumeTimes: %s", status))
	}
	properties := msg.Properties()
	if timesStr, ok := properties[statusConsumeTimesHeader]; ok {
		if times, err := strconv.Atoi(timesStr); err == nil {
			return times
		}
	}
	return 0
}

func (p *messageParser) GetStatusReentrantTimes(status internal.MessageStatus, msg pulsar.Message) int {
	statusReentrantTimesHeader, ok := statusReentrantTimesMap[status]
	if !ok {
		panic("invalid status for statusReentrantTimes")
	}
	properties := msg.Properties()
	if timesStr, ok2 := properties[statusReentrantTimesHeader]; ok2 {
		if times, err := strconv.Atoi(timesStr); err == nil {
			return times
		}
	}
	return 0
}

func (p *messageParser) GetConsumeTime(msg pulsar.Message) time.Time {
	properties := msg.Properties()
	if timeStr, ok := properties[XPropertyConsumeTime]; ok {
		if t, err := time.Parse(internal.RFC3339TimeInSecondPattern, timeStr); err == nil {
			return t
		}
	}
	return time.Time{}
}

func (p *messageParser) GetReentrantTime(msg pulsar.Message) time.Time {
	properties := msg.Properties()
	if timeStr, ok := properties[XPropertyReentrantTime]; ok {
		if t, err := time.Parse(internal.RFC3339TimeInSecondPattern, timeStr); err == nil {
			return t
		}
	}
	return time.Time{}
}

func (p *messageParser) GetMessageId(msg pulsar.Message) string {
	id := msg.ID()
	return fmt.Sprintf("%d:%d:%d", id.LedgerID(), id.EntryID(), id.PartitionIdx())
}
