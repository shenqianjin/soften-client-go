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

func (p *messageParser) GetCurrentStatus(msg pulsar.ConsumerMessage) internal.MessageStatus {
	properties := msg.Message.Properties()
	if status, ok := properties[XPropertyCurrentMessageStatus]; ok {
		if messageStatus, err := StatusOf(status); err == nil {
			return messageStatus
		}
	}
	return StatusReady
}

func (p *messageParser) GetPreviousStatus(msg pulsar.ConsumerMessage) internal.MessageStatus {
	properties := msg.Message.Properties()
	if status, ok := properties[XPropertyPreviousMessageStatus]; ok {
		if messageStatus, err := StatusOf(status); err == nil {
			return messageStatus
		}
	}
	return ""
}

func (p *messageParser) GetXReconsumeTimes(msg pulsar.ConsumerMessage) int {
	properties := msg.Message.Properties()
	if timesStr, ok := properties[XPropertyReconsumeTimes]; ok {
		if times, err := strconv.Atoi(timesStr); err == nil {
			return times
		}
	}
	return 0
}

func (p *messageParser) GetReentrantStartRedeliveryCount(msg pulsar.ConsumerMessage) uint32 {
	properties := msg.Message.Properties()
	if timesStr, ok := properties[XPropertyReentrantStartRedeliveryCount]; ok {
		if times, err := strconv.ParseUint(timesStr, 10, 32); err == nil {
			return uint32(times)
		}
	}
	return 0
}

func (p *messageParser) GetStatusReconsumeTimes(status internal.MessageStatus, msg pulsar.ConsumerMessage) int {
	statusConsumeTimesHeader, ok := statusConsumeTimesMap[status]
	if !ok {
		panic("invalid status for statusConsumeTimes")
	}
	properties := msg.Message.Properties()
	if timesStr, ok := properties[statusConsumeTimesHeader]; ok {
		if times, err := strconv.Atoi(timesStr); err == nil {
			return times
		}
	}
	return 0
}

func (p *messageParser) GetStatusReentrantTimes(status internal.MessageStatus, msg pulsar.ConsumerMessage) int {
	reentrantTimesHeader, ok := statusReentrantTimesMap[status]
	if !ok {
		panic("invalid status for statusReentrantTimes")
	}
	properties := msg.Message.Properties()
	if timesStr, ok := properties[reentrantTimesHeader]; ok {
		if times, err := strconv.Atoi(timesStr); err == nil {
			return times
		}
	}
	return 0
}

func (p *messageParser) GetReconsumeTime(msg pulsar.ConsumerMessage) time.Time {
	properties := msg.Message.Properties()
	if timeStr, ok := properties[XPropertyReconsumeTime]; ok {
		if t, err := time.Parse(internal.RFC3339TimeInSecondPattern, timeStr); err == nil {
			return t
		}
	}
	return time.Time{}
}

func (p *messageParser) GetReentrantTime(msg pulsar.ConsumerMessage) time.Time {
	properties := msg.Message.Properties()
	if timeStr, ok := properties[XPropertyReentrantTime]; ok {
		if t, err := time.Parse(internal.RFC3339TimeInSecondPattern, timeStr); err == nil {
			return t
		}
	}
	return time.Time{}
}

func (p *messageParser) GetMessageId(msg pulsar.ConsumerMessage) string {
	id := msg.Message.ID()
	return fmt.Sprintf("%d:%d:%d", id.LedgerID(), id.EntryID(), id.PartitionIdx())
}
