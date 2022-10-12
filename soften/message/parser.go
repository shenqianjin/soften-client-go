package message

import (
	"fmt"
	"strconv"
	"strings"
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

func (p *messageParser) GetMessageCounter(msg pulsar.Message) internal.MessageCounter {
	properties := msg.Properties()
	if timesStr, ok := properties[XPropertyMessageCounter]; ok {
		return p.parseMessageCounter(timesStr)
	}
	return internal.MessageCounter{}
}

func (p *messageParser) parseMessageCounter(timesStr string) internal.MessageCounter {
	countedMsg := internal.MessageCounter{}
	segments := strings.Split(timesStr, ":")
	segmentsLen := len(segments)
	switch segmentsLen {
	case 3:
		if times, err := strconv.Atoi(segments[2]); err == nil {
			countedMsg.ConsumeReckonTimes = times
		}
		fallthrough
	case 2:
		if times, err := strconv.Atoi(segments[1]); err == nil {
			countedMsg.ConsumeTimes = times
		}
		fallthrough
	case 1:
		if times, err := strconv.Atoi(segments[0]); err == nil {
			countedMsg.PublishTimes = times
		}
	}
	return countedMsg
}

func (p *messageParser) GetStatusMessageCounter(status internal.MessageStatus, msg pulsar.Message) internal.MessageCounter {
	statusConsumeTimesHeader, ok := statusMessageCounterMap[status]
	if !ok {
		panic(fmt.Sprintf("invalid status for statusMessageCounter: %s", status))
	}
	properties := msg.Properties()
	if timesStr, ok2 := properties[statusConsumeTimesHeader]; ok2 {
		return p.parseMessageCounter(timesStr)
	}
	return internal.MessageCounter{}
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
