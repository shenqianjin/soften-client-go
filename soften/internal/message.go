package internal

import (
	"fmt"
	"strings"
)

// ------ message status ------

type MessageStatus string

const (
	DefaultMessageStatusReady = "Ready"
	DefaultMessageStatusDead  = "Dead"
)

func (status MessageStatus) String() string {
	return string(status)
}

func (status MessageStatus) TopicSuffix() string {
	if status == DefaultMessageStatusReady {
		return ""
	} else if status == DefaultMessageStatusDead {
		return "-DLQ"
	} else {
		return "-" + strings.ToUpper(string(status))
	}
}

// ------ status message interface ------

type StatusMessage interface {
	Status() MessageStatus
}

// ------ leveled message interface ------

type LeveledMessage interface {
	Level() TopicLevel
}

// ------ message couter ------

type MessageCounter struct {
	PublishTimes       int // 入队次数
	ConsumeTimes       int // 消费次数
	ConsumeReckonTimes int // 消费计数次数
}

func (c *MessageCounter) ToString() string {
	return fmt.Sprintf("%d:%d:%d", c.PublishTimes, c.ConsumeTimes, c.ConsumeReckonTimes)
}
