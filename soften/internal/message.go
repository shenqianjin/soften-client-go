package internal

import (
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
