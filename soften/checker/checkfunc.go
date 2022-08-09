package checker

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

// ------ check func (for consumer handle message) ------

type PrevHandleCheckFunc func(pulsar.Message) CheckStatus

type PostHandleCheckFunc func(pulsar.Message, error) CheckStatus

// ------ intercept func (for producer send message) ------

type PrevSendCheckFunc func(msg *pulsar.ProducerMessage) CheckStatus

// ------ check status interface ------

type CheckStatus interface {
	IsPassed() bool
	GetHandledDefer() func()

	// extra for reroute checker

	GetRerouteTopic() string
}
