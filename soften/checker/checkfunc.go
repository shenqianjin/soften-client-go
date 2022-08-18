package checker

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

// ------ check func (for consumer handle message) ------

type PrevHandleCheckFunc func(msg pulsar.Message) CheckStatus

type PostHandleCheckFunc func(msg pulsar.Message, err error) CheckStatus

// ------ intercept func (for producer send message) ------

type PrevSendCheckFunc func(msg *pulsar.ProducerMessage) CheckStatus

// ------ check status interface ------

type CheckStatus interface {
	IsPassed() bool
	GetHandledDefer() func()

	// extra for reroute checker

	GetRerouteTopic() string
}
