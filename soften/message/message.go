package message

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ message interface ------

type Message interface {
	pulsar.Message
	internal.StatusMessage
	internal.LeveledMessage
}

// ConsumerMessage represents a pair of a Consumer and Message.
type ConsumerMessage struct {
	pulsar.Consumer
	Message
}

type ProducerMessage struct {
	*pulsar.ProducerMessage
	internal.LeveledMessage
}
