package soften

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/internal/strategy"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

// ------ Route message ------

type RouteMessage struct {
	producerMsg *pulsar.ProducerMessage
	callback    func(messageID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error)
}

// ------ soften consumer message ------

type consumerMessage struct {
	pulsar.Consumer
	message.Message
	internalExtra *internalExtraMessage
}

func (msg *consumerMessage) Ack() {
	err := msg.Consumer.Ack(msg)
	for err != nil { // SDK 内部错误, 重试
		msg.internalExtra.consumerMetrics.ConsumeMessageAckErrs.Inc()
		err = msg.Consumer.Ack(msg.Message)
	}
}

type messageImpl struct {
	pulsar.Message
	internal.StatusMessage
	internal.LeveledMessage
}

// ---------------------------------------

// ------ status message implementation ------

type statusMessage struct {
	status internal.MessageStatus
}

func (m *statusMessage) Status() internal.MessageStatus {
	return m.status
}

// ------ leveled message implementation ------

type leveledMessage struct {
	level internal.TopicLevel
}

func (m *leveledMessage) Level() internal.TopicLevel {
	return m.level
}

// ------ message extra info ------

type internalExtraMessage struct {
	receivedTime       time.Time
	listenedTime       time.Time
	prevCheckBeginTime time.Time

	consumerMetrics    *internal.ListenerConsumerMetrics
	deciderMetrics     *internal.ListenerDeciderMetrics
	messagesEndMetrics *internal.ListenerMessagesMetrics

	deferFunc func()
}

// ------ message receiver help ------

var messageChSelector = &messageChSelectorImpl{}

type messageChSelectorImpl struct {
}

func (mcs *messageChSelectorImpl) receiveAny(chs []<-chan consumerMessage) (consumerMessage, bool) {
	/*select {
	case msg, ok := <-chs[0]:
		return msg, ok
	case msg, ok := <-chs[1]:
		return msg, ok
	case msg, ok := <-chs[2]:
		return msg, ok
	case msg, ok := <-chs[3]:
		return msg, ok
	}*/
	cases := make([]reflect.SelectCase, len(chs))
	for i, ch := range chs {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	_, value, ok := reflect.Select(cases)
	// ok will be true if the channel has not been closed.
	if rv, valid := value.Interface().(consumerMessage); !valid {
		panic(fmt.Sprintf("convert %v to consumerMessage failed", value))
	} else {
		return rv, ok
	}
}

func (mcs *messageChSelectorImpl) receiveOneByWeight(chs []<-chan consumerMessage, balanceStrategy strategy.IBalanceStrategy, excludedIndexes *[]int) (consumerMessage, bool) {
	if len(*excludedIndexes) >= len(chs) {
		excludedIndexes = &[]int{}
		return mcs.receiveAny(chs)
	}
	index := balanceStrategy.Next(*excludedIndexes...)
	select {
	case msg, ok := <-chs[index]:
		return msg, ok
	default:
		*excludedIndexes = append(*excludedIndexes, index)
		return mcs.receiveOneByWeight(chs, balanceStrategy, excludedIndexes)
	}
}

// ------ helper ------

var checkTypeGotoMap = map[checker.CheckType]internal.DecideGoto{
	checker.CheckTypePrevDiscard:  internal.GotoDiscard,
	checker.CheckTypePrevDead:     internal.GotoDead,
	checker.CheckTypePrevUpgrade:  internal.GotoUpgrade,
	checker.CheckTypePrevDegrade:  internal.GotoDegrade,
	checker.CheckTypePrevShift:    internal.GotoShift,
	checker.CheckTypePrevBlocking: internal.GotoBlocking,
	checker.CheckTypePrevPending:  internal.GotoPending,
	checker.CheckTypePrevRetrying: internal.GotoRetrying,
	checker.CheckTypePrevTransfer: internal.GotoTransfer,

	checker.CheckTypePostDiscard:  internal.GotoDiscard,
	checker.CheckTypePostDead:     internal.GotoDead,
	checker.CheckTypePostUpgrade:  internal.GotoUpgrade,
	checker.CheckTypePostDegrade:  internal.GotoDegrade,
	checker.CheckTypePostShift:    internal.GotoShift,
	checker.CheckTypePostBlocking: internal.GotoBlocking,
	checker.CheckTypePostPending:  internal.GotoPending,
	checker.CheckTypePostRetrying: internal.GotoRetrying,
	checker.CheckTypePostTransfer: internal.GotoTransfer,

	checker.ProduceCheckTypeDiscard:  internal.GotoDiscard,
	checker.ProduceCheckTypeDead:     internal.GotoDead,
	checker.ProduceCheckTypeUpgrade:  internal.GotoUpgrade,
	checker.ProduceCheckTypeDegrade:  internal.GotoDegrade,
	checker.ProduceCheckTypeShift:    internal.GotoShift,
	checker.ProduceCheckTypeTransfer: internal.GotoTransfer,
}

// ------ log payload formatter ------

func formatPayloadLogContent(payload []byte) string {
	result := string(payload)
	if len(result) > 1024 {
		result = result[0:1024]
	}
	return result
}
