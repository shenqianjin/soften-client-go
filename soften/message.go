package soften

import (
	"fmt"
	"reflect"
	"time"

	"github.com/shenqianjin/soften-client-go/soften/message"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/internal/strategy"
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
	checker.CheckTypePrevDiscard:  decider.GotoDiscard,
	checker.CheckTypePrevDead:     decider.GotoDead,
	checker.CheckTypePrevUpgrade:  decider.GotoUpgrade,
	checker.CheckTypePrevDegrade:  decider.GotoDegrade,
	checker.CheckTypePrevShift:    decider.GotoShift,
	checker.CheckTypePrevBlocking: decider.GotoBlocking,
	checker.CheckTypePrevPending:  decider.GotoPending,
	checker.CheckTypePrevRetrying: decider.GotoRetrying,
	checker.CheckTypePrevTransfer: decider.GotoTransfer,

	checker.CheckTypePostDiscard:  decider.GotoDiscard,
	checker.CheckTypePostDead:     decider.GotoDead,
	checker.CheckTypePostUpgrade:  decider.GotoUpgrade,
	checker.CheckTypePostDegrade:  decider.GotoDegrade,
	checker.CheckTypePostShift:    decider.GotoShift,
	checker.CheckTypePostBlocking: decider.GotoBlocking,
	checker.CheckTypePostPending:  decider.GotoPending,
	checker.CheckTypePostRetrying: decider.GotoRetrying,
	checker.CheckTypePostTransfer: decider.GotoTransfer,

	checker.ProduceCheckTypeDiscard:  decider.GotoDiscard,
	checker.ProduceCheckTypeDead:     decider.GotoDead,
	checker.ProduceCheckTypeUpgrade:  decider.GotoUpgrade,
	checker.ProduceCheckTypeDegrade:  decider.GotoDegrade,
	checker.ProduceCheckTypeShift:    decider.GotoShift,
	checker.ProduceCheckTypeTransfer: decider.GotoTransfer,
}

// ------ log payload formatter ------

func formatPayloadLogContent(payload []byte) string {
	result := string(payload)
	if len(result) > 1024 {
		result = result[0:1024]
	}
	return result
}
