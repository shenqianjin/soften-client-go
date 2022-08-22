package soften

import (
	"fmt"
	"reflect"

	"github.com/shenqianjin/soften-client-go/soften/handler"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/internal/strategy"
)

// ------ re-reRouter message ------

type RerouteMessage struct {
	producerMsg pulsar.ProducerMessage
	consumerMsg pulsar.ConsumerMessage
}

// ------ custom consumer message ------

type ConsumerMessage struct {
	pulsar.ConsumerMessage
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

// ------ message receiver help ------

var messageChSelector = &messageChSelectorImpl{}

type messageChSelectorImpl struct {
}

func (mcs *messageChSelectorImpl) receiveAny(chs []<-chan ConsumerMessage) (ConsumerMessage, bool) {
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
	if rv, valid := value.Interface().(ConsumerMessage); !valid {
		panic(fmt.Sprintf("convert %v to ConsumerMessage failed", value))
	} else {
		return rv, ok
	}
}

func (mcs *messageChSelectorImpl) receiveOneByWeight(chs []<-chan ConsumerMessage, balanceStrategy strategy.IBalanceStrategy, excludedIndexes *[]int) (ConsumerMessage, bool) {
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

var internalGotoReroute = internal.HandleGoto("Reroute") // for consumer
var internalGotoRoute = internal.HandleGoto("Route")     // for producer

var checkTypeGotoMap = map[checker.CheckType]internal.HandleGoto{
	checker.CheckTypePrevDiscard:  handler.GotoDiscard,
	checker.CheckTypePrevDead:     handler.GotoDead,
	checker.CheckTypePrevUpgrade:  handler.GotoUpgrade,
	checker.CheckTypePrevDegrade:  handler.GotoDegrade,
	checker.CheckTypePrevBlocking: handler.GotoBlocking,
	checker.CheckTypePrevPending:  handler.GotoPending,
	checker.CheckTypePrevRetrying: handler.GotoRetrying,
	checker.CheckTypePrevReroute:  internalGotoReroute,

	checker.CheckTypePostDiscard:  handler.GotoDiscard,
	checker.CheckTypePostDead:     handler.GotoDead,
	checker.CheckTypePostUpgrade:  handler.GotoUpgrade,
	checker.CheckTypePostDegrade:  handler.GotoDegrade,
	checker.CheckTypePostBlocking: handler.GotoBlocking,
	checker.CheckTypePostPending:  handler.GotoPending,
	checker.CheckTypePostRetrying: handler.GotoRetrying,
	checker.CheckTypePostReroute:  internalGotoReroute,

	checker.ProduceCheckTypeDiscard:  handler.GotoDiscard,
	checker.ProduceCheckTypeDead:     handler.GotoDead,
	checker.ProduceCheckTypeUpgrade:  handler.GotoUpgrade,
	checker.ProduceCheckTypeDegrade:  handler.GotoDegrade,
	checker.ProduceCheckTypeBlocking: handler.GotoBlocking,
	checker.ProduceCheckTypePending:  handler.GotoPending,
	checker.ProduceCheckTypeRetrying: handler.GotoRetrying,
	checker.ProduceCheckTypeRoute:    internalGotoRoute,
}
