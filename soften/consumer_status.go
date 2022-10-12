package soften

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

// statusConsumer received messages from a specified status topic.
// It referred to pulsar.Consumer.
type statusConsumer struct {
	pulsar.Consumer
	logger          log.Logger
	level           internal.TopicLevel
	status          internal.MessageStatus
	policy          *config.StatusPolicy
	metrics         *internal.ListenerConsumerMetrics
	statusMessageCh chan consumerMessage // channel used to deliver message to clients
	decider         *statusDecider
}

type statusConsumerOptions struct {
	level   internal.TopicLevel
	status  internal.MessageStatus
	policy  *config.StatusPolicy
	metrics *internal.ListenerConsumerMetrics
}

func newStatusConsumer(parentLogger log.Logger, pulsarConsumer pulsar.Consumer, options statusConsumerOptions, decider *statusDecider) *statusConsumer {
	sc := &statusConsumer{
		logger:          parentLogger.SubLogger(log.Fields{"status": options.status}),
		Consumer:        pulsarConsumer,
		level:           options.level,
		status:          options.status,
		policy:          options.policy,
		metrics:         options.metrics,
		decider:         decider,
		statusMessageCh: make(chan consumerMessage, 10),
	}
	go sc.start()
	sc.metrics.ConsumersOpened.Inc()
	sc.logger.Infof("created %v status consumer", options.status)
	return sc
}

func (sc *statusConsumer) start() {
	for {
		// block to read pulsar chan
		msg := <-sc.Consumer.Chan()
		// record metrics
		receivedTime := time.Now()
		if !msg.PublishTime().IsZero() {
			sc.metrics.ReceiveLatencyFromPublish.Observe(time.Since(msg.PublishTime()).Seconds())
		}

		// original map to reentrant
		originalProps := make(map[string]string)
		for k, v := range msg.Properties() {
			originalProps[k] = v
		}

		// prepare message
		consumerMsg := consumerMessage{
			Consumer: msg.Consumer,
			Message: &messageImpl{
				Message:        msg.Message,
				StatusMessage:  &statusMessage{status: sc.status},
				LeveledMessage: &leveledMessage{level: sc.level},
			},
			internalExtra: &internalExtraMessage{receivedTime: receivedTime, consumerMetrics: sc.metrics},
		}

		consumeTime := message.Parser.GetConsumeTime(msg)
		now := time.Now().UTC()

		// consume ready message immediately
		// (1) zero consume time
		// (2) consume time is before / equal now
		if consumeTime.IsZero() || !consumeTime.After(now) {
			sc.increaseConsumeTimes(consumerMsg)
			sc.statusMessageCh <- consumerMsg
			continue
		}

		reentrantTime := message.Parser.GetReentrantTime(msg) // it appears in [pending,blocking,retrying] statuses.
		// process for only consumeTime: application should ensure the delay intervals in a real topic are the same for all messages
		// (1) ready messages: not support reentrant time for ready status
		// (2) zero reentrant time: no need wait for listening
		if reentrantTime.IsZero() || sc.status == message.StatusReady {
			// wait to consume time
			if consumeTime.After(now) {
				time.Sleep(consumeTime.Sub(now))
			}
			sc.increaseConsumeTimes(consumerMsg)
			sc.statusMessageCh <- consumerMsg
			continue
		}

		// process for reentrantTime
		// wait to reentrant time
		if reentrantTime.After(now) {
			time.Sleep(reentrantTime.Sub(now))
		}
		now = time.Now().UTC()
		// delivery message to client if consumeTime is before/equal now.
		if !consumeTime.After(now) {
			sc.increaseConsumeTimes(consumerMsg)
			sc.statusMessageCh <- consumerMsg
			continue
		}
		// TODO: Nack later if the wait time is less than one reentrant delay
		/*// Nack or wait by hanging it in memory util meet the reconsume time if consumeTime is before `now + policy.ReentrantDelay`
		maxNackDelayAt := now.Add(time.Duration(sc.policy.ReentrantDelay))
		if consumeTime.Before(maxNackDelayAt) {
			// do not increase reconsume times if it is after less than one reentrant delay
			msg.Consumer.NackLater(msg, maxNackDelayAt.Sub(now))
			consumerMsg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
			continue
		}*/
		// reentrant again util meet the reconsume time
		if ok := sc.decider.Reentrant(consumerMsg, originalProps); ok {
			continue
		}
		// delivery the msg to client as well for other cases, such as decide failed
		sc.increaseConsumeTimes(consumerMsg)
		sc.statusMessageCh <- consumerMsg
	}
}

func (sc *statusConsumer) increaseConsumeTimes(msg consumerMessage) {
	msgCounter := message.Parser.GetMessageCounter(msg.Message)
	statusMsgCounter := message.Parser.GetStatusMessageCounter(sc.status, msg.Message)
	// please note if topic ownership changed, msg.RedeliveryCount() will be recalculated
	incr := int(msg.RedeliveryCount()) + 1

	// increase consume times
	msgCounter.ConsumeTimes += incr
	// increase reckon times only if consume times limit is enable on current status
	if sc.status == message.StatusReady || sc.policy.ConsumeMaxTimes > 0 {
		msgCounter.ConsumeReckonTimes += incr
	}
	message.Helper.InjectMessageCounter(msg.Message.Properties(), msgCounter)

	// increase status consume times
	statusMsgCounter.ConsumeTimes += incr
	// increase status reckon times only if consume times limit is enable on current status
	if sc.status == message.StatusReady || sc.policy.ConsumeMaxTimes > 0 {
		statusMsgCounter.ConsumeReckonTimes += incr
	}
	message.Helper.InjectStatusMessageCounter(msg.Message.Properties(), sc.status, statusMsgCounter)
}

func (sc *statusConsumer) StatusChan() <-chan consumerMessage {
	return sc.statusMessageCh
}

func (sc *statusConsumer) Close() {
	sc.Consumer.Close()
	sc.metrics.ConsumersOpened.Desc()
	sc.logger.Infof("closed %v status consumer", sc.status)
}
