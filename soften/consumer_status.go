package soften

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
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
	decider         internalConsumeDecider
}

type statusConsumerOptions struct {
	level   internal.TopicLevel
	status  internal.MessageStatus
	policy  *config.StatusPolicy
	metrics *internal.ListenerConsumerMetrics
}

func newStatusConsumer(parentLogger log.Logger, pulsarConsumer pulsar.Consumer, options statusConsumerOptions, decider internalConsumeDecider) *statusConsumer {
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
			sc.statusMessageCh <- consumerMsg
			continue
		}

		// process for only consumeTime: application should ensure the delay intervals are the same for all messages
		reentrantTime := message.Parser.GetReentrantTime(msg)
		if reentrantTime.IsZero() || sc.status == message.StatusReady { // not support reentrant time for ready status
			// wait to reentrant time
			if consumeTime.After(now) {
				time.Sleep(consumeTime.Sub(now))
			}
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
			sc.statusMessageCh <- consumerMsg
			continue
		}
		// Nack or wait by hanging it in memory util meet the reconsume time if consumeTime is before `now + policy.ReentrantDelay`
		maxNackDelayAt := now.Add(time.Duration(sc.policy.ReentrantDelay))
		if consumeTime.Before(maxNackDelayAt) {
			// do not increase reconsume times if it is after less than one reentrant delay
			msg.Consumer.NackLater(msg, maxNackDelayAt.Sub(now))
			consumerMsg.internalExtra.consumerMetrics.ConsumeMessageNacks.Inc()
			continue
		}
		// reentrant again util meet the reconsume time
		if ok := sc.decider.Decide(consumerMsg, checker.CheckStatusPassed.WithGotoExtra(decider.GotoExtra{ConsumeTime: consumeTime})); ok {
			continue
		}
		// delivery the msg to client as well for other cases, such as decide failed
		sc.statusMessageCh <- consumerMsg
	}
}

func (sc *statusConsumer) StatusChan() <-chan consumerMessage {
	return sc.statusMessageCh
}

func (sc *statusConsumer) Close() {
	sc.Consumer.Close()
	sc.metrics.ConsumersOpened.Desc()
	sc.logger.Infof("closed %v status consumer", sc.status)
}
