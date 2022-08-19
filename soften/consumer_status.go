package soften

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

// statusConsumer received messages from a specified status topic.
// It referred to pulsar.Consumer.
type statusConsumer struct {
	pulsar.Consumer
	logger          log.Logger
	status          internal.MessageStatus
	policy          *config.StatusPolicy
	statusMessageCh chan ConsumerMessage // channel used to deliver message to clients
	decider         internalDecider
}

func newStatusConsumer(parentLogger log.Logger, pulsarConsumer pulsar.Consumer, status internal.MessageStatus, policy *config.StatusPolicy, handler internalDecider) *statusConsumer {
	sc := &statusConsumer{
		logger:          parentLogger.SubLogger(log.Fields{"status": status}),
		Consumer:        pulsarConsumer,
		status:          status,
		policy:          policy,
		decider:         handler,
		statusMessageCh: make(chan ConsumerMessage, 10),
	}
	go sc.start()
	sc.logger.Info("created status consumer")
	return sc
}

func (sc *statusConsumer) start() {
	for {
		// block to read pulsar chan
		msg := <-sc.Consumer.Chan()
		reentrantTime := message.Parser.GetReentrantTime(msg)

		// consume ready message immediately
		if sc.status == message.StatusReady || reentrantTime.IsZero() {
			sc.statusMessageCh <- ConsumerMessage{
				ConsumerMessage: msg,
				StatusMessage:   &statusMessage{status: sc.status},
			}
			continue
		}

		// wait to reentrant time
		now := time.Now().UTC()
		if reentrantTime.After(now) {
			time.Sleep(reentrantTime.Sub(now))
		}

		reconsumeTime := message.Parser.GetReconsumeTime(msg)
		now = time.Now().UTC()
		// delivery message to client if reconsumeTime doesn't exist or before/equal now
		// 'not exist' means it is consumed first time, 'before/equal now' means it is time to consume
		if reconsumeTime.IsZero() || !reconsumeTime.After(now) {
			sc.statusMessageCh <- ConsumerMessage{
				ConsumerMessage: msg,
				StatusMessage:   &statusMessage{status: sc.status},
			}
			continue
		}

		// Nack or wait by hang it in memory util meet the reconsume time if reconsumeTime is before `now + policy.MaxNackDelay`
		maxNackDelayAt := time.Now().UTC().Add(time.Duration(sc.policy.ReentrantDelay))
		if reconsumeTime.Before(maxNackDelayAt) {
			// do not increase reconsume times if it is after less than one reentrant delay
			msg.Consumer.NackLater(msg, maxNackDelayAt.Sub(reconsumeTime))
			continue
		}

		// reentrant again util meet the reconsume time
		if ok := sc.decider.Decide(msg, checker.CheckStatusPassed); ok {
			continue
		}

		// let application handle other case: such as reentrant failed
		sc.statusMessageCh <- ConsumerMessage{
			ConsumerMessage: msg,
			StatusMessage:   &statusMessage{status: sc.status},
		}
	}
}

func (sc *statusConsumer) StatusChan() <-chan ConsumerMessage {
	return sc.statusMessageCh
}

func (sc *statusConsumer) Close() {
	sc.Consumer.Close()
	sc.logger.Info("created status consumer")
}
