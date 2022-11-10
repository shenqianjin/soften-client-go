package soften

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
)

type finalStatusDecider struct {
	logger          log.Logger
	metricsProvider *internal.MetricsProvider
	options         finalStatusDeciderOptions
}

type finalStatusDeciderOptions struct {
	groundTopic  string
	subscription string
	msgGoto      internal.DecideGoto
}

func newFinalStatusDecider(parentLog log.Logger, options finalStatusDeciderOptions, metricsProvider *internal.MetricsProvider) (*finalStatusDecider, error) {
	if options.msgGoto == "" {
		return nil, errors.New("final message status cannot be empty")
	}
	if options.msgGoto != decider.GotoDone && options.msgGoto != decider.GotoDiscard {
		return nil, errors.New(fmt.Sprintf("%s is not a final message goto action", options.msgGoto))
	}
	d := &finalStatusDecider{logger: parentLog.SubLogger(log.Fields{"goto": options.msgGoto}), options: options, metricsProvider: metricsProvider}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, d.options.msgGoto).DecidersOpened.Inc()
	return d, nil

}

func (d *finalStatusDecider) Decide(ctx context.Context, msg consumerMessage, cheStatus checker.CheckStatus) (success bool) {
	if !cheStatus.IsPassed() {
		return false
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	switch d.options.msgGoto {
	case decider.GotoDone:
		msg.Ack()
		msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		logEntry.Debugf("Success to decide message as done: %v", msg.Message.ID())
		success = true
	case decider.GotoDiscard:
		msg.Ack()
		msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		logEntry.Debugf("Success to decide message as discard: %v", msg.Message.ID())
		success = true
	}
	if success {
		now := time.Now()
		msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		if !msg.PublishTime().IsZero() {
			msg.internalExtra.messagesEndMetrics.EndLatencyFromPublish.Observe(now.Sub(msg.PublishTime()).Seconds())
		}
		if !msg.EventTime().IsZero() && msg.EventTime().After(internal.EarliestEventTime) {
			msg.internalExtra.messagesEndMetrics.EndLatencyFromEvent.Observe(now.Sub(msg.EventTime()).Seconds())
		}
	}
	return success
}

func (d *finalStatusDecider) close() {
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, d.options.msgGoto).DecidersOpened.Dec()
}
