package soften

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/interceptor"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/sirupsen/logrus"
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

	discard  *config.DiscardPolicy
	done     *config.DonePolicy
	logLevel logrus.Level

	leveledInterceptorsMap map[internal.TopicLevel]interceptor.ConsumeInterceptors
}

func newFinalStatusDecider(parentLog log.Logger, options finalStatusDeciderOptions, metricsProvider *internal.MetricsProvider) (*finalStatusDecider, error) {
	if options.msgGoto == "" {
		return nil, errors.New("final message status cannot be empty")
	}
	if options.msgGoto != internal.GotoDone && options.msgGoto != internal.GotoDiscard {
		return nil, errors.New(fmt.Sprintf("%s is not a final message goto action", options.msgGoto))
	}
	if options.msgGoto == internal.GotoDiscard {
		if options.discard == nil {
			return nil, errors.New("missing discard policy for final decider")
		}
		if options.discard.LogLevel != "" {
			if logLvl, err := logrus.ParseLevel(options.discard.LogLevel); err != nil {
				return nil, err
			} else {
				options.logLevel = logLvl
			}
		}
	} else if options.msgGoto == internal.GotoDone {
		if options.done == nil {
			return nil, errors.New("missing done policy for final decider")
		}
		if options.done.LogLevel != "" {
			if logLvl, err := logrus.ParseLevel(options.done.LogLevel); err != nil {
				return nil, err
			} else {
				options.logLevel = logLvl
			}
		}
	}

	d := &finalStatusDecider{logger: parentLog.SubLogger(log.Fields{"goto": options.msgGoto}), options: options, metricsProvider: metricsProvider}
	d.metricsProvider.GetListenerDecidersMetrics(d.options.groundTopic, d.options.subscription, d.options.msgGoto).DecidersOpened.Inc()
	return d, nil

}

func (d *finalStatusDecider) Decide(ctx context.Context, msg consumerMessage, decision decider.Decision) (success bool) {
	if decision.GetGoto() != d.options.msgGoto {
		return false
	}
	// execute on decide interceptors
	if len(d.options.leveledInterceptorsMap[msg.Level()]) > 0 {
		d.options.leveledInterceptorsMap[msg.Level()].OnDecide(ctx, msg, decision)
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	switch d.options.msgGoto {
	case internal.GotoDone:
		msg.Ack()
		msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		if d.options.logLevel >= logrus.InfoLevel {
			logEntry.Infof("Success to decide message as done: %v from topic: %v", msg.Message.ID(), msg.Topic())
		}
		success = true
	case internal.GotoDiscard:
		msg.Ack()
		msg.internalExtra.consumerMetrics.ConsumeMessageAcks.Inc()
		if d.options.logLevel >= logrus.InfoLevel {
			logEntry.Infof("Success to decide message as discard: %v from topic: %v", msg.Message.ID(), msg.Topic())
		}
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
