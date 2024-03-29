package soften

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/sirupsen/logrus"
)

type producerFinalDeciderOptions struct {
	groundTopic string
	level       internal.TopicLevel
	discard     *config.DiscardPolicy
	logLevel    logrus.Level
}

type producerFinalDecider struct {
	logger          log.Logger
	options         *producerFinalDeciderOptions
	metricsProvider *internal.MetricsProvider
}

func newProducerFinalDecider(producer *producer, options *producerFinalDeciderOptions, metricsProvider *internal.MetricsProvider) (*producerFinalDecider, error) {
	if options == nil {
		return nil, errors.New("missing options for producer final decider")
	}
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

	d := &producerFinalDecider{
		logger:          producer.logger.SubLogger(log.Fields{"goto": internal.GotoDiscard}),
		options:         options,
		metricsProvider: metricsProvider,
	}
	topic := d.options.groundTopic + d.options.level.TopicSuffix()
	d.metricsProvider.GetProducerDeciderMetrics(d.options.groundTopic, topic, internal.GotoDiscard.String()).DecidersOpened.Inc()
	return d, nil
}

func (d *producerFinalDecider) Decide(ctx context.Context, msg *pulsar.ProducerMessage,
	checkStatus checker.CheckStatus) (mid pulsar.MessageID, err error, decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err = errors.New(fmt.Sprintf("Failed to decide message as discard as check status is not passed. message: %v", formatPayloadLogContent(msg.Payload)))
		decided = false
		return
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	// discard
	if d.options.logLevel >= logrus.InfoLevel {
		logEntry.Infof("Success to decide message as discard. message: %v", formatPayloadLogContent(msg.Payload))
	}
	return nil, nil, true
}

func (d *producerFinalDecider) DecideAsync(ctx context.Context, msg *pulsar.ProducerMessage, checkStatus checker.CheckStatus,
	callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) (decided bool) {
	// valid check status
	if !checkStatus.IsPassed() {
		err := errors.New(fmt.Sprintf("Failed to decide message as discard as check status is not passed. message: %v", formatPayloadLogContent(msg.Payload)))
		callback(nil, msg, err)
		decided = false
		return
	}
	// parse log entry
	logEntry := util.ParseLogEntry(ctx, d.logger)
	// discard
	if d.options.logLevel >= logrus.InfoLevel {
		logEntry.Infof("Success to decide message as discard. message: %v", formatPayloadLogContent(msg.Payload))
	}
	callback(nil, msg, nil)
	decided = true
	return
}

func (d *producerFinalDecider) close() {
	topic := d.options.groundTopic + d.options.level.TopicSuffix()
	d.metricsProvider.GetProducerDeciderMetrics(d.options.groundTopic, topic, internal.GotoDead.String()).DecidersOpened.Desc()
}
