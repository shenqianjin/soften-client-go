package soften

import (
	"errors"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

type gradeOptions struct {
	topic       string
	grade2Level internal.TopicLevel
	level       internal.TopicLevel
	msgGoto     internal.HandleGoto
}

type gradeDecider struct {
	router  *reRouter
	logger  log.Logger
	options gradeOptions
	metrics *internal.ListenerDecideGotoMetrics
}

func newGradeDecider(client *client, listener *consumeListener, options gradeOptions) (*gradeDecider, error) {
	if options.topic == "" {
		return nil, errors.New("topic cannot be blank")
	}
	if options.grade2Level == "" {
		return nil, errors.New("topic level is empty")
	}
	suffix := options.grade2Level.TopicSuffix()
	routerOption := reRouterOptions{Topic: options.topic + suffix}
	rt, err := newReRouter(client.logger, client.Client, routerOption)
	if err != nil {
		return nil, err
	}
	metrics := client.metricsProvider.GetListenerLeveledDecideGotoMetrics(listener.logTopics, listener.logLevels, options.level, options.msgGoto)
	hd := &gradeDecider{router: rt, logger: client.logger, options: options, metrics: metrics}
	metrics.DecidersOpened.Inc()
	return hd, nil
}

func (hd *gradeDecider) Decide(msg pulsar.ConsumerMessage, cheStatus checker.CheckStatus) bool {
	if !cheStatus.IsPassed() {
		return false
	}
	// prepare to upgrade / degrade
	props := make(map[string]string)
	for k, v := range msg.Properties() {
		props[k] = v
	}
	// record origin information when re-route first time
	message.Helper.InjectOriginTopic(msg, &props)
	message.Helper.InjectOriginMessageId(msg, &props)
	message.Helper.InjectOriginPublishTime(msg, &props)
	message.Helper.InjectOriginLevel(msg, &props)
	message.Helper.InjectOriginStatus(msg, &props)
	// record previous level/status information
	message.Helper.InjectPreviousLevel(msg, &props)
	message.Helper.InjectPreviousStatus(msg, &props)

	producerMsg := pulsar.ProducerMessage{
		Payload:     msg.Payload(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		Properties:  props,
		EventTime:   msg.EventTime(),
	}
	hd.router.Chan() <- &RerouteMessage{
		consumerMsg: msg,
		producerMsg: producerMsg,
	}
	return true
}

func (hd *gradeDecider) close() {

}
