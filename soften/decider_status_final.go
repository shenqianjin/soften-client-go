package soften

import (
	"errors"
	"fmt"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/handler"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type finalStatusDecider struct {
	logger  log.Logger
	msgGoto internal.HandleGoto
	metrics *internal.ListenerDecideGotoMetrics
}

func newFinalStatusDecider(cli *client, listener *consumeListener, msgGoto internal.HandleGoto) (*finalStatusDecider, error) {
	if msgGoto == "" {
		return nil, errors.New("final message status cannot be empty")
	}
	if msgGoto != handler.GotoDone && msgGoto != handler.GotoDiscard {
		return nil, errors.New(fmt.Sprintf("%s is not a final message goto action", msgGoto))
	}
	metrics := cli.metricsProvider.GetListenerDecideGotoMetrics(listener.logTopics, listener.logLevels, msgGoto)
	decider := &finalStatusDecider{logger: cli.logger, msgGoto: msgGoto, metrics: metrics}
	metrics.DecidersOpened.Inc()
	return decider, nil

}

func (d *finalStatusDecider) Decide(msg pulsar.ConsumerMessage, cheStatus checker.CheckStatus) (success bool) {
	if !cheStatus.IsPassed() {
		return false
	}
	switch d.msgGoto {
	case handler.GotoDone:
		msg.Consumer.Ack(msg.Message)
		//d.logger.Warnf("Decide message: done")
		return true
	case handler.GotoDiscard:
		msg.Consumer.Ack(msg.Message)
		//d.logger.Warnf("Decide message: discard")
		return true
	}
	return false
}

func (d *finalStatusDecider) close() {
	d.metrics.DecidersOpened.Dec()
}
