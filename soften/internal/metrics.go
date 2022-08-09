package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shenqianjin/soften-client-go/soften/checker"
)

// message consume stages: published -> received -> listened -> prev_checked -> handled -> post_checked -> decided

type MetricsProvider struct {
	metricsLevel int

	// client labels: {url=}
	clientsOpened *prometheus.GaugeVec // gauge of opened clients

	// producer labels: {topic=}
	producersOpened        *prometheus.GaugeVec     // gauge of producers
	producerPublishLatency *prometheus.HistogramVec // producers publish latency
	producerDecideLatency  *prometheus.HistogramVec // route latency

	// producer checker label: {topic=, check_type=}
	producerCheckersOpened       *prometheus.GaugeVec   // gauge of produce checkers
	producerCheckerCheckPassed   *prometheus.CounterVec // counter of produce check passed
	producerCheckerCheckRejected *prometheus.CounterVec // counter of produce check rejected
	producerCheckerCheckLatency  *prometheus.HistogramVec

	// producer decider labels: {topic=, goto=}
	producerDecidersOpened     *prometheus.GaugeVec     // gauge of routers
	producerDeciderGotoSuccess *prometheus.CounterVec   // counter of producers decide messages
	producerDeciderGotoFailed  *prometheus.CounterVec   // counter of producers decide messages
	producerDeciderGotoLatency *prometheus.HistogramVec // route latency

	// producer decider (router) labels: {topic=, goto=, route_topic}
	producerRouteDecidersOpened     *prometheus.GaugeVec     // gauge of routers
	producerRouteDeciderGotoSuccess *prometheus.CounterVec   // counter of producers decide messages
	producerRouteDeciderGotoFailed  *prometheus.CounterVec   // counter of producers decide messages
	producerRouteDeciderGotoLatency *prometheus.HistogramVec // route latency

	// listener labels: {topics, levels}
	listenersOpened        *prometheus.GaugeVec     // gauge of opened listeners
	listenersRunning       *prometheus.GaugeVec     // gauge of running listeners
	listenerReceiveLatency *prometheus.HistogramVec // labels: topic, level, status
	listenerListenLatency  *prometheus.HistogramVec // labels: topic, level, status
	listenerCheckLatency   *prometheus.HistogramVec
	listenerHandleLatency  *prometheus.HistogramVec
	listenerDecideLatency  *prometheus.HistogramVec

	// listener checker labels: {topics, levels, check_type}
	listenerCheckersOpened       *prometheus.GaugeVec   // gauge of checkers
	listenerCheckerCheckPassed   *prometheus.CounterVec // counter of produce check passed
	listenerCheckerCheckRejected *prometheus.CounterVec // counter of produce check rejected
	listenerCheckerCheckLatency  *prometheus.HistogramVec

	// listener handle labels: {topics=, levels=, goto=}
	listenerDecidersOpened     *prometheus.GaugeVec
	listenerHandlerGotoLatency *prometheus.HistogramVec
	listenerDeciderGotoLatency *prometheus.HistogramVec
	// listener leveled decider labels: {topics=, levels=, goto=, level=}
	listenerLeveledDecidersOpened *prometheus.GaugeVec

	// consumer labels: {topics, levels, topic, level, status}
	consumersOpened        *prometheus.GaugeVec     // gauge of consumers
	consumerReceiveLatency *prometheus.HistogramVec // labels: topic, level, status
	consumerListenLatency  *prometheus.HistogramVec // labels: topic, level, status
	consumerCheckLatency   *prometheus.HistogramVec // labels: topic, level, status
	consumerHandleLatency  *prometheus.HistogramVec // labels: topic, level, status
	consumerDecideLatency  *prometheus.HistogramVec // labels: topic, level, status
	consumerConsumeLatency *prometheus.HistogramVec // including receive, listen, check, handle and decide

	// consumer handle labels: {topics, levels, topic, level, status, goto}
	consumerHandlerGoto             *prometheus.CounterVec   // labels: topics, levels, topic, level, status, goto
	consumerHandlerGotoLatency      *prometheus.HistogramVec // labels: topics, levels, topic, level, status, goto, reason: {over_reconsume_times, internal_error}
	consumerHandlerGotoConsumeTimes *prometheus.HistogramVec // labels: topics, levels, topic, level, status, goto, reason: {over_reconsume_times, internal_error}
	consumerDecidersOpened          *prometheus.GaugeVec     // gauge of handlers
	consumerDeciderGotoSuccess      *prometheus.CounterVec
	consumerDecideGotoFailed        *prometheus.CounterVec
	consumerDecideGotoLatency       *prometheus.HistogramVec // labels: topic, level, status

	// messages terminate labels: {topic=, level=, original_topic=, original_level=,}
	messagesAcks             *prometheus.CounterVec
	messagesNacks            *prometheus.CounterVec
	messagesTerminateLatency *prometheus.HistogramVec // whole latency on message lifecycle, from published to done/discard/dead. labels: topic, level, status[done, discard, dead]

}

func NewMetricsProvider(metricsLevel int, userDefinedLabels map[string]string) *MetricsProvider {
	constLabels := map[string]string{
		"client": "soften",
	}
	for k, v := range userDefinedLabels {
		constLabels[k] = v
	}

	metrics := &MetricsProvider{metricsLevel: metricsLevel}

	// client labels: {url=}
	clientLabels := []string{"url"}
	metrics.clientsOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_client_opened",
		Help:        "Gauge of opened clients",
		ConstLabels: constLabels,
	}, clientLabels))

	// producer labels: {topic=}
	producerLabels := []string{"topic"}
	metrics.producersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_client_producers_opened",
		Help:        "Gauge of opened producers",
		ConstLabels: constLabels,
	}, producerLabels))
	metrics.producerPublishLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_client_producer_publish_latency",
		Help:        "Publish latency experienced by the client",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, producerLabels))
	metrics.producerDecideLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_client_producer_decide_latency",
		Help:        "Decide latency experienced by the client",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, producerLabels))

	// producer checker label: {topic=, check_type=}
	produceCheckerLabels := []string{"topic", "check_type"}
	metrics.producerCheckersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_produce_checkers_opened",
		Help:        "Gauge of opened produce checkers",
		ConstLabels: constLabels,
	}, produceCheckerLabels))
	metrics.producerCheckerCheckPassed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_produce_checker_check_passed",
		Help:        "Counter of check passed by produce checker",
		ConstLabels: constLabels,
	}, produceCheckerLabels))
	metrics.producerCheckerCheckRejected = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_produce_checker_check_rejected",
		Help:        "Counter of check rejected by produce checker",
		ConstLabels: constLabels,
	}, produceCheckerLabels))
	metrics.producerCheckerCheckLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_produce_checker_check_latency",
		Help:        "Check latency experienced by the client",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, produceCheckerLabels))

	// producer decider labels: {topic=, goto=}
	produceDeciderGotoLabels := []string{"topic", "goto"}
	metrics.producerDecidersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_produce_deciders_opened",
		Help:        "Gauge of opened produce deciders",
		ConstLabels: constLabels,
	}, produceDeciderGotoLabels))
	metrics.producerDeciderGotoSuccess = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_produce_decider_decide_passed",
		Help:        "Counter of check passed by produce checker",
		ConstLabels: constLabels,
	}, produceDeciderGotoLabels))
	metrics.producerDeciderGotoFailed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_produce_decider_decide_rejected",
		Help:        "Counter of decide rejected by produce checker",
		ConstLabels: constLabels,
	}, produceDeciderGotoLabels))
	metrics.producerDeciderGotoLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_produce_decider_decide_latency",
		Help:        "Decide latency experienced by the producer",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, produceDeciderGotoLabels))

	// producer decider (router) labels: {topic=, goto=, route_topic}
	produceRouteDeciderGotoLabels := []string{"topic", "goto", "route_topic"}
	metrics.producerRouteDecidersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_produce_route_deciders_opened",
		Help:        "Gauge of opened produce deciders",
		ConstLabels: constLabels,
	}, produceRouteDeciderGotoLabels))
	metrics.producerRouteDeciderGotoSuccess = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_produce_route_decider_decide_passed",
		Help:        "Counter of check passed by produce checker",
		ConstLabels: constLabels,
	}, produceRouteDeciderGotoLabels))
	metrics.producerRouteDeciderGotoFailed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_produce_route_decider_decide_rejected",
		Help:        "Counter of decide rejected by produce checker",
		ConstLabels: constLabels,
	}, produceRouteDeciderGotoLabels))
	metrics.producerRouteDeciderGotoLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_produce_route_decider_decide_latency",
		Help:        "Decide latency experienced by the producer",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, produceRouteDeciderGotoLabels))

	// listener labels: {topics, levels}
	listenerLabels := []string{"topics", "levels"}
	metrics.listenersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_listeners_opened",
		Help:        "Gauge of opened listeners",
		ConstLabels: constLabels,
	}, listenerLabels))
	metrics.listenersRunning = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_listeners_running",
		Help:        "Gauge of running listeners",
		ConstLabels: constLabels,
	}, listenerLabels))
	metrics.listenerReceiveLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_listener_receive_latency",
		Help:        "Receive latency experienced by the listener",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listenerLabels))
	metrics.listenerListenLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_listener_listen_latency",
		Help:        "Listen latency experienced by the listener",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listenerLabels))
	metrics.listenerCheckLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_listener_check_latency",
		Help:        "Check latency experienced by the listener",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listenerLabels))
	metrics.listenerHandleLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_listener_handle_latency",
		Help:        "Handle latency experienced by the listener",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listenerLabels))
	metrics.listenerDecideLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_listener_decide_latency",
		Help:        "Decide latency experienced by the listener",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listenerLabels))

	// listener checker labels: {topics, levels, check_type}
	listeneCheckerLabels := []string{"topics", "levels", "check_type"}
	metrics.listenerCheckersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_listen_checkers_opened",
		Help:        "Gauge of opened listen checkers",
		ConstLabels: constLabels,
	}, listeneCheckerLabels))
	metrics.listenerCheckerCheckPassed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_listen_checker_check_passed",
		Help:        "Counter of check passed by listen checker",
		ConstLabels: constLabels,
	}, listeneCheckerLabels))
	metrics.listenerCheckerCheckRejected = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_listen_checker_check_rejected",
		Help:        "Counter of check rejected by listen checker",
		ConstLabels: constLabels,
	}, listeneCheckerLabels))
	metrics.listenerCheckerCheckLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_listen_checker_check_latency",
		Help:        "Check latency experienced by listen checker",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listeneCheckerLabels))

	// listener handle labels: {topics=, levels=, goto=}
	listenHandlerGotoLabels := []string{"topics", "levels", "goto"}
	metrics.listenerHandlerGotoLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_listen_handler_goto_latency",
		Help:        "Handle latency experienced by the handler",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listenHandlerGotoLabels))
	metrics.listenerDeciderGotoLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_listen_decider_decide_latency",
		Help:        "Decide latency experienced by the listen decider",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listenHandlerGotoLabels))
	metrics.listenerDecidersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_consumers_opened",
		Help:        "Gauge of opened consumers",
		ConstLabels: constLabels,
	}, listenHandlerGotoLabels))
	// listener leveled decider labels: {topics=, levels=, goto=, level=}
	listenLeveledDeciderLabels := []string{"topics", "levels", "goto", "level"}
	metrics.listenerLeveledDecidersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_consumers_opened",
		Help:        "Gauge of opened consumers",
		ConstLabels: constLabels,
	}, listenLeveledDeciderLabels))

	// consumer labels: {topics, levels, topic, level, status}
	consumerLabels := []string{"topics", "levels", "topic", "level", "status"}
	metrics.consumersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_consumers_opened",
		Help:        "Gauge of opened consumers",
		ConstLabels: constLabels,
	}, consumerLabels))
	metrics.consumerReceiveLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_receive_latency",
		Help:        "Receive latency experienced by the consumer",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerLabels))
	metrics.consumerListenLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_listen_latency",
		Help:        "Listen latency experienced by the consumer",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerLabels))
	metrics.consumerCheckLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_check_latency",
		Help:        "Check latency experienced by the consumer",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerLabels))
	metrics.consumerHandleLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_handle_latency",
		Help:        "Handle latency experienced by the consumer",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerLabels))
	metrics.consumerDecideLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_decide_latency",
		Help:        "Decide latency experienced by the consumer",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerLabels))
	metrics.consumerConsumeLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_decide_latency",
		Help:        "Whole consume latency experienced by the consumer",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerLabels))

	// consumer handle labels: {topics, levels, topic, level, status, goto}
	consumerHandlerLabels := []string{"topics", "levels", "topic", "level", "status", "goto"}
	metrics.consumerHandlerGotoLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consume_handler_goto_latency",
		Help:        "Handle latency experienced by the consume handler",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerHandlerLabels))
	metrics.consumerHandlerGoto = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consume_handler_handled",
		Help:        "Counter of handle done by consume handler",
		ConstLabels: constLabels,
	}, consumerHandlerLabels))
	metrics.consumerHandlerGotoLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consume_handler_handle_latency",
		Help:        "Handle latency experienced by the consume handler",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerHandlerLabels))
	metrics.consumerHandlerGotoConsumeTimes = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consume_handler_handle_times",
		Help:        "Handle times experienced by the consume handler",
		ConstLabels: constLabels,
		Buckets:     []float64{1, 2, 3, 5, 8, 10, 13, 17, 20, 30, 40, 50, 60, 80, 100},
	}, consumerHandlerLabels))
	metrics.consumerDecidersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_consume_deciders_opened",
		Help:        "Gauge of opened consume deciders",
		ConstLabels: constLabels,
	}, consumerHandlerLabels))
	metrics.consumerDeciderGotoSuccess = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consume_decider_decide_passed",
		Help:        "Counter of check passed by consume checker",
		ConstLabels: constLabels,
	}, consumerHandlerLabels))
	metrics.consumerDecideGotoFailed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consume_decider_decide_rejected",
		Help:        "Counter of decide rejected by consume checker",
		ConstLabels: constLabels,
	}, consumerHandlerLabels))
	metrics.consumerDecideGotoLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consume_decider_decide_latency",
		Help:        "Decide latency experienced by the consume decider",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerHandlerLabels))

	// messages terminate labels: {topic=, level=, original_topic=, original_level=,}
	commonMessageLabels := []string{"topic", "level", "original_topic", "original_level"}
	metrics.messagesAcks = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consume_decider_decide_rejected",
		Help:        "Counter of decide rejected by consume checker",
		ConstLabels: constLabels,
	}, commonMessageLabels))
	metrics.messagesNacks = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consume_decider_decide_rejected",
		Help:        "Counter of decide rejected by consume checker",
		ConstLabels: constLabels,
	}, commonMessageLabels))
	metrics.messagesTerminateLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consume_decider_decide_latency",
		Help:        "Decide latency experienced by the consume decider",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, commonMessageLabels)) // whole latency on message lifecycle, from published to done/discard/dead

	return metrics
}
func (v *MetricsProvider) tryRegisterCounter(vec *prometheus.CounterVec) *prometheus.CounterVec {
	err := prometheus.DefaultRegisterer.Register(vec)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			vec = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	return vec
}

func (v *MetricsProvider) tryRegisterGauge(vec *prometheus.GaugeVec) *prometheus.GaugeVec {
	err := prometheus.DefaultRegisterer.Register(vec)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			vec = are.ExistingCollector.(*prometheus.GaugeVec)
		}
	}
	return vec
}

func (v *MetricsProvider) tryRegisterHistogram(vec *prometheus.HistogramVec) *prometheus.HistogramVec {
	err := prometheus.DefaultRegisterer.Register(vec)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			vec = are.ExistingCollector.(*prometheus.HistogramVec)
		}
	}
	return vec
}

func (v *MetricsProvider) GetClientMetrics(url string) *ClientMetrics {
	labels := prometheus.Labels{"url": url}
	metrics := &ClientMetrics{
		ClientsOpened: v.clientsOpened.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetProducerMetrics(topic string) *ProducerMetrics {
	labels := prometheus.Labels{"topic": topic}
	metrics := &ProducerMetrics{
		ProducersOpened: v.producersOpened.With(labels),
		PublishLatency:  v.producerPublishLatency.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetProducerTypedCheckMetrics(topic string, checkType checker.CheckType) *TypedCheckMetrics {
	labels := prometheus.Labels{"topic": topic, "check_type": checkType.String()}
	metrics := &TypedCheckMetrics{
		CheckersOpened: v.producerCheckersOpened.With(labels),
		CheckPassed:    v.producerCheckerCheckPassed.With(labels),
		CheckRejected:  v.producerCheckerCheckRejected.With(labels),
		CheckLatency:   v.producerCheckerCheckLatency.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetProducerDecideGotoMetrics(topic, handleGoto string) *DecideGotoMetrics {
	labels := prometheus.Labels{"topic": topic, "goto": handleGoto}
	metrics := &DecideGotoMetrics{
		DecidersOpened: v.producerDecidersOpened.With(labels),
		DecideSuccess:  v.producerDeciderGotoSuccess.With(labels),
		DecideFailed:   v.producerDeciderGotoFailed.With(labels),
		DecideLatency:  v.producerDeciderGotoLatency.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetProducerDecideRouteMetrics(topic, handleGoto string, routeTopic string) *DecideGotoMetrics {
	labels := prometheus.Labels{"topic": topic, "goto": handleGoto, "route_topic": routeTopic}
	metrics := &DecideGotoMetrics{
		DecidersOpened: v.producerDecidersOpened.With(labels),
		DecideSuccess:  v.producerDeciderGotoSuccess.With(labels),
		DecideFailed:   v.producerDeciderGotoFailed.With(labels),
		DecideLatency:  v.producerDeciderGotoLatency.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetListenMetrics(topics, levels string) *ListenMetrics {
	labels := prometheus.Labels{"topics": topics, "levels": levels}
	metrics := &ListenMetrics{
		ListenersOpened:  v.listenersOpened.With(labels),
		ListenersRunning: v.listenersRunning.With(labels),
		ReceiveLatency:   v.listenerReceiveLatency.With(labels),
		ListenLatency:    v.listenerListenLatency.With(labels),
		CheckLatency:     v.listenerCheckLatency.With(labels),
		HandleLatency:    v.listenerHandleLatency.With(labels),
		DecideLatency:    v.listenerDecideLatency.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetListenerTypedCheckMetrics(topics, levels string, checkType checker.CheckType) *TypedCheckMetrics {
	labels := prometheus.Labels{"topics": topics, "levels": levels, "check_type": checkType.String()}
	metrics := &TypedCheckMetrics{
		CheckersOpened: v.listenerCheckersOpened.With(labels),
		CheckPassed:    v.listenerCheckerCheckPassed.With(labels),
		CheckRejected:  v.listenerCheckerCheckRejected.With(labels),
		CheckLatency:   v.listenerCheckerCheckLatency.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetListenerHandleMetrics(topics, levels string, msgGoto HandleGoto) *ListenerHandleMetrics {
	labels := prometheus.Labels{"topics": topics, "levels": levels, "goto": msgGoto.String()}
	metrics := &ListenerHandleMetrics{
		HandleGotoLatency: v.listenerHandlerGotoLatency.With(labels),
		DecideGotoLatency: v.listenerDeciderGotoLatency.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetListenerDecideGotoMetrics(topics, levels string, msgGoto HandleGoto) *ListenerDecideGotoMetrics {
	labels := prometheus.Labels{"topics": topics, "levels": levels, "goto": msgGoto.String()}
	metrics := &ListenerDecideGotoMetrics{
		DecidersOpened: v.listenerDecidersOpened.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetListenerLeveledDecideGotoMetrics(topics, levels string, level TopicLevel, msgGoto HandleGoto) *ListenerDecideGotoMetrics {
	labels := prometheus.Labels{"topics": topics, "levels": levels, "level": level.String(), "goto": msgGoto.String()}
	metrics := &ListenerDecideGotoMetrics{
		DecidersOpened: v.listenerLeveledDecidersOpened.With(labels),
	}
	return metrics
}

func (v *MetricsProvider) GetConsumerMetrics(topics, levels, topic string, level TopicLevel, status MessageStatus) *ConsumerMetrics {
	labels := prometheus.Labels{"topics": topics, "levels": levels, "topic": topic, "level": level.String(), "status": status.String()}
	metrics := &ConsumerMetrics{
		ConsumersOpened: v.consumersOpened.With(labels),
		ReceiveLatency:  v.consumerReceiveLatency.With(labels),
		ListenLatency:   v.consumerListenLatency.With(labels),
		CheckLatency:    v.consumerCheckLatency.With(labels),
		HandleLatency:   v.consumerHandleLatency.With(labels),
		DecideLatency:   v.consumerDecideLatency.With(labels),
		ConsumeLatency:  v.consumerConsumeLatency.With(labels),
	}
	return metrics

}

func (v *MetricsProvider) GetConsumerHandleGotoMetrics(topics, levels, topic string, level TopicLevel, status MessageStatus, msgGoto HandleGoto) *ConsumerHandleGotoMetrics {
	labels := prometheus.Labels{"topics": topics, "levels": levels, "topic": topic, "level": level.String(),
		"status": status.String(), "goto": msgGoto.String()}
	metrics := &DecideGotoMetrics{
		DecidersOpened: v.consumerDecidersOpened.With(labels),
		DecideSuccess:  v.consumerDeciderGotoSuccess.With(labels),
		DecideFailed:   v.consumerDecideGotoFailed.With(labels),
		DecideLatency:  v.consumerDecideGotoLatency.With(labels),
	}
	handledMetrics := &ConsumerHandleGotoMetrics{
		DecideGotoMetrics:      metrics,
		HandleGoto:             v.consumerHandlerGoto.With(labels),
		HandleGotoLatency:      v.consumerHandlerGotoLatency.With(labels),
		HandleGotoConsumeTimes: v.consumerHandlerGotoConsumeTimes.With(labels),
	}
	return handledMetrics
}

func (v *MetricsProvider) GetMessageMetrics(topic, handleGoto string, routeTopic string) *MessageMetrics {
	labels := prometheus.Labels{"topic": topic, "goto": handleGoto, "route_topic": routeTopic}
	metrics := &MessageMetrics{
		Acks:              v.messagesAcks.MustCurryWith(labels),
		Nacks:             v.messagesNacks.MustCurryWith(labels),
		TerminatedLatency: v.messagesTerminateLatency.MustCurryWith(labels),
	}
	return metrics
}

// ------ helper ------

// ClientMetrics composes these metrics related to client with {url=} label:
// soften_clients_opened {url=};
type ClientMetrics struct {
	ClientsOpened prometheus.Gauge
}

// ProducerMetrics composes these metrics generated during producing with {topic=} label:
// soften_client_producers_opened {topic=};
// soften_client_producer_messages_published {topic=};
// soften_client_producer_publish_latency {topic=};
type ProducerMetrics struct {
	ProducersOpened prometheus.Gauge    // gauge of producers
	PublishLatency  prometheus.Observer // producers publish latency
}

type TypedCheckMetrics struct {
	CheckersOpened prometheus.Gauge   // gauge of produce checkers
	CheckPassed    prometheus.Counter // counter of produce check passed
	CheckRejected  prometheus.Counter // counter of produce check rejected
	CheckLatency   prometheus.Observer
}

// DecideGotoMetrics composes these metrics generated during routing with {topic=, route_topic=} label:
// soften_client_routers_opened {topic=, route_topic=};
// soften_client_messages_routed {topic=, route_topic=};
// soften_client_messages_publish_latency {topic=, route_topic};
type DecideGotoMetrics struct {
	DecidersOpened prometheus.Gauge    // gauge of routers
	DecideSuccess  prometheus.Counter  // counter of producers decide messages
	DecideFailed   prometheus.Counter  // counter of producers decide messages
	DecideLatency  prometheus.Observer // route latency
}

// ListenMetrics composed these metrics related multi-level framework, including listen, check, handler, reroute:
// soften_client_listeners_running {topics=,levels=};
// soften_client_listeners_closed {topics=,levels=};
type ListenMetrics struct {
	ListenersOpened  prometheus.Gauge    // gauge of opened listeners
	ListenersRunning prometheus.Gauge    // gauge of running listeners
	ReceiveLatency   prometheus.Observer // labels: topic, level, status
	ListenLatency    prometheus.Observer // labels: topic, level, status
	CheckLatency     prometheus.Observer
	HandleLatency    prometheus.Observer
	DecideLatency    prometheus.Observer
}

// ListenerHandleMetrics composed these metrics related to handle process:
// soften_client_handlers_active {topics=,levels=};
type ListenerHandleMetrics struct {
	HandleGotoLatency prometheus.Observer
	DecideGotoLatency prometheus.Observer
}

type ListenerDecideGotoMetrics struct {
	DecidersOpened prometheus.Gauge // gauge of routers
}

// ConsumerMetrics composes these metrics generated during consume with {topic=, level=, status=} label:
// soften_client_consumers_opened {topic=, level=, status=};
// soften_client_messages_received
// soften_client_messages_listened
// soften_client_messages_processed
type ConsumerMetrics struct {
	ConsumersOpened prometheus.Gauge    // gauge of consumers
	ReceiveLatency  prometheus.Observer // labels: topic, level, status
	ListenLatency   prometheus.Observer // labels: topic, level, status
	CheckLatency    prometheus.Observer // labels: topic, level, status
	HandleLatency   prometheus.Observer // labels: topic, level, status
	DecideLatency   prometheus.Observer // labels: topic, level, status
	ConsumeLatency  prometheus.Observer // latency on client consumer consume from received to done/transferred. labels: topic, level, status

}

type ConsumerHandleGotoMetrics struct {
	*DecideGotoMetrics
	HandleGoto             prometheus.Counter  // labels: topics, levels, topic, level, status, goto
	HandleGotoLatency      prometheus.Observer // labels: topics, levels, topic, level, statu
	HandleGotoConsumeTimes prometheus.Observer
}

// MessageMetrics composed these metrics related to transfer process:
// soften_client_transfer_opened {topics=,levels=};
type MessageMetrics struct {
	Acks              *prometheus.CounterVec
	Nacks             *prometheus.CounterVec
	TerminatedLatency prometheus.ObserverVec
}
