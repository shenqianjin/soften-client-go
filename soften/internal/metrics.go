package internal

import (
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricsProvider is a helper to track metrics for topic/message in one of message lifecycle:
// business : event happen
// publish  : prev check -> send(sendAsync) -> (pending)  -> [to pulsar]
// consume  : receive -> listen -> prev check -> handle -> post check -> decide
// note that publish time is generated in 'publish' stage before set the message into events chan.
type MetricsProvider struct {
	metricsLevel int

	// client labels: {url=}
	clientsMetricsMap sync.Map // map[{url=}]*internal.ClientMetrics
	clientsOpened     *prometheus.GaugeVec

	// producer labels: {ground_topic=, topic=}
	producersMetricsMap              sync.Map // map[{ground_topic=, topic=}]*internal.ProducerMetrics
	producersOpened                  *prometheus.GaugeVec
	producerPublishSuccess           *prometheus.CounterVec
	producerPublishFailed            *prometheus.CounterVec
	producerPublishLatencyFromPCheck *prometheus.HistogramVec
	producerPublishLatencyFromEvent  *prometheus.HistogramVec

	// producer checkers label: {ground_topic=, topic=, check_type=}
	producerCheckersMetricsMap sync.Map // map[{ground_topic=, topic=, check_type=}]*internal.ProducerCheckerMetrics
	producerCheckersOpened     *prometheus.GaugeVec
	// producer checker label: {ground_topic=, topic=, check_type=}
	producerCheckMetricsMap sync.Map // map[{ground_topic=, topic=, check_type=}]*internal.ProducerCheckerMetrics
	producerCheckPassed     *prometheus.CounterVec
	producerCheckRejected   *prometheus.CounterVec
	producerCheckLatency    *prometheus.HistogramVec

	// producer decider labels: {ground_topic=, topic=, goto=}
	producerDecideMetricsMap sync.Map // map[{ground_topic=, topic=, goto=}]*internal.ProducerDeciderMetrics
	producerDecidersOpened   *prometheus.GaugeVec
	producerDecideSuccess    *prometheus.CounterVec
	producerDecideFailed     *prometheus.CounterVec
	producerDecideLatency    *prometheus.HistogramVec

	// listener metrics, labels: {ground_topic=, subscription=}
	listenersMetricsMap sync.Map // map[{ground_topic=, subscription=}]*internal.ListenerMetrics
	listenersOpened     *prometheus.GaugeVec
	listenersRunning    *prometheus.GaugeVec

	// consumer consume metrics, labels: {ground_topic=, level, status, subscription, topic}
	consumerConsumeMetricsMap         sync.Map // map[{ground_topic=, level, status, subscription, topic}]*internal.ListenerConsumerMetrics
	consumersOpened                   *prometheus.GaugeVec
	consumerReceiveLatencyFromPublish *prometheus.HistogramVec
	consumerListenLatency             *prometheus.HistogramVec
	consumerListenLatencyFromEvent    *prometheus.HistogramVec
	consumerConsumeMessageAcks        *prometheus.CounterVec
	consumerConsumeMessageNacks       *prometheus.CounterVec
	consumerConsumeMessageEscape      *prometheus.CounterVec

	// consumer checkers metrics, labels: {ground_topic=, subscription, check_type}
	consumerCheckersMetricsMap sync.Map // map[{ground_topic=, subscription, check_type}]*internal.ListenerCheckersMetrics
	consumerCheckersOpened     *prometheus.GaugeVec
	// consumer check metrics, labels: {ground_topic=, level, status, subscription, topic, check_type}
	consumerCheckMetricsMap sync.Map // map[{ground_topic=, level, status, subscription, topic, check_type}]*internal.ListenerCheckerMetrics
	consumerCheckPassed     *prometheus.CounterVec
	consumerCheckRejected   *prometheus.CounterVec
	consumerCheckLatency    *prometheus.HistogramVec

	// consumer handle metrics, labels: {ground_topic=, level, status, subscription, topic, goto}
	consumerHandleMetricsMap sync.Map // map[{ground_topic=, topic, level, status, goto}]*internal.ListenerHandlerMetrics
	consumerHandleLatency    *prometheus.HistogramVec
	consumerHandleConsumes   *prometheus.HistogramVec

	// consumer deciders metrics, labels: {ground_topic=, subscription, goto}
	consumerDecidersMetricsMap sync.Map // map[{ground_topic=, subscription, goto}]*internal.ListenerDecidersMetrics
	consumerDecidersOpened     *prometheus.GaugeVec
	// consumer decide metrics, labels: {ground_topic=, level, status, subscription, topic, goto}
	consumerDecideMetricsMap         sync.Map // map[{ground_topic=, level, status, subscription, topic, goto}]*internal.ListenerDeciderMetrics
	consumerDecideSuccess            *prometheus.CounterVec
	consumerDecideFailed             *prometheus.CounterVec
	consumerDecideLatency            *prometheus.HistogramVec
	consumerDecideLatencyFromReceive *prometheus.HistogramVec
	consumerDecideLatencyFromListen  *prometheus.HistogramVec
	consumerDecideLatencyFromLPCheck *prometheus.HistogramVec

	// messages metrics, labels: {ground_topic=, level, status, subscription, topic, goto[done, discard, dead]}
	messageMetricsMap            sync.Map // map[{ground_topic=, level, status, subscription, topic, goto}]*internal.ListenerMessagesMetrics
	messageEndLatencyFromPublish *prometheus.HistogramVec
	messageEndLatencyFromEvent   *prometheus.HistogramVec
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
		Name:        "soften_clients_opened",
		Help:        "Gauge of opened clients",
		ConstLabels: constLabels,
	}, clientLabels))

	// producer labels: {ground_topic=, topic=}
	producerLabels := []string{"ground_topic", "topic"}
	metrics.producersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_producers_opened",
		Help:        "Gauge of opened producers",
		ConstLabels: constLabels,
	}, producerLabels))
	metrics.producerPublishSuccess = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_producer_publish_success",
		Help:        "Counter of publish success by produce deciders",
		ConstLabels: constLabels,
	}, producerLabels))
	metrics.producerPublishFailed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_producer_publish_failed",
		Help:        "Counter of publish failed by produce deciders",
		ConstLabels: constLabels,
	}, producerLabels))
	metrics.producerPublishLatencyFromPCheck = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_producer_publish_latency_from_PCheck",
		Help:        "Publish latency from prev-check start time to the server response time",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600},
	}, producerLabels))
	metrics.producerPublishLatencyFromEvent = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_producer_publish_latency_from_Event",
		Help:        "Publish latency from event time to the server response time",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600},
	}, producerLabels))

	// producer checker label: {ground_topic=, topic=, check_type=}
	produceCheckerLabels := []string{"ground_topic", "topic", "check_type"}
	metrics.producerCheckersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_producer_checkers_opened",
		Help:        "Gauge of opened produce checkers",
		ConstLabels: constLabels,
	}, produceCheckerLabels))
	metrics.producerCheckPassed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_producer_check_passed",
		Help:        "Counter of check passed by produce checkers",
		ConstLabels: constLabels,
	}, produceCheckerLabels))
	metrics.producerCheckRejected = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_producer_check_rejected",
		Help:        "Counter of check rejected by produce checkers",
		ConstLabels: constLabels,
	}, produceCheckerLabels))
	metrics.producerCheckLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_producer_check_latency",
		Help:        "Check latency experienced by produce checkers",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600},
	}, produceCheckerLabels))

	// producer decider labels: {ground_topic=, topic=, goto=}
	produceDeciderGotoLabels := []string{"ground_topic", "topic", "goto"}
	metrics.producerDecidersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_producer_deciders_opened",
		Help:        "Gauge of opened produce deciders",
		ConstLabels: constLabels,
	}, produceDeciderGotoLabels))
	metrics.producerDecideSuccess = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_producer_decide_success",
		Help:        "Counter of decide success by produce deciders",
		ConstLabels: constLabels,
	}, produceDeciderGotoLabels))
	metrics.producerDecideFailed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_producer_decide_failed",
		Help:        "Counter of decide failed by produce deciders",
		ConstLabels: constLabels,
	}, produceDeciderGotoLabels))
	metrics.producerDecideLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_producer_decide_latency",
		Help:        "Decide latency experienced by produce deciders",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, produceDeciderGotoLabels))

	// listener labels: {ground_topic=, subscription=}
	listenerLabels := []string{"ground_topic", "subscription"}
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

	// listener consumer labels: {ground_topic=, level, status, subscription, topic}
	listenerConsumerLabels := []string{"ground_topic", "level", "status", "subscription", "topic"}
	metrics.consumersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_consumers_opened",
		Help:        "Gauge of opened consumers",
		ConstLabels: constLabels,
	}, listenerConsumerLabels))
	metrics.consumerReceiveLatencyFromPublish = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_receive_latency_from_Publish",
		Help:        "Publish latency from publish time to receive time",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, listenerConsumerLabels))
	metrics.consumerListenLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_listen_latency",
		Help:        "Listen latency from receive time to listen time",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
			24 * 3600, 2 * 24 * 3600, 3 * 24 * 3600, 4 * 24 * 3600, 5 * 24 * 3600, 6 * 24 * 3600, 7 * 24 * 3600, 15 * 24 * 3600, 30 * 24 * 3600},
	}, listenerConsumerLabels))
	metrics.consumerListenLatencyFromEvent = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_listen_latency_from_Event",
		Help:        "Listen latency from event time to listen time",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
			24 * 3600, 2 * 24 * 3600, 3 * 24 * 3600, 4 * 24 * 3600, 5 * 24 * 3600, 6 * 24 * 3600, 7 * 24 * 3600, 15 * 24 * 3600, 30 * 24 * 3600},
	}, listenerConsumerLabels))
	metrics.consumerConsumeMessageAcks = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consumer_consume_messages_acks",
		Help:        "Counter of message ack by consumers",
		ConstLabels: constLabels,
	}, listenerConsumerLabels))
	metrics.consumerConsumeMessageNacks = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consumer_consume_messages_nacks",
		Help:        "Counter of message nack by consumers",
		ConstLabels: constLabels,
	}, listenerConsumerLabels))
	metrics.consumerConsumeMessageEscape = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consumer_consume_messages_escape",
		Help:        "Counter of message escape by consumers",
		ConstLabels: constLabels,
	}, listenerConsumerLabels))

	// listener checker labels: {ground_topic=, subscription, check_type}
	listenerCheckersLabels := []string{"ground_topic", "subscription", "check_type"}
	metrics.consumerCheckersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_consumer_checkers_opened",
		Help:        "Gauge of opened consume checkers",
		ConstLabels: constLabels,
	}, listenerCheckersLabels))
	// consumer check metrics, labels: {ground_topic=, level, status, subscription, topic, check_type}
	listenerCheckerLabels := []string{"ground_topic", "level", "status", "subscription", "topic", "check_type"}
	metrics.consumerCheckPassed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consumer_check_passed",
		Help:        "Counter of check passed by consume checkers",
		ConstLabels: constLabels,
	}, listenerCheckerLabels))
	metrics.consumerCheckRejected = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consumer_check_rejected",
		Help:        "Counter of check rejected by consume checkers",
		ConstLabels: constLabels,
	}, listenerCheckerLabels))
	metrics.consumerCheckLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_check_latency",
		Help:        "Check latency experienced by consume checkers",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600},
	}, listenerCheckerLabels))

	// listener consumer handle labels: {ground_topic=, level, status, subscription, topic, goto}
	consumerHandlerLabels := []string{"ground_topic", "level", "status", "subscription", "topic", "goto"}
	metrics.consumerHandleLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_handle_latency",
		Help:        "Handle latency experienced by consume handlers",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
			24 * 3600},
	}, consumerHandlerLabels))
	metrics.consumerHandleConsumes = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_handle_messages_consumes",
		Help:        "Consume times experienced by consume handlers",
		ConstLabels: constLabels,
		Buckets: []float64{1, 2, 3, 5, 8, 10, 13, 17, 20, 30, 40, 50,
			60, 80, 100, 150, 200, 250, 300, 350, 400, 500, 600, 700, 800, 900, 1000},
	}, consumerHandlerLabels))

	// consumer decide labels: {ground_topic=, subscription, goto}
	consumerDecidersLabels := []string{"ground_topic", "subscription", "goto"}
	metrics.consumerDecidersOpened = metrics.tryRegisterGauge(prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "soften_consumer_deciders_opened",
		Help:        "Gauge of opened consume deciders",
		ConstLabels: constLabels,
	}, consumerDecidersLabels))
	// consumer decide labels: {ground_topic=, level, status, subscription, topic, goto}
	consumerDeciderLabels := consumerHandlerLabels
	metrics.consumerDecideSuccess = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consumer_decide_success",
		Help:        "Counter of check success by consume checkers",
		ConstLabels: constLabels,
	}, consumerDeciderLabels))
	metrics.consumerDecideFailed = metrics.tryRegisterCounter(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "soften_consumer_decide_failed",
		Help:        "Counter of decide failed by consume checkers",
		ConstLabels: constLabels,
	}, consumerDeciderLabels))
	metrics.consumerDecideLatency = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_decide_latency",
		Help:        "Decide latency experienced by consume deciders",
		ConstLabels: constLabels,
		Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, consumerDeciderLabels))
	metrics.consumerDecideLatencyFromReceive = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_decide_latency_from_Receive",
		Help:        "Decide latency from receive time to decided time",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
			24 * 3600, 2 * 24 * 3600, 3 * 24 * 3600, 4 * 24 * 3600, 5 * 24 * 3600, 6 * 24 * 3600, 7 * 24 * 3600, 15 * 24 * 3600, 30 * 24 * 3600},
	}, consumerDeciderLabels))
	metrics.consumerDecideLatencyFromListen = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_decide_latency_from_Listen",
		Help:        "Decide latency from listen time to decided time",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
			24 * 3600},
	}, consumerDeciderLabels))
	metrics.consumerDecideLatencyFromLPCheck = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_consumer_decide_latency_from_LPCheck",
		Help:        "Decide latency from listen prev-check start time to decided time",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
			24 * 3600},
	}, consumerDeciderLabels))

	// listener consumer handle labels: {ground_topic=, level, status, subscription, topic, goto}
	listenerMessagesLabels := consumerHandlerLabels
	metrics.messageEndLatencyFromPublish = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_message_end_latency_from_Publish",
		Help:        "Message end latency from publish time to end time (discard, done, dead)",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
			24 * 3600, 2 * 24 * 3600, 3 * 24 * 3600, 4 * 24 * 3600, 5 * 24 * 3600, 6 * 24 * 3600, 7 * 24 * 3600, 15 * 24 * 3600, 30 * 24 * 3600},
	}, listenerMessagesLabels))
	metrics.messageEndLatencyFromEvent = metrics.tryRegisterHistogram(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "soften_message_end_latency_from_Event",
		Help:        "Message end latency from event time to end time (discard, done, dead)",
		ConstLabels: constLabels,
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
			60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
			600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
			3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
			24 * 3600, 2 * 24 * 3600, 3 * 24 * 3600, 4 * 24 * 3600, 5 * 24 * 3600, 6 * 24 * 3600, 7 * 24 * 3600, 15 * 24 * 3600, 30 * 24 * 3600},
	}, listenerMessagesLabels))

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
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.clientsMetricsMap.LoadOrStore(key, &ClientMetrics{
		ClientsOpened: v.clientsOpened.With(labels),
	})
	return metrics.(*ClientMetrics)
}

func (v *MetricsProvider) GetProducerMetrics(groundTopic, topic string) *ProducerMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "topic": topic}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.producersMetricsMap.LoadOrStore(key, &ProducerMetrics{
		ProducersOpened:          v.producersOpened.With(labels),
		PublishSuccess:           v.producerPublishSuccess.With(labels),
		PublishFailed:            v.producerPublishFailed.With(labels),
		PublishLatencyFromPCheck: v.producerPublishLatencyFromPCheck.With(labels),
		PublishLatencyFromEvent:  v.producerPublishLatencyFromEvent.With(labels),
	})
	return metrics.(*ProducerMetrics)
}

func (v *MetricsProvider) GetProducerCheckersMetrics(groundTopic, topic string, checkType string) *ProducerCheckersMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "topic": topic, "check_type": checkType}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.producerCheckersMetricsMap.LoadOrStore(key, &ProducerCheckersMetrics{
		CheckersOpened: v.producerCheckersOpened.With(labels),
	})
	return metrics.(*ProducerCheckersMetrics)
}

func (v *MetricsProvider) GetProducerCheckerMetrics(groundTopic, topic string, checkType string) *ProducerCheckerMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "topic": topic, "check_type": checkType}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.producerCheckMetricsMap.LoadOrStore(key, &ProducerCheckerMetrics{
		CheckPassed:   v.producerCheckPassed.With(labels),
		CheckRejected: v.producerCheckRejected.With(labels),
		CheckLatency:  v.producerCheckLatency.With(labels),
	})
	return metrics.(*ProducerCheckerMetrics)
}

func (v *MetricsProvider) GetProducerDeciderMetrics(groundTopic, topic, handleGoto string) *ProducerDeciderMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "topic": topic, "goto": handleGoto}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.producerDecideMetricsMap.LoadOrStore(key, &ProducerDeciderMetrics{
		DecidersOpened: v.producerDecidersOpened.With(labels),
		DecideSuccess:  v.producerDecideSuccess.With(labels),
		DecideFailed:   v.producerDecideFailed.With(labels),
		DecideLatency:  v.producerDecideLatency.With(labels),
	})
	return metrics.(*ProducerDeciderMetrics)
}

func (v *MetricsProvider) GetListenMetrics(groundTopic string, subscription string) *ListenerMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "subscription": subscription}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.listenersMetricsMap.LoadOrStore(key, &ListenerMetrics{
		ListenersOpened:  v.listenersOpened.With(labels),
		ListenersRunning: v.listenersRunning.With(labels),
	})
	return metrics.(*ListenerMetrics)

}

func (v *MetricsProvider) GetListenerCheckersMetrics(groundTopic string, subscription string, checkType string) *ListenerCheckersMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "subscription": subscription, "check_type": checkType}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.consumerCheckersMetricsMap.LoadOrStore(key, &ListenerCheckersMetrics{
		CheckersOpened: v.consumerCheckersOpened.With(labels),
	})
	return metrics.(*ListenerCheckersMetrics)
}

func (v *MetricsProvider) GetListenerCheckerMetrics(groundTopic string, level TopicLevel, status MessageStatus, subscription string, topic string, checkType string) *ListenerCheckerMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "level": level.String(), "status": status.String(),
		"subscription": subscription, "topic": topic, "check_type": checkType}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.consumerCheckMetricsMap.LoadOrStore(key, &ListenerCheckerMetrics{
		CheckPassed:   v.consumerCheckPassed.With(labels),
		CheckRejected: v.consumerCheckRejected.With(labels),
		CheckLatency:  v.consumerCheckLatency.With(labels),
	})
	return metrics.(*ListenerCheckerMetrics)
}

func (v *MetricsProvider) GetListenerConsumerMetrics(groundTopic string, level TopicLevel, status MessageStatus, subscription string, topic string) *ListenerConsumerMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "level": level.String(), "status": status.String(),
		"subscription": subscription, "topic": topic}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.consumerConsumeMetricsMap.LoadOrStore(key, &ListenerConsumerMetrics{
		ConsumersOpened:           v.consumersOpened.With(labels),
		ReceiveLatencyFromPublish: v.consumerReceiveLatencyFromPublish.With(labels),
		ListenLatency:             v.consumerListenLatency.With(labels),
		ListenLatencyFromEvent:    v.consumerListenLatencyFromEvent.With(labels),
		ConsumeMessageAcks:        v.consumerConsumeMessageAcks.With(labels),
		ConsumeMessageNacks:       v.consumerConsumeMessageNacks.With(labels),
		ConsumeMessageEscape:      v.consumerConsumeMessageEscape.With(labels),
	})
	return metrics.(*ListenerConsumerMetrics)

}

func (v *MetricsProvider) GetListenerHandleMetrics(groundTopic string, level TopicLevel, status MessageStatus, subscription, topic string, msgGoto DecideGoto) *ListenerHandlerMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "level": level.String(), "status": status.String(),
		"subscription": subscription, "topic": topic, "goto": msgGoto.String()}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.consumerHandleMetricsMap.LoadOrStore(key, &ListenerHandlerMetrics{
		HandleLatency:        v.consumerHandleLatency.With(labels),
		HandleReconsumeTimes: v.consumerHandleConsumes.With(labels),
	})
	return metrics.(*ListenerHandlerMetrics)
}

func (v *MetricsProvider) GetListenerDecidersMetrics(groundTopic, subscription string, msgGoto DecideGoto) *ListenerDecidersMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "subscription": subscription, "goto": msgGoto.String()}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.consumerDecidersMetricsMap.LoadOrStore(key, &ListenerDecidersMetrics{
		DecidersOpened: v.consumerDecidersOpened.With(labels),
	})
	return metrics.(*ListenerDecidersMetrics)
}

func (v *MetricsProvider) GetListenerDecideMetrics(groundTopic string, level TopicLevel, status MessageStatus, subscription, topic string, msgGoto DecideGoto) *ListenerDeciderMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "level": level.String(), "status": status.String(),
		"subscription": subscription, "goto": msgGoto.String(), "topic": topic}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.consumerDecideMetricsMap.LoadOrStore(key, &ListenerDeciderMetrics{
		DecideSuccess:             v.consumerDecideSuccess.With(labels),
		DecideFailed:              v.consumerDecideFailed.With(labels),
		DecideLatency:             v.consumerDecideLatency.With(labels),
		DecideLatencyFromReceive:  v.consumerDecideLatencyFromReceive.With(labels),
		DecideLatencyFromListen:   v.consumerDecideLatencyFromListen.With(labels),
		DecicdeLatencyFromLPCheck: v.consumerDecideLatencyFromLPCheck.With(labels),
	})
	return metrics.(*ListenerDeciderMetrics)
}

func (v *MetricsProvider) GetListenerMessagesMetrics(groundTopic string, level TopicLevel, status MessageStatus, subscription, topic string, msgGoto DecideGoto) *ListenerMessagesMetrics {
	labels := prometheus.Labels{"ground_topic": groundTopic, "level": level.String(), "status": status.String(),
		"subscription": subscription, "goto": msgGoto.String(), "topic": topic}
	key := v.convertLabelsToKey(labels)
	metrics, _ := v.messageMetricsMap.LoadOrStore(key, &ListenerMessagesMetrics{
		EndLatencyFromPublish: v.messageEndLatencyFromPublish.With(labels),
		EndLatencyFromEvent:   v.messageEndLatencyFromEvent.With(labels),
	})
	return metrics.(*ListenerMessagesMetrics)
}

func (v *MetricsProvider) convertLabelsToKey(labels prometheus.Labels) string {
	kvs := make([]string, 0)
	for k, v := range labels {
		kvs = append(kvs, k+"="+v)
	}
	sort.Strings(kvs)
	return strings.Join(kvs, "; ")
}

// ------ helper ------

// ClientMetrics composes these metrics related to client with {url=} label:
// soften_clients_opened {url=};
type ClientMetrics struct {
	ClientsOpened prometheus.Gauge
}

// ProducerMetrics composes these metrics generated during producing with {topic=} label:
type ProducerMetrics struct {
	ProducersOpened          prometheus.Gauge    // gauge of producers
	PublishSuccess           prometheus.Counter  // counter of producers publish messages successfully
	PublishFailed            prometheus.Counter  // counter of producers publish messages failed
	PublishLatencyFromEvent  prometheus.Observer // producers event time to PC latency
	PublishLatencyFromPCheck prometheus.Observer // producers publish latency
}

type ProducerCheckersMetrics CheckersMetrics

// ProducerCheckerMetrics composes these metrics generated during producing with {topic=, check_type=} label:
type ProducerCheckerMetrics CheckerMetrics

type ListenerCheckersMetrics CheckersMetrics

// ListenerCheckerMetrics composes these metrics generated during listening with {topic=, check_type=} label:
type ListenerCheckerMetrics CheckerMetrics

// ProducerDeciderMetrics composes these metrics generated during producing with {topic=, goto=} label:
type ProducerDeciderMetrics struct {
	DecidersOpened prometheus.Gauge    // gauge of routers
	DecideSuccess  prometheus.Counter  // counter of producers decide messages successfully
	DecideFailed   prometheus.Counter  // counter of producers decide messages failed
	DecideLatency  prometheus.Observer // Transfer latency
}

// ListenerDecidersMetrics composes these decide metrics generated during listening with {topics=, levels=, goto=} label:
type ListenerDecidersMetrics struct {
	DecidersOpened prometheus.Gauge // gauge of routers
}

// ListenerDeciderMetrics composes these decide metrics generated during listening with {topics, levels, topic, level, status, goto} label:
type ListenerDeciderMetrics struct {
	DecideSuccess             prometheus.Counter  // counter of producers decide messages
	DecideFailed              prometheus.Counter  // counter of producers decide messages
	DecideLatency             prometheus.Observer // Transfer latency
	DecideLatencyFromReceive  prometheus.Observer
	DecideLatencyFromListen   prometheus.Observer
	DecicdeLatencyFromLPCheck prometheus.Observer
}

// ListenerMetrics composed these metrics generated during listening with {topics=,levels=} labels:
type ListenerMetrics struct {
	ListenersOpened  prometheus.Gauge // gauge of opened listeners
	ListenersRunning prometheus.Gauge // gauge of running listeners
}

// ListenerConsumerMetrics composes these metrics generated during consume with {topic=, level=, status=} label:
// soften_client_consumers_opened {topic=, level=, status=};
// soften_client_messages_received
// soften_client_messages_listened
// soften_client_messages_processed
type ListenerConsumerMetrics struct {
	ConsumersOpened           prometheus.Gauge    // gauge of consumers
	ReceiveLatencyFromPublish prometheus.Observer // labels: topic, level, status
	ListenLatency             prometheus.Observer // labels: topic, level, status
	ListenLatencyFromEvent    prometheus.Observer // labels: topic, level, status
	ConsumeMessageAcks        prometheus.Counter
	ConsumeMessageNacks       prometheus.Counter
	ConsumeMessageEscape      prometheus.Counter
}

type ListenerHandlerMetrics struct {
	HandleLatency        prometheus.Observer // labels: topics, levels, topic, level, status
	HandleReconsumeTimes prometheus.Observer
}

// ListenerMessagesMetrics composed these metrics related to transfer process:
// soften_client_transfer_opened {topics=,levels=};
type ListenerMessagesMetrics struct {
	EndLatencyFromPublish prometheus.Observer
	EndLatencyFromEvent   prometheus.Observer
}

// ------ helper common -----

type CheckersMetrics struct {
	CheckersOpened prometheus.Gauge // gauge of produce checkers
}

type CheckerMetrics struct {
	CheckPassed   prometheus.Counter // counter of produce check passed
	CheckRejected prometheus.Counter // counter of produce check rejected
	CheckLatency  prometheus.Observer
}
