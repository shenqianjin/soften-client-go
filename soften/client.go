package soften

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type Client interface {
	// RawClient returns its inline pulsar.Client instance
	RawClient() pulsar.Client

	// CreateProducer create producer, with checkpoints as optional parameter
	CreateProducer(conf config.ProducerConfig, checkpoints ...checker.ProduceCheckpoint) (*producer, error)

	// CreateListener create listener, with checkpoints as optional parameter.
	//
	// A listener embeds a couple of consumers of different statuses or levels,
	// And then listens all messages of them within configured status policies and leveled policies.
	// Checkpoint provides ability to decide messages which meets checking conditions goto these statuses or levels.
	CreateListener(conf config.ConsumerConfig, checkpoints ...checker.ConsumeCheckpoint) (*consumeListener, error)

	// Close closes the Client and free correlative resources
	Close()
}

type client struct {
	pulsar.Client
	logger log.Logger
	url    string

	metricsProvider *internal.MetricsProvider
}

func NewClient(conf config.ClientConfig) (*client, error) {
	// validate and default conf
	if err := config.Validator.ValidateAndDefaultClientConfig(&conf); err != nil {
		return nil, err
	}
	// create pulsar client
	clientOption := pulsar.ClientOptions{
		URL:                     conf.URL,
		ConnectionTimeout:       time.Duration(conf.ConnectionTimeout) * time.Second,
		OperationTimeout:        time.Duration(conf.OperationTimeout) * time.Second,
		MaxConnectionsPerBroker: int(conf.MaxConnectionsPerBroker),
		Logger:                  conf.Logger,
		MetricsCardinality:      conf.MetricsCardinality,
	}
	pulsarClient, err := pulsar.NewClient(clientOption)
	if err != nil {
		return nil, err
	}
	metricsProvider := internal.NewMetricsProvider(2, nil,
		string(conf.MetricsPolicy.MetricsTopicMode), conf.MetricsPolicy.MetricsBuckets)
	// create client
	cli := &client{Client: pulsarClient, logger: conf.Logger, metricsProvider: metricsProvider, url: conf.URL}
	cli.metricsProvider.GetClientMetrics(cli.url).ClientsOpened.Inc()
	cli.logger.Infof("created soften client, url: %s", conf.URL)
	return cli, nil
}

func (c *client) RawClient() pulsar.Client {
	return c.Client
}

func (c *client) CreateProducer(conf config.ProducerConfig, checkpoints ...checker.ProduceCheckpoint) (*producer, error) {
	// validate and default config
	if err := config.Validator.ValidateAndDefaultProducerConfig(&conf); err != nil {
		return nil, err
	}
	// validate checkpoints
	checkpointMap, err := checker.Validator.ValidateProduceCheckpoint(checkpoints)
	if err != nil {
		return nil, err
	}
	// create producer
	return newProducer(c, &conf, checkpointMap)

}

func (c *client) CreateListener(conf config.ConsumerConfig, checkpoints ...checker.ConsumeCheckpoint) (*consumeListener, error) {
	// validate and default config
	if err := config.Validator.ValidateAndDefaultConsumerConfig(&conf); err != nil {
		return nil, err
	}
	// validate checkpoints
	checkpointMap, err := checker.Validator.ValidateConsumeCheckpoint(checkpoints)
	if err != nil {
		return nil, err
	}
	// create consumer
	if consumer, err := newConsumeListener(c, conf, checkpointMap); err != nil {
		return nil, err
	} else {
		return consumer, err
	}
}

func (c *client) Close() {
	c.logger.Info("closed soften client")
	c.Client.Close()
	c.metricsProvider.GetClientMetrics(c.url).ClientsOpened.Dec()
}
