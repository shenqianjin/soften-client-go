package soften

import (
	"errors"
	"time"

	"github.com/shenqianjin/soften-client-go/soften/checker"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type Client interface {
	RawClient() pulsar.Client
	CreateProducer(conf config.ProducerConfig, checkpoints ...checker.ProduceCheckpoint) (*producer, error)
	CreateListener(conf config.ConsumerConfig, checkpoints ...checker.ConsumeCheckpoint) (*consumeListener, error)
	Close() // close the Client and free associated resources
}

type client struct {
	pulsar.Client
	logger log.Logger

	metricsProvider *internal.MetricsProvider
	metrics         *internal.ClientMetrics
}

func NewClient(conf config.ClientConfig) (*client, error) {
	// validate and default conf
	if err := config.Validator.ValidateAndDefaultClientConfig(&conf); err != nil {
		return nil, err
	}
	// create client
	clientOption := pulsar.ClientOptions{
		URL:                     conf.URL,
		ConnectionTimeout:       time.Duration(conf.ConnectionTimeout) * time.Second,
		OperationTimeout:        time.Duration(conf.OperationTimeout) * time.Second,
		MaxConnectionsPerBroker: int(conf.MaxConnectionsPerBroker),
		Logger:                  conf.Logger,
	}
	pulsarClient, err := pulsar.NewClient(clientOption)
	if err != nil {
		return nil, err
	}
	metricsProvider := internal.NewMetricsProvider(2, nil)
	cli := &client{Client: pulsarClient, logger: conf.Logger, metricsProvider: metricsProvider,
		metrics: metricsProvider.GetClientMetrics(conf.URL)}
	cli.metrics.ClientsOpened.Inc()
	cli.logger.Infof("created soften client, url: %s", conf.URL)
	return cli, nil
}

func (c *client) RawClient() pulsar.Client {
	return c.Client
}

func (c *client) CreateProducer(conf config.ProducerConfig, checkpoints ...checker.ProduceCheckpoint) (*producer, error) {
	if conf.Topic == "" {
		return nil, errors.New("topic is empty")
	}
	// validate checkpoints
	checkpointMap, err := checker.Validator.ValidateProduceCheckpoint(checkpoints)
	if err != nil {
		return nil, err
	}
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
	c.metrics.ClientsOpened.Dec()
}
