package config

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/topic"
)

// ------ library configuration ------

var DebugMode = false

// ------ client configuration ------

// ClientConfig define client configuration with json formatter. It referred to pulsar.ClientOptions
type ClientConfig struct {
	URL                     string `json:"url"`
	ConnectionTimeout       uint   `json:"connection_timeout"`         // Optional: default 5s
	OperationTimeout        uint   `json:"operation_timeout"`          // Optional: default 30s
	MaxConnectionsPerBroker uint   `json:"max_connections_per_broker"` // Optional: default 1
	TLSTrustCertsFilePath   string

	Logger         log.Logger `json:"-"`
	Authentication pulsar.Authentication
}

// ------ producer configuration ------

// ProducerConfig define producer configuration with json formatter. It referred to pulsar.ProducerOptions
type ProducerConfig struct {
	Topic              string `json:"topic"`                // Required:
	Name               string `json:"name"`                 // Optional:
	SendTimeoutMs      int64  `json:"send_timeout_ms"`      // Optional:
	SendTimeout        uint   `json:"send_timeout"`         // Optional:
	MaxPendingMessages int    `json:"max_pending_messages"` // Optional:
	HashingScheme      int    `json:"hashing_scheme"`       // Optional:
	CompressionType    int    `json:"compression_type"`     // Optional:
	CompressionLevel   int    `json:"compression_level"`    // Optional:

	DisableBatching           bool  `json:"disable_batching"`              // Optional:
	BatchingMaxPublishDelayMs int64 `json:"batching_max_publish_delay_ms"` // Optional:
	BatchingMaxMessages       uint  `json:"batching_max_messages"`         // Optional:
	BatchingMaxSize           uint  `json:"batching_max_size"`             // Optional:
	BatcherBuilderType        int   `json:"batcher_builder_type"`          // Optional:

	HandleTimeout     uint                `json:"handle_timeout"`      // Optional: 发送消息超时时间, Default: 30 seconds
	DeadEnable        bool                `json:"dead_enable"`         // Optional:
	DiscardEnable     bool                `json:"discard_enable"`      // Optional:
	BlockingEnable    bool                `json:"blocking_enable"`     // Optional:
	PendingEnable     bool                `json:"pending_enable"`      // Optional:
	RetryingEnable    bool                `json:"retrying_enable"`     // Optional:
	RouteEnable       bool                `json:"route_enable"`        // Optional: 自定义路由开关
	Route             *RoutePolicy        `json:"route"`               // Optional:
	UpgradeEnable     bool                `json:"upgrade_enable"`      // Optional:
	UpgradeTopicLevel internal.TopicLevel `json:"upgrade_topic_level"` // Optional: 主动升级队列级别
	DegradeEnable     bool                `json:"degrade_enable"`      // Optional:
	DegradeTopicLevel internal.TopicLevel `json:"degrade_topic_level"` // Optional:
}

// ------ consumer configuration (multi-status) ------

// ConsumerConfig define producer configuration with json formatter. It referred to pulsar.ConsumerOptions
type ConsumerConfig struct {
	Concurrency                 *ConcurrencyPolicy                 `json:"concurrency"`                   // Optional: 并发控制
	Topics                      []string                           `json:"topics"`                        // Alternative with Topic: 如果有值, Topic 配置将被忽略; 第一个为核心主题
	Topic                       string                             `json:"topic"`                         // Alternative with Topics: Topics缺失的情况下，该值生效
	SubscriptionName            string                             `json:"subscription_name"`             // Required:
	Level                       internal.TopicLevel                `json:"level"`                         // Optional:
	Type                        pulsar.SubscriptionType            `json:"type"`                          // Optional:
	SubscriptionInitialPosition pulsar.SubscriptionInitialPosition `json:"subscription_initial_position"` // Optional:
	NackBackoffPolicy           pulsar.NackBackoffPolicy           `json:"-"`                             // Optional: Unrecommended, compatible with origin pulsar client
	NackRedeliveryDelay         time.Duration                      `json:"nack_redelivery_delay"`         // Optional: Unrecommended, compatible with origin pulsar client
	RetryEnable                 bool                               `json:"retry_enable"`                  // Optional: Unrecommended, compatible with origin pulsar client
	DLQ                         *DLQPolicy                         `json:"dlq"`                           // Optional: Unrecommended, compatible with origin pulsar client
	ConsumeMaxTimes             int                                `json:"consume_max_times"`             // Optional: 最大消费次数
	BalanceStrategy             internal.BalanceStrategy           `json:"balance_strategy"`              // Optional: 消费均衡策略
	Ready                       *StatusPolicy                      `json:"ready"`                         // Optional: Ready 主题检查策略
	BlockingEnable              bool                               `json:"blocking_enable"`               // Optional: Blocking 检查开关
	Blocking                    *StatusPolicy                      `json:"blocking"`                      // Optional: Blocking 主题检查策略
	PendingEnable               bool                               `json:"pending_enable"`                // Optional: Pending 检查开关
	Pending                     *StatusPolicy                      `json:"pending"`                       // Optional: Pending 主题检查策略
	RetryingEnable              bool                               `json:"retrying_enable"`               // Optional: Retrying 重试检查开关
	Retrying                    *StatusPolicy                      `json:"retrying"`                      // Optional: Retrying 主题检查策略
	RerouteEnable               bool                               `json:"reroute_enable"`                // Optional: PreReRoute 检查开关, 默认false
	Reroute                     *ReroutePolicy                     `json:"reroute"`                       // Optional: Handle失败时的动态重路由
	UpgradeEnable               bool                               `json:"upgrade_enable"`                // Optional: 主动升级
	UpgradeTopicLevel           internal.TopicLevel                `json:"upgrade_topic_level"`           // Optional: 主动升级队列级别
	DegradeEnable               bool                               `json:"degrade_enable"`                // Optional: 主动降级
	DegradeTopicLevel           internal.TopicLevel                `json:"degrade_topic_level"`           // Optional: 主动降级队列级别
	DeadEnable                  bool                               `json:"dead_enable"`                   // Optional: 死信队列开关, 默认false; 如果所有校验器都没能校验通过, 应用代码需要自行Ack或者Nack
	Dead                        *StatusPolicy                      `json:"dead"`                          // Optional: Dead 主题检查策略
	DiscardEnable               bool                               `json:"discard_enable"`                // Optional: 丢弃消息开关, 默认false
	Discard                     *StatusPolicy                      `json:"discard"`                       // Optional: Di 主题检查策略
	HandleTimeout               uint                               `json:"handle_timeout"`                // Optional: 处理消息超时时间 (default: 30 seconds)

	// ------ consumer configuration (multi-level) ------

	Levels               topic.Levels             `json:"levels"`                 // Required: 默认L1, 且消费的Topic Level级别, len(Topics) == 1 or Topic存在的时候才生效
	LevelBalanceStrategy internal.BalanceStrategy `json:"level_balance_strategy"` // Optional: Topic级别消费策略
	LevelPolicies        LevelPolicies            `json:"level_policies"`         // Optional: 级别消费策略
}

type LevelPolicies map[internal.TopicLevel]*LevelPolicy

// ------ helper structs ------

// StatusPolicy 定义单状态的消费重入策略。
// 消费权重: 按整形值记录。
// 补偿策略:
// (1) 补偿延迟小于等于 NackBackoffMaxDelay(默认1min)时, 优先选择 Nack方式进行补偿;
// (2) 借助于 Reentrant 进行补偿, 每次重入代表额外增加一个 ReentrantDelay 延迟;
// (3) 如果 补偿延迟 - ReentrantDelay 仍然大于 NackBackoffMaxDelay, 那么会发生多次重入。
type StatusPolicy struct {
	ConsumeWeight     uint                `json:"consume_weight"`      // Optional: 消费权重
	ConsumeMaxTimes   int                 `json:"consume_max_times"`   // Optional: 消费次数上限
	BackoffDelays     []string            `json:"backoff_delays"`      // Optional: 补偿延迟策略 e.g: [5s, 2m, 1h], 如果值大于 ReentrantDelay 时，自动取整为 ReentrantDelay 的整数倍 (默认向下取整)
	BackoffPolicy     StatusBackoffPolicy `json:"-"`                   // Optional: 补偿策略, 优先级高于 BackoffDelays
	ReentrantDelay    uint                `json:"reentrant_delay"`     // Optional: 重入延迟
	ReentrantMaxTimes int                 `json:"reentrant_max_times"` // Optional: 重入次数上限
	NackMaxDelay      int                 `json:"nack_max_delay"`      // Optional: Nack延迟上限
	NackMaxTimes      int                 `json:"nack_max_times"`      // Optional: Nack次数上限
}

type LevelPolicy struct {
	ConsumeWeight uint                `json:"consume_weight"` // Optional: consume weight
	UpgradeLevel  internal.TopicLevel `json:"upgrade_level"`  // Optional: upgrade level
	DegradeLevel  internal.TopicLevel `json:"degrade_level"`  // Optional: degrade level

	Ready    *StatusPolicy `json:"ready"`    // Optional: Ready status policy
	Blocking *StatusPolicy `json:"blocking"` // Optional: Blocking status policy
	Pending  *StatusPolicy `json:"pending"`  // Optional: Pending status policy
	Retrying *StatusPolicy `json:"retrying"` // Optional: Retrying status policy
}

type RoutePolicy struct {
	ConnectInSyncEnable bool `json:"connect_in_sync_enable"` // Optional: 是否同步建立连接, 首次发送消息需阻塞等待客户端与服务端连接完成
}

type ReroutePolicy struct {
	ConnectInSyncEnable bool `json:"connect_in_sync_enable"` // Optional: 是否同步建立连接, 首次发送消息需阻塞等待客户端与服务端连接完成
}

// DLQPolicy represents the configuration for the Dead Letter Queue multiStatusConsumeFacade policy. It referred to pulsar.DLQPolicy
type DLQPolicy struct {
	// MaxDeliveries specifies the maximum number of times that a message will be delivered before being
	// sent to the dead letter queue.
	MaxDeliveries uint32 `json:"max_deliveries"`

	// DeadLetterTopic specifies the name of the topic where the failing messages will be sent.
	DeadLetterTopic string `json:"dead_letter_topic"`

	// RetryLetterTopic specifies the name of the topic where the retry messages will be sent.
	RetryLetterTopic string `json:"retry_letter_topic"`
}

type ConcurrencyPolicy struct {
	CorePoolSize    uint `json:"core_pool_size"`    // Optional: default 1
	MaximumPoolSize uint `json:"maximum_pool_size"` // Optional: default 1
	KeepAliveTime   uint `json:"keep_alive_time"`   // Optional: default 60(s)

	PanicHandler func(interface{}) `json:"-"` // Optional, handle panics comes from executing message handler
}
