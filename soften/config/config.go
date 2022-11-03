package config

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

var DebugMode = true

// ------ library configuration ------

type PulsarConfig struct {
	Client    ClientConfig     `json:"client"`    // client configuration
	Producer  ProducerConfig   `json:"producer"`  // producer instance configuration, it has higher priority than producers.
	Producers []ProducerConfig `json:"producers"` // a list of producer instances to support more than one producer instances, either a producer.
	Consumer  ConsumerConfig   `json:"consumer"`  // consumer instance configuration, it has higher priority than consumers.
	Consumers []ConsumerConfig `json:"consumers"` // a list of consumer instances to support more than one consumer instances, either a consumer.
}

// ------ client configuration ------

// ClientConfig define client configuration with json formatter. It referred to pulsar.ClientOptions
type ClientConfig struct {
	URL                     string                `json:"url"`
	ConnectionTimeout       uint                  `json:"connection_timeout"`         // Optional: default 5s
	OperationTimeout        uint                  `json:"operation_timeout"`          // Optional: default 30s
	MaxConnectionsPerBroker uint                  `json:"max_connections_per_broker"` // Optional: default 1
	TLSTrustCertsFilePath   string                `json:"tls_trust_certs_file_path"`  // Optional: trusted TLS certificate file
	Authentication          pulsar.Authentication `json:"-"`                          // Optional: custom authentication interface

	LogLevel int        `json:"log_level"` // log trace level (logrus): 0 panic; 1 Fatal; 2 Error; 3 Warn; 4 Info; 5 Debug; 6 Trace
	Logger   log.Logger `json:"-"`

	// Specify metric cardinality to the tenant, namespace or topic levels, or remove it completely.
	// Default: pulsar.MetricsCardinalityNamespace --> pulsar.MetricsCardinalityTopic
	MetricsCardinality pulsar.MetricsCardinality `json:"metrics_cardinality"`
}

// ------ producer configuration ------

// ProducerConfig define producer configuration with json formatter. It referred to pulsar.ProducerOptions
type ProducerConfig struct {
	Topic              string              `json:"topic"`                // Required: the topic this producer will be publishing on
	Level              internal.TopicLevel `json:"level"`                // Optional: default L1. real topic is Topic + Level
	Name               string              `json:"name"`                 // Optional: name for the producer
	SendTimeout        uint                `json:"send_timeout"`         // Optional: timeout since sent. Default is 30s, negative such as -1 to disable
	MaxPendingMessages int                 `json:"max_pending_messages"` // Optional: max size of pending messages to receive an ack from the broker
	HashingScheme      int                 `json:"hashing_scheme"`       // Optional: hash mode: 0 JavaStringHash; 1 Murmur3_32Hash
	CompressionType    int                 `json:"compression_type"`     // Optional: compression type: 0 NoCompression; 1 LZ4; 2 ZLib; 3 ZSTD
	CompressionLevel   int                 `json:"compression_level"`    // Optional: compression level: 0 Default; 1 Faster; 2 Better

	DisableBatching         bool  `json:"disable_batching"`           // Optional:
	BatchingMaxPublishDelay int64 `json:"batching_max_publish_delay"` // Optional:
	BatchingMaxMessages     uint  `json:"batching_max_messages"`      // Optional:
	BatchingMaxSize         uint  `json:"batching_max_size"`          // Optional:
	BatcherBuilderType      int   `json:"batcher_builder_type"`       // Optional: batcher builcer type: 0 DefaultBatchBuilder; 1 KeyBasedBatchBuilder

	DiscardEnable  *bool           `json:"discard_enable"` // Optional:
	DeadEnable     *bool           `json:"dead_enable"`    // Optional:
	Dead           *ShiftPolicy    `json:"dead"`           // Optional: fix dead to D1 currently
	TransferEnable *bool           `json:"route_enable"`   // Optional: 自定义路由开关
	Transfer       *TransferPolicy `json:"Transfer"`       // Optional:
	UpgradeEnable  *bool           `json:"upgrade_enable"` // Optional:
	Upgrade        *ShiftPolicy    `json:"upgrade"`        // Optional: 主动升级队列级别
	DegradeEnable  *bool           `json:"degrade_enable"` // Optional:
	Degrade        *ShiftPolicy    `json:"degrade"`        // Optional:
	ShiftEnable    *bool           `json:"shift_enable"`   // Optional:
	Shift          *ShiftPolicy    `json:"shift"`          // Optional:

	HandleTimeout uint           `json:"handle_timeout"` // Optional: 发送消息超时时间, Default: 30 seconds
	Publish       *PublishPolicy `json:"shift"`          // Optional: 发布策略

}

// ------ consumer configuration (multi-status) ------

// ConsumerConfig define producer configuration with json formatter. It referred to pulsar.ConsumerOptions
type ConsumerConfig struct {
	*LevelPolicy

	Concurrency                 *ConcurrencyPolicy                 `json:"concurrency"`                   // Optional: 并发控制
	Topics                      []string                           `json:"topics"`                        // Alternative with Topic: 如果有值, Topic 配置将被忽略; 第一个为核心主题
	Topic                       string                             `json:"topic"`                         // Alternative with Topics: Topics缺失的情况下，该值生效
	SubscriptionName            string                             `json:"subscription_name"`             // Required:
	Level                       internal.TopicLevel                `json:"level"`                         // Optional:
	Type                        pulsar.SubscriptionType            `json:"type"`                          // Optional: 订阅类型: 0 Exclusive; 1 Shared; 2 Failover; 3 KeyShared
	SubscriptionInitialPosition pulsar.SubscriptionInitialPosition `json:"subscription_initial_position"` // Optional: 订阅初始消费位置: 0 Latest; 1 Earliest
	NackBackoffPolicy           pulsar.NackBackoffPolicy           `json:"-"`                             // Optional: Unrecommended, compatible with origin pulsar client
	NackRedeliveryDelay         uint                               `json:"nack_redelivery_delay"`         // Optional: Unrecommended, compatible with origin pulsar client
	RetryEnable                 bool                               `json:"retry_enable"`                  // Optional: Unrecommended, compatible with origin pulsar client
	DLQ                         *DLQPolicy                         `json:"dlq"`                           // Optional: Unrecommended, compatible with origin pulsar client
	BalanceStrategy             internal.BalanceStrategy           `json:"balance_strategy"`              // Optional: 消费均衡策略
	HandleTimeout               uint                               `json:"handle_timeout"`                // Optional: 处理消息超时时间 (default: 30 seconds)
	EscapeHandler               EscapeHandler                      `json:"-"`                             // Optional: 逃逸消息处理器, 优先级高于 EscapeHandleType
	EscapeHandleType            EscapeHandleType                   `json:"escape_handle_type"`            // Optional: 逃逸消息处理类型: 0 Panic; 1 Ack; 2 Nack

	// ------ consumer configuration (multi-level) ------

	Levels               message.Levels           `json:"levels"`                 // Required: 默认L1, 且消费的Topic Level级别, len(Topics) == 1 or Topic存在的时候才生效
	LevelBalanceStrategy internal.BalanceStrategy `json:"level_balance_strategy"` // Optional: Topic级别消费策略
	LevelPolicies        LevelPolicies            `json:"level_policies"`         // Optional: 级别消费策略
}

type LevelPolicies map[internal.TopicLevel]*LevelPolicy

// ------ helper structs ------

// LevelPolicy defines different policies for different levels.
// status/transfer policies default as the same with main level if missing.
// upgrade/degrade policies must be specified one by one for each multi-level
type LevelPolicy struct {
	ConsumeWeight   uint `json:"consume_weight"`    // Optional: consume weight
	ConsumeMaxTimes int  `json:"consume_max_times"` // Optional: 最大消费次数

	UpgradeEnable  *bool           `json:"upgrade_enable"`  // Optional: 升级开关
	Upgrade        *ShiftPolicy    `json:"upgrade"`         // Optional: 升级策略
	DegradeEnable  *bool           `json:"degrade_enable"`  // Optional: 降级开关
	Degrade        *ShiftPolicy    `json:"degrade"`         // Optional: 降级策略
	ShiftEnable    *bool           `json:"shift_enable"`    // Optional: 变换级别开关
	Shift          *ShiftPolicy    `json:"shift"`           // Optional: 变换级别策略
	TransferEnable *bool           `json:"transfer_enable"` // Optional: PreReTransfer 检查开关, 默认false
	Transfer       *TransferPolicy `json:"transfer"`        // Optional: Handle失败时的动态重路由

	Ready          *ReadyPolicy  `json:"ready"`           // Optional: Ready 主题检查策略
	BlockingEnable *bool         `json:"blocking_enable"` // Optional: Blocking 检查开关
	Blocking       *StatusPolicy `json:"blocking"`        // Optional: Blocking 主题检查策略
	PendingEnable  *bool         `json:"pending_enable"`  // Optional: Pending 检查开关
	Pending        *StatusPolicy `json:"pending"`         // Optional: Pending 主题检查策略
	RetryingEnable *bool         `json:"retrying_enable"` // Optional: Retrying 重试检查开关
	Retrying       *StatusPolicy `json:"retrying"`        // Optional: Retrying 主题检查策略
	DeadEnable     *bool         `json:"dead_enable"`     // Optional: 死信队列开关, 默认false; 如果所有校验器都没能校验通过, 应用代码需要自行Ack或者Nack
	Dead           *DeadPolicy   `json:"dead"`            // Optional: Dead 主题检查策略
	DiscardEnable  *bool         `json:"discard_enable"`  // Optional: 丢弃消息开关, 默认false
}

type ReadyPolicy struct {
	ConsumeWeight uint `json:"consume_weight"` // Optional: 消费权重
}

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
	PublishPolicy     *PublishPolicy      `json:"publish"`             // Optional: 发布策略
}

type DeadPolicy struct {
	PublishPolicy *PublishPolicy `json:"publish"` // Optional: 发布策略
}

type TransferPolicy struct {
	ConnectInSyncEnable bool           `json:"connect_in_sync_enable"` // Optional: 是否同步建立连接, 首次发送消息需阻塞等待客户端与服务端连接完成
	Topic               string         `json:"topic"`                  // Optional: 默认转移到的队列: 优先级比 GotoExtra参数中指定的低
	ConsumeDelay        uint64         `json:"consume_delay"`          // Optional: 消费延迟(近似值: 通过1次或者多次重入实现, 不足1次重入延迟时当1次处理), 默认 0
	CountMode           CountPassMode  `json:"count_pass_mode"`        // Optional: 计数传递模式: 0 传递累计计数; 1 重置计数
	PublishPolicy       *PublishPolicy `json:"publish"`                // Optional: 发布策略
}

type ShiftPolicy struct {
	ConnectInSyncEnable bool                `json:"connect_in_sync_enable"` // Optional: 是否同步建立连接, 首次发送消息需阻塞等待客户端与服务端连接完成
	Level               internal.TopicLevel `json:"level"`                  // Optional: 默认升降级的级别: 优先级比 GotoExtra参数中指定的低
	ConsumeDelay        uint64              `json:"consume_delay"`          // Optional: 消费延迟(近似值: 通过1次或者多次重入实现, 不足1次重入延迟时当1次处理), 默认 0
	CountMode           CountPassMode       `json:"count_pass_mode"`        // Optional: 计数传递模式: 0 透传累计计数; 1 重置计数
	PublishPolicy       *PublishPolicy      `json:"publish"`                // Optional: 发布策略
}

type PublishPolicy struct {
	BackoffMaxTimes uint          `json:"backoff_max_times"` // Optional: 发送失败默认重试次数
	BackoffDelays   []string      `json:"backoff_delays"`    // Optional: 失败重试延迟
	BackoffPolicy   BackoffPolicy `json:"-"`                 // Optional: 补偿策略, 优先级高于 BackoffDelays
}

type CountPassMode int

const (
	CountPassThrough CountPassMode = iota // 透传累计计数
	CountPassNull                         // 重置计数
)

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
	CorePoolSize    uint `json:"core_pool_size"`    // Optional: default 16
	MaximumPoolSize uint `json:"maximum_pool_size"` // Optional: fixed same with CorePoolSize currently.
	KeepAliveTime   uint `json:"keep_alive_time"`   // Optional: default 60(s)

	PanicHandler func(interface{}) `json:"-"` // Optional, handle panics comes from executing message handler
}

type ConsumeLimit struct {
	ConsumeMaxTimes  uint
	PendingMaxTimes  uint
	RetryingMaxTimes uint
	BlockingMaxTimes uint
}

// ------ escape handle type ------

type EscapeHandler func(ctx context.Context, msg message.ConsumerMessage)

type EscapeHandleType int

const (
	// EscapeAsPanic panic process if any message is escaped
	EscapeAsPanic EscapeHandleType = iota

	// EscapeAsAck acknowledges these escaped messages, logging it in warn level
	EscapeAsAck

	// EscapeAsNack negatively acknowledges these escaped messages, logging it in warn level
	EscapeAsNack
)
