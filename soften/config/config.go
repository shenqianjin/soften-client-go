package config

import (
	"time"

	"github.com/shenqianjin/soften-client-go/soften/topic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ library configuration ------

var DebugMode = false

// ------ client configuration ------

type ClientConfig struct {
	// extract out from pulsar.ClientOptions
	URL                     string `json:"url"`
	ConnectionTimeout       uint   `json:"connection_timeout"`         // Optional: default 5s
	OperationTimeout        uint   `json:"operation_timeout"`          // Optional: default 30s
	MaxConnectionsPerBroker uint   `json:"max_connections_per_broker"` // Optional: default 1
	TLSTrustCertsFilePath   string

	Logger         log.Logger `json:"-"`
	Authentication pulsar.Authentication
}

// ------ producer configuration ------

type ProducerConfig struct {
	// extract out from pulsar.ProducerOptions
	Topic              string `json:"topic"` // Required:
	Name               string `json:"name"`
	SendTimeoutMs      int64  `json:"send_timeout_ms"`
	SendTimeout        uint   `json:"send_timeout"` // time.Duration // Optional:
	MaxPendingMessages int    `json:"max_pending_messages"`
	HashingScheme      int    `json:"hashing_scheme"`
	CompressionType    int    `json:"compression_type"`
	CompressionLevel   int    `json:"compression_level"`

	DisableBatching           bool  `json:"disable_batching"`
	BatchingMaxPublishDelayMs int64 `json:"batching_max_publish_delay_ms"`
	BatchingMaxMessages       uint  `json:"batching_max_messages"`
	BatchingMaxSize           uint  `json:"batching_max_size"`
	BatcherBuilderType        int   `json:"batcher_builder_type"`

	MessageRouter func(*pulsar.ProducerMessage, pulsar.TopicMetadata) int

	RouteEnable       bool         // Optional: 自定义路由开关
	Route             *RoutePolicy //
	HandleTimeout     uint         `json:"handle_timeout"` // Optional: 发送消息超时时间 (default: 30 seconds)
	UpgradeEnable     bool
	DegradeEnable     bool
	UpgradeTopicLevel internal.TopicLevel `json:"upgrade_topic_level"` // Optional: 主动升级队列级别
	DegradeTopicLevel internal.TopicLevel `json:"degrade_topic_level"`

	//TopicRouter        func(*pulsar.ProducerMessage) internal.TopicLevel // 自定义Topic路由器
	//UidTopicRouters    map[uint32]internal.TopicLevel                    // Uid级别路由静态配置; 优先级低于Bucket级别路由;
	//BucketTopicRouters map[string]internal.TopicLevel                    // Bucket级别路由静态配置; 优先级高于Uid级别路由;

	DeadEnable     bool
	DiscardEnable  bool
	BlockingEnable bool
	PendingEnable  bool
	RetryingEnable bool
	//RouteEnable    bool
	//UpgradeEnable  bool
	//DegradeEnable  bool
}

// ------ consumer configuration (multi-status) ------

type ConsumerConfig struct {
	// extract from pulsar.ConsumerOptions
	Concurrency                 *ConcurrencyPolicy                 `json:"concurrency"`                   // Optional: 并发控制
	Topics                      []string                           `json:"topics"`                        // Alternative with Topic: 如果有值, Topic 配置将被忽略; 第一个为核心主题
	Topic                       string                             `json:"topic"`                         // Alternative with Topics: Topics缺失的情况下，该值生效
	SubscriptionName            string                             `json:"subscription_name"`             //
	Level                       internal.TopicLevel                `json:"level"`                         // Optional:
	Type                        pulsar.SubscriptionType            `json:"type"`                          //
	SubscriptionInitialPosition pulsar.SubscriptionInitialPosition `json:"subscription_initial_position"` //
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
	Levels               topic.Levels             // Required: 默认L1, 且消费的Topic Level级别, len(Topics) == 1 or Topic存在的时候才生效
	LevelBalanceStrategy internal.BalanceStrategy // Optional: Topic级别消费策略
	LevelPolicies        LevelPolicies            // Optional: 级别消费策略
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
	ConsumeWeight     uint                // 消费权重
	ConsumeMaxTimes   int                 // 最大消费次数
	BackoffDelays     []string            // 补偿延迟 e.g: [5s, 2m, 1h], 如果值大于 ReentrantDelay 时，自动取整为 ReentrantDelay 的整数倍 (默认向下取整)
	BackoffPolicy     StatusBackoffPolicy // 补偿策略, 优先级高于 BackoffDelays
	ReentrantDelay    uint                // 重入 补偿延迟, 单状态固定时间
	ReentrantMaxTimes int                 // 重入 补偿延迟最大次数
	CheckerMandatory  bool                // Checker 存在性检查标识
	//ConnectInSyncEnable bool                // Optional: 是否同步建立连接, 首次发送消息需阻塞等待客户端与服务端连接完成
	//NackMaxDelay      int      // Nack 补偿策略最大延迟粒度
	//NackMaxTimes      int      // Nack 补偿延迟最大次数
	//BackoffPolicy     StatusBackoffPolicy // 补偿策略, 优先级高于 BackoffDelays
}

type LevelPolicy struct {
	ConsumeWeight uint                // 消费权重
	UpgradeLevel  internal.TopicLevel // 升级级别
	DegradeLevel  internal.TopicLevel // 降级级别
}

type RoutePolicy struct {
	ConnectInSyncEnable bool // Optional: 是否同步建立连接, 首次发送消息需阻塞等待客户端与服务端连接完成
}

type ReroutePolicy struct {
	ConnectInSyncEnable bool // Optional: 是否同步建立连接, 首次发送消息需阻塞等待客户端与服务端连接完成
}

/*type ReroutePolicy struct {
	//ReRouteMode      ReRouteMode       // 重路由模式: local; config
	UidPreRouters    map[uint32]string // Uid级别打散路由静态配置; 优先级低于Bucket级别路由;
	UidParseFunc     func(message pulsar.Message) uint
	BucketPreRouters map[string]string // Bucket级别打散路由静态配置; 优先级高于Uid级别路由;
	BucketParseFunc  func(message pulsar.Message) string
}*/

// DLQPolicy represents the configuration for the Dead Letter Queue multiStatusConsumeFacade policy.
type DLQPolicy struct {
	// MaxDeliveries specifies the maximum number of times that a message will be delivered before being
	// sent to the dead letter queue.
	MaxDeliveries uint32

	// DeadLetterTopic specifies the name of the topic where the failing messages will be sent.
	DeadLetterTopic string

	// RetryLetterTopic specifies the name of the topic where the retry messages will be sent.
	RetryLetterTopic string
}

type ConcurrencyPolicy struct {
	CorePoolSize    uint // Optional: default 1
	MaximumPoolSize uint // Optional: default 1
	KeepAliveTime   uint // Optional: default 1 min

	PanicHandler func(interface{}) // Optional, handle panics comes from executing message handler
}
