package config

import (
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// ------ logger ------

var Logger log.Logger

// ------ default weights ------

const (
	defaultConsumeWeightMain     = uint(10) // 业务Main  队列: 50% 权重
	defaultConsumeWeightRetrying = uint(6)  // Retrying 队列: 30% 权重
	defaultConsumeWeightPending  = uint(3)  // Pending  队列: 15% 权重
	defaultConsumeWeightBlocking = uint(1)  // Blocking 队列:  5% 权重

	defaultLeveledConsumeWeightMain = uint(10)
)

// ------ default others ------

var (
	defaultConsumeConcurrency  = 16                                              // 默认消费并发数
	defaultConsumeMaxTimes     = 30                                              // 一个消息整个生命周期中的最大消费次数
	defaultStatusBackoffDelays = []string{"3s", "5s", "8s", "14s", "30s", "60s"} // 消息默认Nack重试间隔策略策略
	//DefaultNackMaxDelay        = 300                                             // 最大Nack延迟，默认5分钟
)

// ------ default consume status policies ------

var (
	// defaultStatusPolicyReady 默认pending状态的校验策略。CheckToTopic default ${TOPIC}, 固定后缀，不允许定制;
	// 默认开启; 默认权重 10; CheckerMandatory 默认false;
	// BackoffPolicy: [3s 5s 8s 14s 30s 60s]; NackBackoffMaxTimes: 6, 累积120s间隔;
	// ReentrantDelay: 0; ReentrantMaxTimes: 0, 累积延迟 0*(0s+120s) + 120s = 2min 内处理不成功到Retrying 或 Dead 或 Discard。
	defaultStatusPolicyReady = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightMain,
		ConsumeMaxTimes:   defaultConsumeMaxTimes,
		BackoffDelays:     defaultStatusBackoffDelays,
		BackoffPolicy:     nil,
		ReentrantDelay:    0, // 不需要
		ReentrantMaxTimes: 0, // 不需要
	}

	// defaultStatusPolicyRetrying 默认pending状态的校验策略。CheckToTopic default ${TOPIC}_PENDING, 固定后缀，不允许定制;
	// 默认开启; 默认权重 10; CheckerMandatory 默认false;
	// BackoffPolicy: [3s 5s 8s 14s 30s 60s]; NackBackoffMaxTimes: 6, 累积120s间隔;
	// ReentrantDelay: 60s; ReentrantMaxTimes: 15, 累积延迟 15 * (60s+60s) = 30min 内处理不成功到DLQ 或 Discard。
	defaultStatusPolicyRetrying = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightRetrying,
		ConsumeMaxTimes:   defaultConsumeMaxTimes,
		BackoffDelays:     defaultStatusBackoffDelays,
		BackoffPolicy:     nil,
		ReentrantDelay:    120,
		ReentrantMaxTimes: 15,
	}

	// defaultStatusPolicyPending 同 defaultStatusPolicyRetrying。
	defaultStatusPolicyPending = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightPending,
		ConsumeMaxTimes:   defaultConsumeMaxTimes,
		BackoffDelays:     defaultStatusBackoffDelays,
		BackoffPolicy:     nil,
		ReentrantDelay:    20,
		ReentrantMaxTimes: 150,
	}

	// defaultStatusPolicyBlocking 默认pending状态的校验策略。CheckToTopic default ${TOPIC}_PENDING, 固定后缀，不允许定制;
	// 默认开启; 默认权重 10; CheckerMandatory 默认false;
	// BackoffPolicy: []; NackBackoffMaxTimes: 0, 累积0s间隔;
	// ReentrantDelay: 1800s; ReentrantMaxTimes: 12, 累积延迟 12 * (1800s+0s) = 6h 内处理不成功到DLQ 或 Discard。
	defaultStatusPolicyBlocking = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightBlocking,
		ConsumeMaxTimes:   defaultConsumeMaxTimes,
		BackoffDelays:     nil,
		BackoffPolicy:     nil,
		ReentrantDelay:    1800,
		ReentrantMaxTimes: 12,
	}
)

// ------ default consume leveled policies ------

var (
	defaultLeveledPolicy = &LevelPolicy{
		ConsumeWeight: defaultLeveledConsumeWeightMain,
	}
)

// ------ default consume concurrency policy ------

var (
	defaultConcurrencyPolicy = &ConcurrencyPolicy{
		CorePoolSize:    50,
		MaximumPoolSize: 50,
		KeepAliveTime:   60,
	}
)

// ------ default route policy ------
var (
	defaultRoutePolicy = &RoutePolicy{
		ConnectInSyncEnable: false,
	}
)

// ------ default reroute policy ------
var (
	defaultReroutePolicy = &ReroutePolicy{
		ConnectInSyncEnable: false,
	}
)
