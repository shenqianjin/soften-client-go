package config

import (
	"fmt"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"strings"
)

// ------ default weights ------

const (
	defaultConsumeWeightMain     = uint(10) // Main     队列: 50% 权重
	defaultConsumeWeightRetrying = uint(6)  // Retrying 队列: 30% 权重
	defaultConsumeWeightPending  = uint(3)  // Pending  队列: 15% 权重
	defaultConsumeWeightBlocking = uint(1)  // Blocking 队列:  5% 权重

	defaultLeveledConsumeWeightMain = uint(10)
)

// ------ default others ------

var (
	DefaultConsumeMaxTimes   = 30  // 一个消息整个生命周期中的消费次数上限
	DefaultReentrantMaxTimes = 30  // 一个消息整个生命周期中的重入次数上限
	DefaultNackMaxDelay      = 300 // 最大Nack延迟，默认5分钟

	// Main 重试间隔策略策略: 累计 120s, 超过6次 每次按60s记
	// defaultMainBackoffDelays = []string{"3s", "5s", "8s", "14s", "30s", "60s"}
	// Retrying 重试间隔策略策略: 累计 20min, 超过9次 每次按600s记
	// expected: []string{"3s", "5s", "8s", "14s", "30s", "60s", "180s", "300s", "600s"}
	defaultRetryingBackoffDelays = []string{"3s", "5s", "8s", "14s", "30s", "60s"}
	// Pending 重试间隔策略策略: 同 defaultRetryingBackoffDelays
	defaultPendingBackoffDelays = defaultRetryingBackoffDelays
	// Retrying 重试间隔策略策略: 累计 4h, 超过5次 每次按2h记
	// expected: []string{"600s", "1200s", "1800s", "3600s", "7200s"}
	defaultBlockingBackoffDelays = []string{"600s"}
)

// ------ default consume status policies ------

var (
	// defaultStatusPolicyReady 默认pending状态的校验策略。
	defaultStatusPolicyReady = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightMain,
		ConsumeMaxTimes:   1,   // 消费 Ready 1次即可
		BackoffDelays:     nil, // 期望不重试
		BackoffPolicy:     nil, //
		ReentrantDelay:    0,   // 不需要
		ReentrantMaxTimes: 0,   // 不需要
	}

	// defaultStatusPolicyRetrying 默认Retrying状态的校验策略。
	defaultStatusPolicyRetrying = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightRetrying,
		ConsumeMaxTimes:   60 * 6,                       // 做多尝试60*6=360次, 6h小时内每分钟重试
		BackoffDelays:     defaultRetryingBackoffDelays, // 前5次累计1分钟, 第6次开始每隔1分钟开始重试
		BackoffPolicy:     nil,                          //
		ReentrantDelay:    60,                           // 每1分钟进行一次重入
		ReentrantMaxTimes: 0,                            // 最大重入次数不限制
	}

	// defaultStatusPolicyPending 默认Pending状态的校验策略。
	defaultStatusPolicyPending = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightPending,
		ConsumeMaxTimes:   0,                           // pending 默认不限制次数
		BackoffDelays:     defaultPendingBackoffDelays, // 前5次累计1分钟, 第6次开始每隔1分钟开始重试
		BackoffPolicy:     nil,                         //
		ReentrantDelay:    60,                          // 每1分钟进行一次重入
		ReentrantMaxTimes: 0,                           // 最多重入30次
	}

	// defaultStatusPolicyBlocking 默认pending状态的校验策略。
	defaultStatusPolicyBlocking = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightBlocking,
		ConsumeMaxTimes:   6 * 24 * 2,                   // 最多消费6 * 24 * 2=288次, 1h*24*2=2d内重试
		BackoffDelays:     defaultBlockingBackoffDelays, // 前4次累计2h, 第5次开始每隔2h开始重试
		BackoffPolicy:     nil,                          //
		ReentrantDelay:    600,                          // 每10min进行一次重入
		ReentrantMaxTimes: 144,                          // 最多重入144次 (1天=144*10min)
	}

	// defaultDeadPolicy default dead to D1
	defaultDeadPolicy = &ShiftPolicy{Level: message.D1}

	// defaultUpgradePolicy
	defaultUpgradePolicy = &ShiftPolicy{}

	// defaultDegradePolicy
	defaultDegradePolicy = &ShiftPolicy{}

	// defaultShiftPolicy
	defaultShiftPolicy = &ShiftPolicy{}
)

// ------ default consume leveled policies ------

var (
	defaultLeveledWeightFunc = func(lvl internal.TopicLevel) uint {
		if strings.HasPrefix(string(lvl), "S") {
			return defaultLeveledConsumeWeightMain * 2
		} else if strings.HasPrefix(string(lvl), "L") {
			return defaultLeveledConsumeWeightMain
		} else if strings.HasPrefix(string(lvl), "B") {
			return defaultLeveledConsumeWeightMain / 2
		} else if strings.HasPrefix(string(lvl), "D") {
			// Dead 队列默认不消费, 如果强制配置为消费, 默认权重同Ln默认权重
			return defaultLeveledConsumeWeightMain
		} else {
			panic(fmt.Sprintf("invalid topic level: %v", lvl))
		}
	}
)

// ------ default consume concurrency policy ------

var (
	defaultConcurrencyPolicy = &ConcurrencyPolicy{
		CorePoolSize:    16,
		MaximumPoolSize: 16,
		KeepAliveTime:   60,

		PanicHandler: func(i interface{}) {
			panic(i)
		}, // panic to exit main process. as default ants, exits worker goroutine but not main process
	}
)

// ------ default Transfer policy ------
var (
	defaultTransferPolicy = &TransferPolicy{
		ConnectInSyncEnable: false,
	}
)

// ------ helper ------

func ToPointer[T any](v T) *T {
	return &v
}

func True() *bool {
	return ToPointer(true)
}

func False() *bool {
	return ToPointer(false)
}
