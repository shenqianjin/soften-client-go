package config

import (
	"fmt"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/internal/backoff"
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
	DefaultConsumeMaxTimes   = uint(30)  // 一个消息整个生命周期中的消费次数上限
	DefaultReentrantMaxTimes = uint(30)  // 一个消息整个生命周期中的重入次数上限
	DefaultPublishMaxTimes   = uint(30)  // 一个消息发布次数上限
	DefaultNackMaxDelay      = uint(300) // 最大Nack延迟，默认5分钟

	// Retrying 补偿重试间隔策略: 前6次60秒, 超过每次间隔60s
	// expected: []string{"3s", "5s", "8s", "14s", "30s", "60s", "180s", "300s", "600s"}
	defaultRetryingBackoffDelays = []string{"3s", "5s", "8s", "14s", "30s", "60s"}
	// Pending 重试间隔策略策略: 同 defaultRetryingBackoffDelays
	defaultPendingBackoffDelays = defaultRetryingBackoffDelays
	// Blocking 重试间隔策略策略: 累计 4h, 超过5次 每次按2h记
	// expected: []string{"600s", "1200s", "1800s", "3600s", "7200s"}
	defaultBlockingBackoffDelays = []string{"600s"}
	// Publish 补偿重试间隔策略: 前7次60秒, 超过每次间隔60s
	defaultPublishBackoffDelays = []string{"1s", "2s", "3s", "5s", "8s", "11s", "30s", "60s"}
)

// ------ default consume status policies ------

var (
	// defaultStatusReadyPolicy 默认pending状态的校验策略。
	defaultStatusReadyPolicy = &ReadyPolicy{
		ConsumeWeight: ToPointer(defaultConsumeWeightMain),
	}

	// defaultStatusPolicyRetrying 默认Retrying状态的校验策略。
	defaultStatusPolicyRetrying = &StatusPolicy{
		ConsumeWeight:      ToPointer(defaultConsumeWeightRetrying),
		ConsumeMaxTimes:    ToPointer(uint(60 * 6)),      // 最多尝试60*6=360次, 6h小时内每分钟重试
		BackoffDelays:      defaultRetryingBackoffDelays, // 前5次累计1分钟, 第6次开始每隔1分钟开始重试
		BackoffDelayPolicy: nil,                          //
		ReentrantDelay:     ToPointer(uint(60)),          // 每1分钟进行一次重入
		ReentrantMaxTimes:  ToPointer(uint(0)),           // 最大重入次数不限制
		Publish:            newDefaultPublishPolicy(),
	}

	// defaultStatusPolicyPending 默认Pending状态的校验策略。
	defaultStatusPolicyPending = &StatusPolicy{
		ConsumeWeight:      ToPointer(defaultConsumeWeightPending),
		ConsumeMaxTimes:    ToPointer(uint(0)),          // pending 默认不限制次数
		BackoffDelays:      defaultPendingBackoffDelays, // 前5次累计1分钟, 第6次开始每隔1分钟开始重试
		BackoffDelayPolicy: nil,                         //
		ReentrantDelay:     ToPointer(uint(60)),         // 每1分钟进行一次重入
		ReentrantMaxTimes:  ToPointer(uint(0)),          // 最多重入30次
		Publish:            newDefaultPublishPolicy(),
	}

	// defaultStatusPolicyBlocking 默认pending状态的校验策略。
	defaultStatusPolicyBlocking = &StatusPolicy{
		ConsumeWeight:      ToPointer(defaultConsumeWeightBlocking),
		ConsumeMaxTimes:    ToPointer(uint(6 * 24 * 2)),  // 最多消费6 * 24 * 2=288次, 1h*24*2=2d内重试
		BackoffDelays:      defaultBlockingBackoffDelays, // 前4次累计2h, 第5次开始每隔2h开始重试
		BackoffDelayPolicy: nil,                          //
		ReentrantDelay:     ToPointer(uint(600)),         // 每10min进行一次重入
		ReentrantMaxTimes:  ToPointer(uint(144)),         // 最多重入144次 (1天=144*10min)
		Publish:            newDefaultPublishPolicy(),
	}

	// defaultDeadPolicy default dead to D1
	defaultDeadPolicy = &DeadPolicy{
		Publish: newDefaultPublishPolicy(),
	}

	// defaultUpgradePolicy
	defaultUpgradePolicy = &ShiftPolicy{
		Publish: newDefaultPublishPolicy(),
	}

	// defaultDegradePolicy
	defaultDegradePolicy = &ShiftPolicy{
		Publish: newDefaultPublishPolicy(),
	}

	// defaultShiftPolicy
	defaultShiftPolicy = &ShiftPolicy{
		Publish: newDefaultPublishPolicy(),
	}

	// defaultDeadPolicy default dead to D1
	defaultShiftDeadPolicy = &ShiftPolicy{
		Level:   message.D1,
		Publish: newDefaultPublishPolicy(),
	}

	// defaultTransferPolicy
	defaultTransferPolicy = &TransferPolicy{
		Publish: newDefaultPublishPolicy(),
	}
)

func newDefaultBackoffPolicy() *BackoffPolicy {
	backoffPolicy, err := backoff.NewAbbrBackoffDelayPolicy(defaultPublishBackoffDelays)
	if err != nil {
		panic(err)
	}
	return &BackoffPolicy{
		Delays:      defaultPublishBackoffDelays,
		MaxTimes:    ToPointer(DefaultPublishMaxTimes), // 默认30次,前7次60s,累计24分钟
		DelayPolicy: backoffPolicy,
	}
}

func newDefaultPublishPolicy() *PublishPolicy {
	publishPolicy := &PublishPolicy{
		Backoff: newDefaultBackoffPolicy(),
	}
	return publishPolicy
}

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

// ------ default metrics policy ------

func newDefaultMetricsPolicy() *MetricsPolicy {
	return &MetricsPolicy{
		MetricsTopicMode: MetricsTopicGeneral,
		MetricsBuckets: &internal.MetricsBuckets{
			ProduceCheckLatencies:  defaultProduceCheckLatencies,
			ProduceEventLatencies:  defaultProduceEventLatencies,
			ConsumeListenLatencies: defaultConsumeListenLatencies,
			ConsumeCheckLatencies:  defaultConsumeCheckLatencies,
			ConsumeHandleLatencies: defaultConsumeHandleLatencies,
			ConsumeEventLatencies:  defaultConsumeEventLatencies,
			ConsumeRoundLatencies:  defaultConsumeRoundLatencies,
			MessageConsumeTimes:    defaultMessageConsumeTimes,
		},
	}
}

var (
	// defaultProduceCheckLatencies 生产检查默认延迟桶
	defaultProduceCheckLatencies = []float64{
		.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
		60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
		600,
	}

	// defaultProduceEventLatencies 生产完成默认事件处理延迟桶
	defaultProduceEventLatencies = []float64{
		.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
		60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
		600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
		3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600,
	}

	// defaultConsumeListenLatencies 消费监听(等待)默认延迟桶
	defaultConsumeListenLatencies = []float64{
		.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
		60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
		600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
		3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
		24 * 3600, 2 * 24 * 3600,
	}

	// defaultConsumeCheckLatencies 消费检查默认延迟桶
	defaultConsumeCheckLatencies = defaultProduceCheckLatencies

	// defaultConsumeListenLatencies 消费消息处理默认延迟桶
	defaultConsumeHandleLatencies = []float64{
		.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
		60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
		600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
		3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
		24 * 3600, 2 * 24 * 3600,
	}

	// defaultConsumeListenLatencies 消费完成默认事件处理延迟桶
	defaultConsumeEventLatencies = []float64{
		.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
		60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
		600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
		3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
		24 * 3600, 2 * 24 * 3600, 3 * 24 * 3600, 4 * 24 * 3600, 5 * 24 * 3600, 6 * 24 * 3600, 7 * 24 * 3600, 15 * 24 * 3600, 30 * 24 * 3600,
	}

	// defaultConsumeRoundLatencies  消费每轮处理默认延迟桶
	defaultConsumeRoundLatencies = []float64{
		.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 40, 50,
		60, 1.5 * 60, 2 * 60, 3 * 60, 4 * 60, 5 * 60, 6 * 60, 7 * 60, 8 * 60, 9 * 60,
		600, 1.5 * 600, 2 * 600, 3 * 600, 4 * 600, 5 * 600,
		3600, 1.5 * 3600, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600, 7 * 3600, 8 * 3600, 9 * 3600, 10 * 3600, 11 * 3600, 12 * 3600,
		24 * 3600, 2 * 24 * 3600,
	}

	// defaultMessageConsumeTimes 消费消息处理次数桶
	defaultMessageConsumeTimes = []float64{
		1, 2, 3, 5, 8, 10, 13, 17, 20, 30, 40, 50,
		60, 80, 100, 150, 200, 250, 300, 350, 400, 500, 600, 700, 800, 900,
		1000, 1500, 2000, 3000, 4000, 5000,
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
