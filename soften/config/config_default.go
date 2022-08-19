package config

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
	defaultMainBackoffDelays = []string{"3s", "5s", "8s", "14s", "30s", "60s"}
	// Retrying 重试间隔策略策略: 累计 20min, 超过9次 每次按600s记
	defaultRetryingBackoffDelays = []string{"3s", "5s", "8s", "14s", "30s", "60s", "180s", "300s", "600s"}
	// Retrying 重试间隔策略策略: 同 defaultRetryingBackoffDelays
	defaultPendingBackoffDelays = defaultRetryingBackoffDelays
	// Retrying 重试间隔策略策略: 累计 4h, 超过5次 每次按2h记
	defaultBlockingBackoffDelays = []string{"600s", "1200s", "1800s", "3600s", "7200s"}
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
		ConsumeMaxTimes:   5 + 30,                       // 最多消费35次(5次Nack+30次重入), 21分钟内重试
		BackoffDelays:     defaultRetryingBackoffDelays, // 前5次累计1分钟, 第6次开始每隔1分钟开始重试
		BackoffPolicy:     nil,                          //
		ReentrantDelay:    60,                           // 每1分钟进行一次重入
		ReentrantMaxTimes: 30,                           // 最多重入30次
	}

	// defaultStatusPolicyPending 默认Pending状态的校验策略。
	defaultStatusPolicyPending = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightPending,
		ConsumeMaxTimes:   5 + 30,                      // 最多消费35次(5次Nack+30次重入), 31分钟内重试
		BackoffDelays:     defaultPendingBackoffDelays, // 前5次累计1分钟, 第6次开始每隔1分钟开始重试
		BackoffPolicy:     nil,                         //
		ReentrantDelay:    60,                          // 每1分钟进行一次重入
		ReentrantMaxTimes: 30,                          // 最多重入30次
	}

	// defaultStatusPolicyBlocking 默认pending状态的校验策略。
	defaultStatusPolicyBlocking = &StatusPolicy{
		ConsumeWeight:     defaultConsumeWeightBlocking,
		ConsumeMaxTimes:   15,                           // 最多消费15次, 1d内重试
		BackoffDelays:     defaultBlockingBackoffDelays, // 前4次累计2h, 第5次开始每隔2h开始重试
		BackoffPolicy:     nil,                          //
		ReentrantDelay:    600,                          // 每10min进行一次重入
		ReentrantMaxTimes: 144,                          // 最多重入144次 (1天=144*10min)
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
		CorePoolSize:    16,
		MaximumPoolSize: 16,
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
