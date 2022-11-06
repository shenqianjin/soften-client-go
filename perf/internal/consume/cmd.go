package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/choice"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/cost"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/limiter"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/stats"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/time/rate"
)

// ConsumeArgs define the parameters required by PerfConsume
type ConsumeArgs struct {
	Topic             string
	SubscriptionName  string
	ReceiverQueueSize int

	costAverageInMs    int64   // 处理消息业务时长
	costPositiveJitter float64 // 处理消息快波动率
	costNegativeJitter float64 // 处理消息慢波动率

	Concurrency        uint
	ConsumeQuota       string
	ConsumeRate        string
	ConsumeConcurrency string
	GotoWeight         string // 处理决策权重, 固定顺序: [done, discard, dead, pending, blocking, retrying, upgrade, degrade]
}

func LoadConsumeFlags(flags *pflag.FlagSet, cArgs *ConsumeArgs) {
	flags.StringVarP(&cArgs.SubscriptionName, "subscription", "S", "sub", "Subscription name")
	flags.IntVar(&cArgs.ReceiverQueueSize, "receive-queue-size", 1000, "Receiver queue size")

	flags.Int64Var(&cArgs.costAverageInMs, "cost-average-in-ms", 100, "mocked average consume cost in milliseconds, use -1 to disable")
	flags.Float64Var(&cArgs.costPositiveJitter, "cost-positive-jitter", 0, "cost positive jitter")
	flags.Float64Var(&cArgs.costNegativeJitter, "cost-negative-jitter", 0, "cost negative jitter")

	flags.UintVarP(&cArgs.Concurrency, "concurrency", "C", 50, "consume concurrency")
	flags.StringVar(&cArgs.ConsumeQuota, "consume-quota", "0,0", "consume quota for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	flags.StringVarP(&cArgs.ConsumeRate, "consume-rate", "R", "50,50", "consume qps for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	flags.StringVar(&cArgs.ConsumeConcurrency, "consume-concurrency", "50,50", "handle concurrences for different types")
	//flags.StringVar(&cArgs.GotoWeight, "goto-weight", "100,5,5,30,10,60,0,0,0",
	flags.StringVar(&cArgs.GotoWeight, "goto-weight", "100,0,0,0,0,0,0,0",
		"mocked goto weights for consume handling. fixed order and number as: [done, discard, dead, pending, blocking, retrying, upgrade, degrade, shift]")
	if cArgs.costAverageInMs < 0 {
		cArgs.costAverageInMs = 0
	}
	if cArgs.costNegativeJitter > 1 {
		cArgs.costNegativeJitter = 1
	}
}

func NewConsumerCommand(rtArgs *internal.RootArgs) *cobra.Command {
	cArgs := &ConsumeArgs{}
	cmd := &cobra.Command{
		Use:   "consume <topic>",
		Short: "Consume from topic",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cArgs.Topic = args[0]
			PerfConsume(rtArgs.Ctx, rtArgs, cArgs)
		},
	}
	// load flags here
	LoadConsumeFlags(cmd.Flags(), cArgs)

	return cmd
}

var handleGotoIndexes = map[string]int{
	decider.GotoDone.String():     0,
	decider.GotoDiscard.String():  1,
	decider.GotoDead.String():     2,
	decider.GotoPending.String():  3,
	decider.GotoBlocking.String(): 4,
	decider.GotoRetrying.String(): 5,
	decider.GotoUpgrade.String():  6,
	decider.GotoDegrade.String():  7,
	decider.GotoShift.String():    8,
}

func PerfConsume(ctx context.Context, rtArgs *internal.RootArgs, cmdArgs *ConsumeArgs) {
	b, _ := json.MarshalIndent(rtArgs.ClientArgs, "", "  ")
	logrus.Info("Client config: ", string(b))
	b, _ = json.MarshalIndent(cmdArgs, "", "  ")
	logrus.Info("Consumer config: ", string(b))

	svc := newConsumer(rtArgs, cmdArgs)

	// create client
	client, err := util.NewClient(&rtArgs.ClientArgs)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// create consumeService
	lvlPolicy := &config.LevelPolicy{
		PendingEnable:  config.True(),
		BlockingEnable: config.True(),
		RetryingEnable: config.True(),
	}
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:            cmdArgs.Topic,
		SubscriptionName: cmdArgs.SubscriptionName,
		Concurrency:      &config.ConcurrencyPolicy{CorePoolSize: cmdArgs.Concurrency},
		LevelPolicy:      lvlPolicy,
	},
		//checker.PrevHandlePending(concurrencyCheck),
		//checker.PrevHandleBlocking(quotaCheck),
		checker.PrevHandleRetrying(svc.rateCheck))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// start monitoring: async
	go stats.ConsumeStats(ctx, svc.consumeStatCh, len(svc.ConsumeConcurrences))

	// start message listener
	err = listener.StartPremium(context.Background(), svc.internalHandle)
	if err != nil {
		log.Fatal(err)
	}

	//
	<-ctx.Done()
}

// ------ consume service ------

type consumeService struct {
	consumerArgs *ConsumeArgs

	handleGotoChoice choice.GotoPolicy
	costPolicy       cost.CostPolicy

	ConsumeQuotas       []uint64
	ConsumeRates        []uint64
	ConsumeConcurrences []uint64
	GotoWeights         []uint64

	consumeStatCh       chan *stats.ConsumeStatEntry
	concurrencyLimiters map[string]limiter.CountLimiter // 超并发限制时,去pending队列
	rateLimiters        map[string]*rate.Limiter        // 超额度限制时,去blocking对列
	quotaLimiters       map[string]limiter.CountLimiter // 超额度限制时,去blocking对列
}

func newConsumer(rtArgs *internal.RootArgs, cmdArgs *ConsumeArgs) *consumeService {
	gotoWeights := util.ParseUint64Array(cmdArgs.GotoWeight)
	gotoWeightMap := map[string]uint64{
		decider.GotoDone.String(): gotoWeights[handleGotoIndexes[decider.GotoDone.String()]],
	}
	if weight := gotoWeights[handleGotoIndexes[decider.GotoRetrying.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoRetrying.String()] = weight
	}
	if weight := gotoWeights[handleGotoIndexes[decider.GotoPending.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoPending.String()] = weight
	}
	if weight := gotoWeights[handleGotoIndexes[decider.GotoBlocking.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoBlocking.String()] = weight
	}
	if weight := gotoWeights[handleGotoIndexes[decider.GotoDead.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoDead.String()] = weight
	}
	if weight := gotoWeights[handleGotoIndexes[decider.GotoDiscard.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoDiscard.String()] = weight
	}
	handleGotoChoice := choice.NewRoundRandWeightGotoPolicy(gotoWeightMap)

	// Retry to create producer indefinitely
	c := &consumeService{
		consumerArgs:     cmdArgs,
		handleGotoChoice: handleGotoChoice,
		consumeStatCh:    make(chan *stats.ConsumeStatEntry),

		ConsumeQuotas:       util.ParseUint64Array(cmdArgs.ConsumeQuota),
		ConsumeRates:        util.ParseUint64Array(cmdArgs.ConsumeRate),
		ConsumeConcurrences: util.ParseUint64Array(cmdArgs.ConsumeConcurrency),

		concurrencyLimiters: make(map[string]limiter.CountLimiter), // 超并发限制时,去pending队列
		rateLimiters:        make(map[string]*rate.Limiter),        // 超额度限制时,去blocking对列
		quotaLimiters:       make(map[string]limiter.CountLimiter), // 超额度限制时,去blocking对列
	}
	// initialize cost policy
	if cmdArgs.costAverageInMs > 0 {
		c.costPolicy = cost.NewAvgCostPolicy(cmdArgs.costAverageInMs, cmdArgs.costPositiveJitter, cmdArgs.costNegativeJitter)
	}
	// handle message type-level concurrency limiters
	if len(c.ConsumeConcurrences) > 0 {
		for index, con := range c.ConsumeConcurrences {
			if con > 0 {
				c.concurrencyLimiters[fmt.Sprintf("Type-%d", index+1)] = limiter.NewCountLimiter(int(con)) // rate.New(int(li), time.Second)
			} else {
				c.concurrencyLimiters[fmt.Sprintf("Type-%d", index+1)] = nil
			}
		}
	}
	// handle message type-level qps limiters
	if len(c.ConsumeRates) > 0 {
		for index, r := range c.ConsumeRates {
			if r > 0 {
				c.rateLimiters[fmt.Sprintf("Type-%d", index+1)] = rate.NewLimiter(rate.Limit(r), int(r)) // rate.New(int(li), time.Second)
			} else {
				c.rateLimiters[fmt.Sprintf("Type-%d", index+1)] = nil
			}
		}

	}
	// handle message type-level quota limiters
	if len(c.ConsumeRates) > 0 {
		for index, quota := range c.ConsumeQuotas {
			if quota > 0 {
				c.quotaLimiters[fmt.Sprintf("Type-%d", index+1)] = limiter.NewCountLimiter(int(quota)) // rate.New(int(li), time.Second)
			} else {
				c.quotaLimiters[fmt.Sprintf("Type-%d", index+1)] = nil
			}
		}

	}

	return c
}

// 各类型超Quota限制时,去blocking队列
func (svc *consumeService) quotaCheck(msg pulsar.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Type"]; ok {
		if limiter, ok2 := svc.quotaLimiters[typeName]; ok2 && limiter != nil {
			if !limiter.TryAcquire() { // non-blocking operation
				return checker.CheckStatusPassed
			} else {
				return checker.CheckStatusRejected.WithHandledDefer(func() {
					limiter.Release()
				})
			}
		}
	}
	return checker.CheckStatusRejected
}

// 各类型超Concurrency限制时,去blocking队列
func (svc *consumeService) concurrencyCheck(msg pulsar.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Type"]; ok {
		if limiter, ok2 := svc.concurrencyLimiters[typeName]; ok2 && limiter != nil {
			if !limiter.TryAcquire() { // non-blocking operation
				return checker.CheckStatusPassed
			} else {
				return checker.CheckStatusRejected.WithHandledDefer(func() {
					limiter.Release()
				})
			}
		}
	}
	return checker.CheckStatusRejected
}

// 各类型超QPS限制时,去retrying队列
func (svc *consumeService) rateCheck(ctx context.Context, msg message.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Type"]; ok {
		if limiter, ok2 := svc.rateLimiters[typeName]; ok2 && limiter != nil {
			if !limiter.Allow() { // non-blocking operation
				return checker.CheckStatusPassed
			}
		}
	}
	return checker.CheckStatusRejected
}

var RFC3339TimeInSecondPattern = "20060102150405.999"

func (svc *consumeService) internalHandle(ctx context.Context, msg message.Message) handler.HandleStatus {
	originPublishTime := msg.PublishTime()
	if originalPublishTime, ok := msg.Properties()[message.XPropertyOriginPublishTime]; ok {
		if parsedTime, err := time.Parse(RFC3339TimeInSecondPattern, originalPublishTime); err == nil {
			originPublishTime = parsedTime
		}
	}
	start := time.Now()
	stat := &stats.ConsumeStatEntry{
		Bytes:           int64(len(msg.Payload())),
		ReceivedLatency: time.Since(msg.PublishTime()).Seconds(),
	}

	if svc.consumerArgs.costAverageInMs > 0 {
		time.Sleep(svc.costPolicy.Next()) // 模拟业务处理
	}
	var result handler.HandleStatus
	n := svc.handleGotoChoice.Next()
	switch n {
	case string(decider.GotoRetrying):
		result = handler.StatusRetrying
	case string(decider.GotoPending):
		result = handler.StatusPending
	case string(decider.GotoBlocking):
		result = handler.StatusBlocking
	case string(decider.GotoDead):
		result = handler.StatusDead
	case string(decider.GotoDiscard):
		result = handler.StatusDiscard
	default:
		result = handler.StatusDone
		stat.FinishedLatency = time.Since(originPublishTime).Seconds() // 从消息产生到处理完成的时间(中间状态不是完成状态)
		if radicalKey, ok := msg.Properties()["Type"]; ok {
			stat.TypeName = radicalKey
			//stat.radicalFinishedLatencies[typeName] = stat.finishedLatency
		}
	}

	stat.HandledLatency = time.Since(start).Seconds()
	if radicalKey, ok := msg.Properties()["Type"]; ok {
		stat.TypeName = radicalKey
		//stat.radicalHandledLatencies[typeName] = stat.handledLatency
	}
	svc.consumeStatCh <- stat
	return result
}
