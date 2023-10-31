package consume

import (
	"context"
	"fmt"
	"time"

	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/choice"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/concurrencylimit"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/cost"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/stats"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
)

// ------ consume service ------

type consumeService struct {
	consumerArgs *ConsumeArgs

	consumeStatCh chan *stats.ConsumeStatEntry
	costPolicy    cost.CostPolicy

	quotaLimits         []uint64
	quotaLimiters       map[string]concurrencylimit.Limiter // 超额度限制时,去blocking对列
	rateLimits          []uint64
	rateLimiters        map[string]*rate.Limiter // 超额度限制时,去blocking对列
	concurrencyLimits   []uint64
	concurrencyLimiters map[string]concurrencylimit.Limiter // 超并发限制时,去pending队列

	gotoWeights      map[string]uint64
	handleGotoChoice choice.GotoPolicy
}

func newConsumer(rtArgs *internal.RootArgs, cmdArgs *ConsumeArgs) *consumeService {
	// Retry to create producer indefinitely
	c := &consumeService{
		consumerArgs:  cmdArgs,
		consumeStatCh: make(chan *stats.ConsumeStatEntry),

		quotaLimits:       util.ParseUint64Array(cmdArgs.ConsumeQuotaLimit),
		rateLimits:        util.ParseUint64Array(cmdArgs.ConsumeRateLimit),
		concurrencyLimits: util.ParseUint64Array(cmdArgs.ConsumeConcurrencyLimit),
		gotoWeights:       util.ParseGotoWeightMap(cmdArgs.ConsumeGoto),

		concurrencyLimiters: make(map[string]concurrencylimit.Limiter), // 超并发限制时,去pending队列
		rateLimiters:        make(map[string]*rate.Limiter),            // 超额度限制时,去blocking对列
		quotaLimiters:       make(map[string]concurrencylimit.Limiter), // 超额度限制时,去blocking对列
	}
	handleGotoChoice := choice.NewRoundRandWeightGotoPolicy(c.gotoWeights)
	c.handleGotoChoice = handleGotoChoice

	// initialize cost policy
	if cmdArgs.handleCostAvgInMs > 0 {
		c.costPolicy = cost.NewAvgCostPolicy(cmdArgs.handleCostAvgInMs, cmdArgs.handleCostPositiveJitter, cmdArgs.handleCostNegativeJitter)
	}
	// handle message type-level concurrency limiters
	if len(c.concurrencyLimits) > 0 {
		for index, con := range c.concurrencyLimits {
			if con > 0 {
				c.concurrencyLimiters[fmt.Sprintf("Group-%d", index+1)] = concurrencylimit.New(int(con)) // rate.New(int(li), time.Second)
			} else {
				c.concurrencyLimiters[fmt.Sprintf("Group-%d", index+1)] = nil
			}
		}
	}
	// handle message type-level qps limiters
	if len(c.rateLimits) > 0 {
		for index, r := range c.rateLimits {
			if r > 0 {
				c.rateLimiters[fmt.Sprintf("Group-%d", index+1)] = rate.NewLimiter(rate.Limit(r), int(r)) // rate.New(int(li), time.Second)
			} else {
				c.rateLimiters[fmt.Sprintf("Group-%d", index+1)] = nil
			}
		}
	}
	// handle message type-level quota limiters
	if len(c.quotaLimits) > 0 {
		for index, quota := range c.quotaLimits {
			if quota > 0 {
				c.quotaLimiters[fmt.Sprintf("Group-%d", index+1)] = concurrencylimit.New(int(quota)) // rate.New(int(li), time.Second)
			} else {
				c.quotaLimiters[fmt.Sprintf("Group-%d", index+1)] = nil
			}
		}
	}

	return c
}

func (svc *consumeService) collectEnables() enables {
	e := enables{ReadyEnable: true}
	if svc.gotoWeights[handler.StatusDiscard.GetGoto().String()] > 0 {
		e.DiscardEnable = true
	}
	if svc.gotoWeights[handler.StatusDead.GetGoto().String()] > 0 {
		e.DeadEnable = true
	}
	if svc.gotoWeights[handler.StatusPending.GetGoto().String()] > 0 ||
		slices.IndexFunc(svc.rateLimits, func(e uint64) bool { return e > 0 }) >= 0 ||
		slices.IndexFunc(svc.concurrencyLimits, func(e uint64) bool { return e > 0 }) >= 0 {
		e.PendingEnable = true
	}

	if svc.gotoWeights[handler.StatusBlocking.GetGoto().String()] > 0 ||
		slices.IndexFunc(svc.quotaLimits, func(e uint64) bool { return e > 0 }) >= 0 {
		e.BlockingEnable = true
	}
	if svc.gotoWeights[handler.StatusRetrying.GetGoto().String()] > 0 {
		e.RetryingEnable = true
	}
	if svc.gotoWeights[handler.StatusUpgrade.GetGoto().String()] > 0 {
		e.UpgradeEnable = true
	}
	if svc.gotoWeights[handler.StatusDegrade.GetGoto().String()] > 0 {
		e.DegradeEnable = true
	}
	if svc.gotoWeights[handler.StatusShift.GetGoto().String()] > 0 {
		e.ShiftEnable = true
	}
	if svc.gotoWeights[handler.StatusTransfer.GetGoto().String()] > 0 {
		e.TransferEnable = true
	}
	return e
}

// 各类型超Quota限制时,去blocking队列
func (svc *consumeService) exceedQuota(ctx context.Context, msg message.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Group"]; ok {
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
func (svc *consumeService) exceedConcurrency(ctx context.Context, msg message.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Group"]; ok {
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
func (svc *consumeService) exceedRate(ctx context.Context, msg message.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Group"]; ok {
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

	if svc.consumerArgs.handleCostAvgInMs > 0 {
		time.Sleep(svc.costPolicy.Next()) // 模拟业务处理
	}
	var result handler.HandleStatus
	n := svc.handleGotoChoice.Next()
	switch n {
	case handler.StatusRetrying.GetGoto().String():
		result = handler.StatusRetrying
	case handler.StatusPending.GetGoto().String():
		result = handler.StatusPending
	case handler.StatusBlocking.GetGoto().String():
		result = handler.StatusBlocking
	case handler.StatusDead.GetGoto().String():
		result = handler.StatusDead
	case handler.StatusDiscard.GetGoto().String():
		result = handler.StatusDiscard
	default:
		result = handler.StatusDone
		stat.FinishedLatency = time.Since(originPublishTime).Seconds() // 从消息产生到处理完成的时间(中间状态不是完成状态)
		if radicalKey, ok := msg.Properties()["Group"]; ok {
			stat.GroupName = radicalKey
			//stat.radicalFinishedLatencies[typeName] = stat.finishedLatency
		}
	}

	stat.HandledLatency = time.Since(start).Seconds()
	if radicalKey, ok := msg.Properties()["Group"]; ok {
		stat.GroupName = radicalKey
		//stat.radicalHandledLatencies[typeName] = stat.handledLatency
	}
	svc.consumeStatCh <- stat
	return result
}

// ------ helpers ------

type enables struct {
	ReadyEnable    bool
	DiscardEnable  bool
	DeadEnable     bool
	PendingEnable  bool
	BlockingEnable bool
	RetryingEnable bool
	UpgradeEnable  bool
	DegradeEnable  bool
	ShiftEnable    bool
	TransferEnable bool
}
