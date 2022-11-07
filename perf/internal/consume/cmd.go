package consume

import (
	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/param"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// ConsumeArgs define the parameters required by PerfConsume
type ConsumeArgs struct {
	Topic             string
	SubscriptionName  string
	ReceiverQueueSize int
	Concurrency       uint

	// limits
	ConsumeQuotaLimit       string
	ConsumeRateLimit        string
	ConsumeConcurrencyLimit string

	// cost
	handleCostAvgInMs        int64   // 处理消息业务时长
	handleCostPositiveJitter float64 // 处理消息快波动率
	handleCostNegativeJitter float64 // 处理消息慢波动率

	// goto weights
	ConsumeGoto param.GotoParam
	StatusParam param.StatusParam
}

func NewConsumerCommand(rtArgs *internal.RootArgs) *cobra.Command {
	cArgs := &ConsumeArgs{}
	cmd := &cobra.Command{
		Use:   "consume <topic>",
		Short: "Consume messages from a topic and measure its performance",
		Example: "(1) soften-perf consume test -R 0\n" +
			"(2) soften-perf consume test -R 20,50,80\n" +
			"(3) soften-perf consume test persistent/public/default/test --consume-goto-retrying-weight 2\n" +
			"(4) soften-perf consume test --consume-quota-limit 20,50,80 --consume-concurrency-limit 20,20,20",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cArgs.Topic = args[0]
			PerfConsume(rtArgs.Ctx, rtArgs, cArgs)
		},
	}
	// load flags here
	LoadConsumeFlags(cmd.Flags(), cArgs)

	return cmd
}

func LoadConsumeFlags(flags *pflag.FlagSet, cArgs *ConsumeArgs) {
	flags.StringVarP(&cArgs.SubscriptionName, "subscription", "S", "sub", "Subscription name")
	flags.IntVar(&cArgs.ReceiverQueueSize, "receive-queue-size", 1000, "Receiver queue size")
	flags.UintVarP(&cArgs.Concurrency, "concurrency", "C", 50, "consume concurrency")

	// limits
	flags.StringVar(&cArgs.ConsumeQuotaLimit, "consume-quota-limit", "0,0", "consume quota for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	flags.StringVarP(&cArgs.ConsumeRateLimit, "consume-rate-limit", "R", "0,0", "consume qps for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	flags.StringVar(&cArgs.ConsumeConcurrencyLimit, "consume-concurrency-limit", "0,0", "handle concurrences for different types")

	// cost
	flags.Int64Var(&cArgs.handleCostAvgInMs, "handle-cost-avg-in-ms", 100, "mocked average consume cost in milliseconds, use -1 to disable")
	flags.Float64Var(&cArgs.handleCostPositiveJitter, "handle-cost-positive-jitter", 0, "cost positive jitter")
	flags.Float64Var(&cArgs.handleCostNegativeJitter, "handle-cost-negative-jitter", 0, "cost negative jitter")

	// handle goto weights
	flags.Uint64Var(&cArgs.ConsumeGoto.DoneWeight, "consume-goto-done-weight", 10, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.DiscardWeight, "consume-goto-discard-weight", 0, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.DeadWeight, "consume-goto-dead-weight", 0, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.PendingWeight, "consume-goto-pending-weight", 0, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.BlockingWeight, "consume-goto-blocking-weight", 0, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.RetryingWeight, "consume-goto-retrying-weight", 0, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.UpgradeWeight, "consume-goto-upgrade-weight", 0, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.DegradeWeight, "consume-goto-degrade-weight", 0, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.ShiftWeight, "consume-goto-shift-weight", 0, "the weight to handle messages to")
	flags.Uint64Var(&cArgs.ConsumeGoto.TransferWeight, "consume-goto-transfer-weight", 0, "the weight to handle messages to")
	flags.StringVar(&cArgs.ConsumeGoto.Weight, "goto-weight", "1,0,0,0,0,0,0,0,0,0",
		//flags.StringVar(&cArgs.Weight, "goto-weight", "100,0,0,0,0,0,0,0,0",
		"the weight to handle messages to: [done, discard, dead, pending, blocking, retrying, upgrade, degrade, shift, transfer]. example like: '100,5,5,30,10,60,0,0,0,0'")

	// status params
	flags.UintVar(&cArgs.StatusParam.PendingReentrantDelay, "pending-reentrant-delay", 20, "reentrant delay for pending")
	flags.UintVar(&cArgs.StatusParam.BlockingReentrantDelay, "blocking-reentrant-delay", 20, "reentrant delay for blocking")
	flags.UintVar(&cArgs.StatusParam.RetryingReentrantDelay, "retrying-reentrant-delay", 20, "reentrant delay for retrying")

	if cArgs.handleCostAvgInMs < 0 {
		cArgs.handleCostAvgInMs = 0
	}
	if cArgs.handleCostNegativeJitter > 1 {
		cArgs.handleCostNegativeJitter = 1
	}
}
