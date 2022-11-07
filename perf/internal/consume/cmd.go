package consume

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/param"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/stats"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
	sutil "github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/sirupsen/logrus"
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
}

func LoadConsumeFlags(flags *pflag.FlagSet, cArgs *ConsumeArgs) {
	flags.StringVarP(&cArgs.SubscriptionName, "subscription", "S", "sub", "Subscription name")
	flags.IntVar(&cArgs.ReceiverQueueSize, "receive-queue-size", 1000, "Receiver queue size")
	flags.UintVarP(&cArgs.Concurrency, "concurrency", "C", 50, "consume concurrency")

	// limits
	flags.StringVar(&cArgs.ConsumeQuotaLimit, "consume-quota-limit", "0,0", "consume quota for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	flags.StringVarP(&cArgs.ConsumeRateLimit, "consume-rate-limit", "R", "50,50", "consume qps for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	flags.StringVar(&cArgs.ConsumeConcurrencyLimit, "consume-concurrency-limit", "50,50", "handle concurrences for different types")

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
	flags.StringVar(&cArgs.ConsumeGoto.Weight, "goto-weight", "100,5,5,30,10,60,0,0,0,0",
		//flags.StringVar(&cArgs.Weight, "goto-weight", "100,0,0,0,0,0,0,0,0",
		"the weight to handle messages to: [done, discard, dead, pending, blocking, retrying, upgrade, degrade, shift, transfer]")

	if cArgs.handleCostAvgInMs < 0 {
		cArgs.handleCostAvgInMs = 0
	}
	if cArgs.handleCostNegativeJitter > 1 {
		cArgs.handleCostNegativeJitter = 1
	}
}

func NewConsumerCommand(rtArgs *internal.RootArgs) *cobra.Command {
	cArgs := &ConsumeArgs{}
	cmd := &cobra.Command{
		Use:   "consume <topic>",
		Short: "Consume messages from a topic and measure its performance",
		Example: "(1) soften-perf consume test -R 0\n" +
			"(2) soften-perf consume test -R 20,50,80\n" +
			"(3) soften-perf consume test persistent/public/default/test --consume-goto-retrying-weight 2",
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
	ens := svc.collectEnables()
	lvlPolicy := &config.LevelPolicy{
		DiscardEnable:  config.ToPointer(ens.DiscardEnable),
		DeadEnable:     config.ToPointer(ens.DeadEnable),
		PendingEnable:  config.ToPointer(ens.PendingEnable),
		BlockingEnable: config.ToPointer(ens.BlockingEnable),
		RetryingEnable: config.ToPointer(ens.RetryingEnable),
		UpgradeEnable:  config.ToPointer(ens.UpgradeEnable),
		DegradeEnable:  config.ToPointer(ens.DegradeEnable),
		ShiftEnable:    config.ToPointer(ens.ShiftEnable),
		TransferEnable: config.ToPointer(ens.TransferEnable),
	}
	lvls := message.Levels{message.L1}
	checkpoints := make([]checker.ConsumeCheckpoint, 0)
	if ens.UpgradeEnable {
		lvls = append(lvls, message.L2)
		lvlPolicy.Upgrade = &config.ShiftPolicy{Level: message.L2, ConnectInSyncEnable: true}
	}
	if ens.DegradeEnable {
		lvls = append(lvls, message.B1)
		lvlPolicy.Degrade = &config.ShiftPolicy{Level: message.B1, ConnectInSyncEnable: true}
	}
	if ens.ShiftEnable {
		lvls = append(lvls, message.L3)
		lvlPolicy.Shift = &config.ShiftPolicy{Level: message.L3, ConnectInSyncEnable: true}
	}
	if ens.TransferEnable {
		lvlPolicy.Transfer = &config.TransferPolicy{Topic: cmdArgs.Topic + message.L4.TopicSuffix(), ConnectInSyncEnable: true}
	}
	// collect status
	statuses := message.Statuses{message.StatusReady}
	if ens.PendingEnable {
		statuses = append(statuses, message.StatusPending)
	}
	if ens.BlockingEnable {
		statuses = append(statuses, message.StatusBlocking)
	}
	if ens.RetryingEnable {
		statuses = append(statuses, message.StatusRetrying)
	}
	if ens.DeadEnable {
		statuses = append(statuses, message.StatusDead)
	}

	// validate topic existence
	if !rtArgs.AutoCreateTopic {
		topics, err1 := sutil.FormatTopics(cmdArgs.Topic, lvls, statuses, cmdArgs.SubscriptionName)
		if err1 != nil {
			logrus.Fatal(err1)
		}
		manager := admin.NewRobustTopicManager(rtArgs.BrokerUrl)
		content := &bytes.Buffer{}
		for _, topic := range topics {
			if _, err2 := manager.Stats(topic); err2 != nil {
				fmt.Fprintf(content, "stats %v err: %v", topic, err2)
			}
		}
		if content.Len() > 0 {
			logrus.Fatalf(`failed to validate topic existence:\n%s`, content.String())
		}
	}

	for _, v := range svc.concurrencyLimiters {
		if v != nil {
			checkpoints = append(checkpoints, checker.PrevHandlePending(svc.exceedConcurrency))
			break
		}
	}
	for _, v := range svc.rateLimiters {
		if v != nil {
			checkpoints = append(checkpoints, checker.PrevHandlePending(svc.exceedRate))
			break
		}
	}
	for _, v := range svc.quotaLimiters {
		if v != nil {
			checkpoints = append(checkpoints, checker.PrevHandlePending(svc.exceedQuota))
			break
		}
	}

	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:            cmdArgs.Topic,
		SubscriptionName: cmdArgs.SubscriptionName,
		Concurrency:      &config.ConcurrencyPolicy{CorePoolSize: cmdArgs.Concurrency},
		LevelPolicy:      lvlPolicy,
		Levels:           lvls,
	}, checkpoints...)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// start monitoring: async
	go stats.ConsumeStats(ctx, svc.consumeStatCh, len(svc.concurrencyLimits))

	// start message listener
	err = listener.StartPremium(context.Background(), svc.internalHandle)
	if err != nil {
		log.Fatal(err)
	}

	//
	<-ctx.Done()
}
