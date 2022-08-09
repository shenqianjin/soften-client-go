package main

import (
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/pflag"
)

var flagLoader = &flagLoaderImpl{}

type flagLoaderImpl struct {
	perfFlagLoadOnce    sync.Once
	consumeFlagLoadOnce sync.Once
	produceFlagLoadOnce sync.Once
}

func (l *flagLoaderImpl) loadPerfFlags(flags *pflag.FlagSet, pArgs *perfArgs, cliArgs *clientArgs) {
	flags.IntVar(&pArgs.profilePort, "profilePort", 6060, "Port to expose profiling info, use -1 to disable")
	flags.IntVar(&pArgs.PrometheusPort, "metrics", 8000, "Port to use to export metrics for Prometheus. Use -1 to disable.")
	flags.BoolVar(&pArgs.flagDebug, "debug", false, "enable debug output")

	flags.StringVarP(&cliArgs.ServiceURL, "service-url", "u", "pulsar://localhost:6650", "The Pulsar service URL")
	flags.StringVar(&cliArgs.TokenFile, "token-file", "", "file path to the Pulsar JWT file")
	flags.StringVar(&cliArgs.TLSTrustCertFile, "trust-cert-file", "", "file path to the trusted certificate file")
}

func (l *flagLoaderImpl) loadProduceFlags(flags *pflag.FlagSet, pArgs *produceArgs) {
	var produceRateStr string
	flags.StringVarP(&produceRateStr, "produce-rate", "r", "20,80", "produce qps for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	pArgs.ProduceRates = l.parseUint64Array(produceRateStr)
	flags.IntVarP(&pArgs.BatchingTimeMillis, "batching-time", "b", 1, "Batching grouping time in millis")
	flags.IntVarP(&pArgs.BatchingMaxSize, "batching-max-size", "", 128, "Max size of a batch (in KB)")
	flags.IntVarP(&pArgs.MessageSize, "msg-size", "s", 1024, "Message size (int B)")
	flags.IntVarP(&pArgs.ProducerQueueSize, "queue-size", "q", 1000, "Produce queue size")

}

func (l *flagLoaderImpl) loadConsumeFlags(flags *pflag.FlagSet, cArgs *consumeArgs) {
	flags.StringVarP(&cArgs.SubscriptionName, "subscription", "S", "sub", "Subscription name")
	flags.IntVar(&cArgs.ReceiverQueueSize, "receiver-queue-size", 100000, "Receiver queue size")

	flags.Int64Var(&cArgs.costAverageInMs, "cost-average-in-ms", 100, "consume cost average in milliseconds, use -1 to disable")
	flags.Float64Var(&cArgs.costPositiveJitter, "cost-positive-jitter", 0, "cost positive jitter")
	flags.Float64Var(&cArgs.costNegativeJitter, "cost-negative-jitter", 0, "cost negative jitter")

	flags.UintVarP(&cArgs.Concurrency, "concurrency", "C", 50, "consume concurrency")
	var consumeQuotaStr string
	flags.StringVar(&consumeQuotaStr, "consume-quota", "0,0", "consume quota for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	cArgs.ConsumeQuotas = l.parseUint64Array(consumeQuotaStr)
	var consumeRateStr string
	flags.StringVarP(&consumeRateStr, "consume-rate", "R", "50,50", "consume qps for types [t1, t2, t3, ..., tn]. 0 means un-throttled")
	cArgs.ConsumeRates = l.parseUint64Array(consumeRateStr)
	var consumeConcurrencesStr string
	flags.StringVar(&consumeConcurrencesStr, "consume-concurrency", "50,50", "handle concurrences for different types")
	cArgs.ConsumeConcurrences = l.parseUint64Array(consumeConcurrencesStr)
	var gotoWeightStr string
	//flags.StringVar(&gotoWeightStr, "goto-weight", "100,5,5,30,10,60,0,0",
	flags.StringVar(&gotoWeightStr, "goto-weight", "100,0,0,0,0,0,0,0",
		"handle goto weights for different types. fixed order and number as: [done, discard, dead, pending, blocking, retrying, upgrade, degrade]")
	cArgs.GotoWeights = l.parseUint64Array(gotoWeightStr)

	if cArgs.costAverageInMs < 0 {
		cArgs.costAverageInMs = 0
	}
	if cArgs.costNegativeJitter > 1 {
		cArgs.costNegativeJitter = 1
	}
}

func (l *flagLoaderImpl) parseUint64Array(str string) []uint64 {
	nums := make([]uint64, 0)
	if str != "" {
		numsStr := strings.Split(str, ",")
		for _, ns := range numsStr {
			if n, err := strconv.ParseUint(ns, 10, 32); err != nil {
				panic(err)
			} else if n < 0 {
				panic("negative rate is invalid")
			} else {
				nums = append(nums, n)
			}
		}
	}
	return nums
}
