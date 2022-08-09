package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/bmizerany/perks/quantile"
	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/shenqianjin/soften-client-go/soften/message"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// consumeArgs define the parameters required by perfConsume
type consumeArgs struct {
	Topic             string
	SubscriptionName  string
	ReceiverQueueSize int

	costAverageInMs    int64   // 处理消息业务时长
	costPositiveJitter float64 // 处理消息快波动率
	costNegativeJitter float64 // 处理消息慢波动率

	Concurrency         uint
	ConsumeQuotas       []uint64 // 消费type粒度Quota限制
	ConsumeRates        []uint64 // 消费type粒度QPS限制
	ConsumeConcurrences []uint64 // 消费type粒度并发限制
	GotoWeights         []uint64 // 处理决策权重, 固定顺序: [done, discard, dead, pending, blocking, retrying, upgrade, degrade]
}

var handleGotoIndexes = map[string]int{
	handler.GotoDone.String():     0,
	handler.GotoDiscard.String():  1,
	handler.GotoDead.String():     2,
	handler.GotoPending.String():  3,
	handler.GotoBlocking.String(): 4,
	handler.GotoRetrying.String(): 5,
	handler.GotoUpgrade.String():  6,
	handler.GotoDegrade.String():  7,
}

type consumer struct {
	clientArgs   *clientArgs
	consumerArgs *consumeArgs

	handleGotoChoice internal.GotoPolicy
	costPolicy       internal.CostPolicy

	consumeStatCh chan *consumeStat

	//radicalLimiters     map[string]*rate.RateLimiter
	concurrencyLimiters map[string]internal.CountLimiter // 超并发限制时,去pending队列
	rateLimiters        map[string]*rate.Limiter         // 超额度限制时,去blocking对列
	quotaLimiters       map[string]internal.CountLimiter // 超额度限制时,去blocking对列
}

type consumeStat struct {
	bytes           int64
	receivedLatency float64
	finishedLatency float64
	handledLatency  float64
	typeName        string
}

func newConsumer(clientArgs *clientArgs, consumerArgs *consumeArgs) *consumer {
	gotoWeightMap := map[string]uint64{
		handler.GotoDone.String(): consumerArgs.GotoWeights[handleGotoIndexes[handler.GotoDone.String()]],
	}
	if weight := consumerArgs.GotoWeights[handleGotoIndexes[handler.GotoRetrying.String()]]; weight > 0 {
		gotoWeightMap[handler.GotoRetrying.String()] = weight
	}
	if weight := consumerArgs.GotoWeights[handleGotoIndexes[handler.GotoPending.String()]]; weight > 0 {
		gotoWeightMap[handler.GotoPending.String()] = weight
	}
	if weight := consumerArgs.GotoWeights[handleGotoIndexes[handler.GotoBlocking.String()]]; weight > 0 {
		gotoWeightMap[handler.GotoBlocking.String()] = weight
	}
	if weight := consumerArgs.GotoWeights[handleGotoIndexes[handler.GotoDead.String()]]; weight > 0 {
		gotoWeightMap[handler.GotoDead.String()] = weight
	}
	if weight := consumerArgs.GotoWeights[handleGotoIndexes[handler.GotoDiscard.String()]]; weight > 0 {
		gotoWeightMap[handler.GotoDiscard.String()] = weight
	}
	handleGotoChoice := internal.NewRoundRandWeightGotoPolicy(gotoWeightMap)

	// Retry to create producer indefinitely
	c := &consumer{
		clientArgs:          clientArgs,
		consumerArgs:        consumerArgs,
		handleGotoChoice:    handleGotoChoice,
		consumeStatCh:       make(chan *consumeStat),
		concurrencyLimiters: make(map[string]internal.CountLimiter),
		rateLimiters:        make(map[string]*rate.Limiter),
		quotaLimiters:       make(map[string]internal.CountLimiter),
	}
	// initialize cost policy
	if consumerArgs.costAverageInMs > 0 {
		c.costPolicy = internal.NewAvgCostPolicy(consumerArgs.costAverageInMs, consumerArgs.costPositiveJitter, consumerArgs.costNegativeJitter)
	}
	// handle message type-level concurrency limiters
	if len(consumerArgs.ConsumeConcurrences) > 0 {
		for index, con := range consumerArgs.ConsumeConcurrences {
			if con > 0 {
				c.concurrencyLimiters[fmt.Sprintf("Type-%d", index+1)] = internal.NewCountLimiter(int(con)) // rate.New(int(li), time.Second)
			} else {
				c.concurrencyLimiters[fmt.Sprintf("Type-%d", index+1)] = nil
			}
		}
	}
	// handle message type-level qps limiters
	if len(consumerArgs.ConsumeRates) > 0 {
		for index, r := range consumerArgs.ConsumeRates {
			if r > 0 {
				c.rateLimiters[fmt.Sprintf("Type-%d", index+1)] = rate.NewLimiter(rate.Limit(r), int(r)) // rate.New(int(li), time.Second)
			} else {
				c.rateLimiters[fmt.Sprintf("Type-%d", index+1)] = nil
			}
		}

	}
	// handle message type-level quota limiters
	if len(consumerArgs.ConsumeRates) > 0 {
		for index, quota := range consumerArgs.ConsumeQuotas {
			if quota > 0 {
				c.quotaLimiters[fmt.Sprintf("Type-%d", index+1)] = internal.NewCountLimiter(int(quota)) // rate.New(int(li), time.Second)
			} else {
				c.quotaLimiters[fmt.Sprintf("Type-%d", index+1)] = nil
			}
		}

	}

	return c
}

func (c *consumer) perfConsume(stop <-chan struct{}) {
	b, _ := json.MarshalIndent(c.clientArgs, "", "  ")
	log.Info("Client config: ", string(b))
	b, _ = json.MarshalIndent(c.consumerArgs, "", "  ")
	log.Info("Consumer config: ", string(b))

	// create client
	client, err := newClient(c.clientArgs)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// create consumer
	listener, err := client.CreateListener(config.ConsumerConfig{
		Topic:            c.consumerArgs.Topic,
		SubscriptionName: c.consumerArgs.SubscriptionName,
		Concurrency:      &config.ConcurrencyPolicy{CorePoolSize: c.consumerArgs.Concurrency},
		//PendingEnable:    true,
		//BlockingEnable:   true,
		RetryingEnable: true,
	},
		//checker.PrevHandlePending(c.concurrencyCheck),
		//checker.PrevHandleBlocking(c.quotaCheck),
		checker.PrevHandleRetrying(c.rateCheck))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	// start monitoring: async
	go c.stats(stop, c.consumeStatCh)

	// start message listener
	err = listener.StartPremium(context.Background(), c.internalHandle)
	if err != nil {
		log.Fatal(err)
	}

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)
	log.Println("soften performing - consumer done")
}

// 各类型超Quota限制时,去blocking队列
func (c *consumer) quotaCheck(msg pulsar.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Type"]; ok {
		if limiter, ok2 := c.quotaLimiters[typeName]; ok2 && limiter != nil {
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
func (c *consumer) concurrencyCheck(msg pulsar.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Type"]; ok {
		if limiter, ok2 := c.concurrencyLimiters[typeName]; ok2 && limiter != nil {
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
func (c *consumer) rateCheck(msg pulsar.Message) checker.CheckStatus {
	if typeName, ok := msg.Properties()["Type"]; ok {
		if limiter, ok2 := c.rateLimiters[typeName]; ok2 && limiter != nil {
			if !limiter.Allow() { // non-blocking operation
				return checker.CheckStatusPassed
			}
		}
	}
	return checker.CheckStatusRejected
}

var RFC3339TimeInSecondPattern = "20060102150405.999"

func (c *consumer) internalHandle(cm pulsar.Message) handler.HandleStatus {
	originPublishTime := cm.PublishTime()
	if originalPublishTime, ok := cm.Properties()[message.XPropertyOriginPublishTime]; ok {
		if parsedTime, err := time.Parse(RFC3339TimeInSecondPattern, originalPublishTime); err == nil {
			originPublishTime = parsedTime
		}
	}
	start := time.Now()
	stat := &consumeStat{
		bytes:           int64(len(cm.Payload())),
		receivedLatency: time.Since(cm.PublishTime()).Seconds(),
	}

	if c.consumerArgs.costAverageInMs > 0 {
		time.Sleep(c.costPolicy.Next()) // 模拟业务处理
	}
	result := handler.HandleStatusOk
	n := c.handleGotoChoice.Next()
	switch n {
	case string(handler.GotoRetrying):
		result = handler.HandleStatusBuilder().Goto(handler.GotoRetrying).Build()
	case string(handler.GotoPending):
		result = handler.HandleStatusBuilder().Goto(handler.GotoPending).Build()
	case string(handler.GotoBlocking):
		result = handler.HandleStatusBuilder().Goto(handler.GotoBlocking).Build()
	case string(handler.GotoDead):
		result = handler.HandleStatusBuilder().Goto(handler.GotoDead).Build()
	case string(handler.GotoDiscard):
		result = handler.HandleStatusBuilder().Goto(handler.GotoDiscard).Build()
	default:
		result = handler.HandleStatusOk
		stat.finishedLatency = time.Since(originPublishTime).Seconds() // 从消息产生到处理完成的时间(中间状态不是完成状态)
		if radicalKey, ok := cm.Properties()["Type"]; ok {
			stat.typeName = radicalKey
			//stat.radicalFinishedLatencies[typeName] = stat.finishedLatency
		}
	}

	stat.handledLatency = time.Since(start).Seconds()
	if radicalKey, ok := cm.Properties()["Type"]; ok {
		stat.typeName = radicalKey
		//stat.radicalHandledLatencies[typeName] = stat.handledLatency
	}
	c.consumeStatCh <- stat
	return result
}

func (c *consumer) stats(stop <-chan struct{}, consumeStatCh <-chan *consumeStat) {
	// Print stats of the perfConsume rate
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	receivedQ := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	finishedQ := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	handledQ := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	msgHandled := int64(0)
	bytesHandled := int64(0)
	typeNames := make([]string, 0)
	radicalHandleMsg := make(map[string]int64, len(c.consumerArgs.ConsumeConcurrences))
	radicalHandleQ := make(map[string]*quantile.Stream, len(c.consumerArgs.ConsumeConcurrences))
	radicalFinishedQ := make(map[string]*quantile.Stream, len(c.consumerArgs.ConsumeConcurrences))

	for {
		select {
		case <-stop:
			log.Infof("Closing consume stats printer")
			return
		case <-tick.C:
			currentMsgReceived := atomic.SwapInt64(&msgHandled, 0)
			currentBytesReceived := atomic.SwapInt64(&bytesHandled, 0)
			msgRate := float64(currentMsgReceived) / float64(10)
			bytesRate := float64(currentBytesReceived) / float64(10)

			statB := &bytes.Buffer{}
			_, _ = fmt.Fprintf(statB, `<<<<<<<<<<
			Summary - Consume rate: %6.1f msg/s - %6.1f Mbps - 
				Received Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f  
				Finished Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f
				Handled  Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`,
				msgRate, bytesRate*8/1024/1024,

				receivedQ.Query(0.5)*1000,
				receivedQ.Query(0.95)*1000,
				receivedQ.Query(0.99)*1000,
				receivedQ.Query(0.999)*1000,
				receivedQ.Query(1.0)*1000,

				finishedQ.Query(0.5)*1000,
				finishedQ.Query(0.95)*1000,
				finishedQ.Query(0.99)*1000,
				finishedQ.Query(0.999)*1000,
				finishedQ.Query(1.0)*1000,

				handledQ.Query(0.5)*1000,
				handledQ.Query(0.95)*1000,
				handledQ.Query(0.99)*1000,
				handledQ.Query(0.999)*1000,
				handledQ.Query(1.0)*1000,
			)
			if len(radicalHandleMsg) > 0 {
				_, _ = fmt.Fprintf(statB, `
			Detail >> `)
			}
			for _, typeName := range typeNames {
				_, _ = fmt.Fprintf(statB, "%s rate: %6.1f msg/s - ", typeName, float64(radicalHandleMsg[typeName])/float64(10))
				radicalHandleMsg[typeName] = 0
			}
			for _, typeName := range typeNames {
				q, ok := radicalFinishedQ[typeName]
				if !ok {
					continue
				}
				_, _ = fmt.Fprintf(statB, `
				  %s Finished Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`, typeName,
					q.Query(0.5)*1000, q.Query(0.95)*1000, q.Query(0.99)*1000, q.Query(0.999)*1000, q.Query(1.0)*1000)
			}
			for _, typeName := range typeNames {
				q, ok := radicalHandleQ[typeName]
				if !ok {
					continue
				}
				_, _ = fmt.Fprintf(statB, `
				  %s Handled  Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`, typeName,
					q.Query(0.5)*1000, q.Query(0.95)*1000, q.Query(0.99)*1000, q.Query(0.999)*1000, q.Query(1.0)*1000)
			}
			log.Info(statB.String())

			receivedQ.Reset()
			finishedQ.Reset()
			handledQ.Reset()
			for _, q := range radicalHandleQ {
				q.Reset()
			}
			for _, q := range radicalFinishedQ {
				q.Reset()
			}
			//messagesConsumed = 0
		case stat := <-consumeStatCh:
			msgHandled++
			bytesHandled += stat.bytes
			receivedQ.Insert(stat.receivedLatency)
			finishedQ.Insert(stat.finishedLatency)
			handledQ.Insert(stat.handledLatency)
			if _, ok := radicalHandleMsg[stat.typeName]; !ok {
				typeNames = append(typeNames, stat.typeName)
				if len(typeNames) == len(c.consumerArgs.ConsumeConcurrences) {
					sort.Strings(typeNames)
				}
			}
			radicalHandleMsg[stat.typeName]++
			// handle
			if _, ok := radicalHandleQ[stat.typeName]; !ok {
				radicalHandleQ[stat.typeName] = quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
			}
			radicalHandleQ[stat.typeName].Insert(stat.handledLatency)
			// finish
			if _, ok := radicalFinishedQ[stat.typeName]; !ok {
				radicalFinishedQ[stat.typeName] = quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
			}
			radicalFinishedQ[stat.typeName].Insert(stat.finishedLatency)
		}
	}
}
