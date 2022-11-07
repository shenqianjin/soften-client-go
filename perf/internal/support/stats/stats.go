package stats

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/bmizerany/perks/quantile"
	"github.com/sirupsen/logrus"
)

// ------ produce stats ------

type ProduceStatEntry struct {
	Latency   float64
	GroupName string
}

func ProduceStats(ctx context.Context, statCh <-chan *ProduceStatEntry, messageSize int, typeSize int) {
	// Print stats of publish rate and latencies
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	q := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	messagesPublished := 0
	typeNames := make([]string, 0)
	radicalMsgPublished := make(map[string]int64, typeSize)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			messageRate := float64(messagesPublished) / float64(10)

			statB := &bytes.Buffer{}
			_, _ = fmt.Fprintf(statB, `>>>>>>>>>>
            ProduceStats - Publish rate: %6.1f msg/s - %6.1f Mbps -
                    Finished Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`,
				messageRate,
				messageRate*float64(messageSize)/1024/1024*8,
				q.Query(0.5)*1000,
				q.Query(0.95)*1000,
				q.Query(0.99)*1000,
				q.Query(0.999)*1000,
				q.Query(1.0)*1000,
			)
			if len(radicalMsgPublished) > 0 {
				_, _ = fmt.Fprintf(statB, `
            Detail >> `)
			}
			for _, typeName := range typeNames {
				_, _ = fmt.Fprintf(statB, `
                    %s rate: %6.1f msg/s`, typeName, float64(radicalMsgPublished[typeName])/float64(10))
				radicalMsgPublished[typeName] = 0
			}
			logrus.Info(statB.String())
			q.Reset()
			messagesPublished = 0
		case stat := <-statCh:
			messagesPublished++
			if _, ok := radicalMsgPublished[stat.GroupName]; !ok {
				typeNames = append(typeNames, stat.GroupName)
				if len(typeNames) == typeSize {
					sort.Strings(typeNames)
				}
			}
			radicalMsgPublished[stat.GroupName]++
			q.Insert(stat.Latency)
		}
	}
}

// ------ consume stats ------

type ConsumeStatEntry struct {
	Bytes           int64
	ReceivedLatency float64
	FinishedLatency float64
	HandledLatency  float64
	GroupName       string
}

func ConsumeStats(ctx context.Context, consumeStatCh <-chan *ConsumeStatEntry, typeSize int) {
	// Print stats of the perfConsume rate
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	receivedQ := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	finishedQ := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	handledQ := quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
	msgHandled := int64(0)
	bytesHandled := int64(0)
	typeNames := make([]string, 0)
	radicalHandleMsg := make(map[string]int64, typeSize)
	radicalHandleQ := make(map[string]*quantile.Stream, typeSize)
	radicalFinishedQ := make(map[string]*quantile.Stream, typeSize)

	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Closing consume stats printer")
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
				_, _ = fmt.Fprintf(statB, `
                %s rate: %6.1f msg/s - `, typeName, float64(radicalHandleMsg[typeName])/float64(10))
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
			logrus.Info(statB.String())

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
			bytesHandled += stat.Bytes
			receivedQ.Insert(stat.ReceivedLatency)
			finishedQ.Insert(stat.FinishedLatency)
			handledQ.Insert(stat.HandledLatency)
			if _, ok := radicalHandleMsg[stat.GroupName]; !ok {
				typeNames = append(typeNames, stat.GroupName)
				if len(typeNames) == typeSize {
					sort.Strings(typeNames)
				}
			}
			radicalHandleMsg[stat.GroupName]++
			// handle
			if _, ok := radicalHandleQ[stat.GroupName]; !ok {
				radicalHandleQ[stat.GroupName] = quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
			}
			radicalHandleQ[stat.GroupName].Insert(stat.HandledLatency)
			// finish
			if _, ok := radicalFinishedQ[stat.GroupName]; !ok {
				radicalFinishedQ[stat.GroupName] = quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0)
			}
			radicalFinishedQ[stat.GroupName].Insert(stat.FinishedLatency)
		}
	}
}