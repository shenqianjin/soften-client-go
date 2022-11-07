package produce

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/stats"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/ratelimit"
)

// ProduceArgs define the parameters required by PerfProduce
type ProduceArgs struct {
	Topic            string
	ProduceRate      string // ordered produce rates: [normal, radical 1, radical 2, ..., radical N]
	MessageSize      int
	PendingQueueSize int
}

func LoadProduceFlags(flags *pflag.FlagSet, pArgs *ProduceArgs) {
	flags.StringVarP(&pArgs.ProduceRate, "produce-rate", "r", "20,80",
		"produce qps for different user group, separate with ',' if more than one group such as 't1,t2,t3,...,tn'. 0 means un-throttled")
	flags.IntVar(&pArgs.MessageSize, "msg-size", 1024, "Message size (int B)")
	flags.IntVar(&pArgs.PendingQueueSize, "pending-queue-size", 1000, "Produce queue size")
}

func NewProducerCommand(rtArgs *internal.RootArgs) *cobra.Command {
	cmdArgs := &ProduceArgs{}
	cmd := &cobra.Command{
		Use:   "produce ",
		Short: "Produce messages into a topic and measure its performance",
		Example: "(1) soften-perf produce test -r 0\n" +
			"(2) soften-perf produce test -r 20,50,80",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.Topic = args[0]
			PerfProduce(rtArgs.Ctx, rtArgs, cmdArgs)
		},
	}
	// load flags here
	LoadProduceFlags(cmd.Flags(), cmdArgs)
	return cmd
}

func PerfProduce(ctx context.Context, rtArgs *internal.RootArgs, cmdArgs *ProduceArgs) {
	// validate params
	if cmdArgs.ProduceRate == "" {
		logrus.Fatalf("empty publish rate is invalid")
	}
	// print client info
	b, _ := json.MarshalIndent(rtArgs.ClientArgs, "", "  ")
	logrus.Info("Client config: ", string(b))
	b, _ = json.MarshalIndent(cmdArgs, "", "  ")
	logrus.Info("Producer config: ", string(b))
	// create client
	client, err := util.NewClient(&rtArgs.ClientArgs)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	// create producer
	realProducer, err := client.CreateProducer(config.ProducerConfig{
		Topic:              cmdArgs.Topic,
		MaxPendingMessages: cmdArgs.PendingQueueSize,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer realProducer.Close()

	statCh := make(chan *stats.ProduceStatEntry)
	rates := util.ParseUint64Array(cmdArgs.ProduceRate)
	// start monitoring: async
	go stats.ProduceStats(rtArgs.Ctx, statCh, cmdArgs.MessageSize)

	// start PerfProduce for different types: sync to hang
	for index, produceRate := range rates {
		go internalProduce4Group(rtArgs.Ctx, realProducer, produceRate, fmt.Sprintf("Group-%d", index+1), statCh, cmdArgs.MessageSize)
	}

	// wait root ctx done
	<-ctx.Done()
}

func internalProduce4Group(ctx context.Context, realProducer pulsar.Producer, r uint64, groupName string, ch chan<- *stats.ProduceStatEntry, messageSize int) {
	payload := make([]byte, messageSize)
	//var rateLimiter *rate.RateLimiter
	var rateLimiter ratelimit.Limiter

	if r > 0 {
		//rateLimiter = rate.New(int(r), time.Second)
		//rateLimiter = rate.New(int(r)/10, 100*time.Millisecond)
		rateLimiter = ratelimit.New(int(r), ratelimit.Per(time.Second))
	}
	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Closing produce stats printer")
			return
		default:
		}

		if rateLimiter != nil {
			//rateLimiter.Wait()
			rateLimiter.Take()
		}

		msg := &pulsar.ProducerMessage{
			Payload: payload,
		}

		if groupName != "" {
			msg.Properties = map[string]string{"Group": groupName}
		}

		start := time.Now()
		realProducer.SendAsync(ctx, msg, func(msgID pulsar.MessageID, message *pulsar.ProducerMessage, e error) {
			if e != nil {
				logrus.WithError(e).Fatal("Failed to publish")
			}

			latency := time.Since(start).Seconds()
			stat := &stats.ProduceStatEntry{Latency: latency}
			if typeKey, ok := msg.Properties["Group"]; ok {
				stat.GroupName = typeKey
			}
			ch <- stat
		})
	}
}
