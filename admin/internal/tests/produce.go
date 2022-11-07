package tests

import (
	"context"
	"log"
	"strconv"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	supportUtil "github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/shenqianjin/soften-client-go/admin/internal/tests/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type produceArgs struct {
	sendCount                    uint64
	sendAsyncCount               uint64
	printProgressIterateInterval uint64
	printMode                    uint
}

func newProduceCommand(rtArgs *internal.RootArgs, mdlArgs *testsArgs) *cobra.Command {
	cmdArgs := &produceArgs{}
	mockedDataSample := `{"uid":2007008,"name":"shen","age":12,"spouse":{"uid":2006008,"name":"wang","age":10},"friends":[{"uid":2005008,"name":"zhang","age":11}]}`
	cmd := &cobra.Command{
		Use:   "produce ",
		Short: "Produce tests messages with mocked random data.",
		Long: "Produce tests messages with mocked random data." +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>\n" +
			"\n" +
			"Please note that mocked string payload looks like:\n" +
			"  " + mockedDataSample,
		Example: "1. produce 6000 messages into 'TEST' topic with 2500 by send and 3500 by send async (in batch):\n" +
			"produce TEST --send-count 2500 --send-async-count 3500\n" +
			"2. produce messages into 'TEST' topic within print message details:\n" +
			"produce TEST print-mode 2\n",

		Args: cobra.MinimumNArgs(1),

		Run: func(cmd *cobra.Command, args []string) {
			mdlArgs.topic = args[0]
			produceMessages(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().Uint64Var(&cmdArgs.printProgressIterateInterval, "print-progress-iterate-interval", 100, constant.PrintProgressIterateIntervalUsage)
	cmd.Flags().UintVar(&cmdArgs.printMode, "print-mode", 0, constant.PrintModeUsage)
	cmd.Flags().Uint64Var(&cmdArgs.sendCount, "send-count", 1200, "the number of mocked messages to send")
	cmd.Flags().Uint64Var(&cmdArgs.sendAsyncCount, "send-async-count", 1800, "the number of mocked messages to send in async")
	return cmd
}

func produceMessages(rtArgs *internal.RootArgs, mdlArgs *testsArgs, cmdArgs *produceArgs) {
	/*manager := admin.NewRobustTopicManager(rtArgs.WebUrl)
	// validate
	if _, err := manager.Stats(mdlArgs.topic); err != nil {
		logrus.Fatal(err)
	}*/

	logrus.Infof("start to mock produce messages into %v\n", mdlArgs.topic)
	// create client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: mdlArgs.BrokerUrl,
	})
	if err != nil {
		logrus.Fatal(err)
	}
	defer client.Close()
	// create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: mdlArgs.topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// send msg
	syncCount := cmdArgs.sendCount
	uid := uint64(1)
	for ; uid <= syncCount; uid++ {
		msg := util.GenerateBusinessMsg(uint32(uid))
		msg.Properties["Index"] = strconv.FormatUint(uid, 10)
		mid, err := producer.Send(context.Background(), msg)
		if err != nil {
			logrus.Fatal(err)
		}
		if cmdArgs.printProgressIterateInterval > 0 && uid%cmdArgs.printProgressIterateInterval == 0 {
			logrus.Infof("mock progress: produce: %v", uid)
		}
		printWhenMockProduceDone(msg, mid, cmdArgs)
	}
	// send async msg
	asyncCount := cmdArgs.sendAsyncCount
	wg := sync.WaitGroup{}
	limit := syncCount + asyncCount
	for ; uid <= limit; uid++ {
		msg := util.GenerateBusinessMsg(uint32(uid))
		msg.Properties["Index"] = strconv.FormatUint(uid, 10)
		wg.Add(1)
		func(uid uint64) {
			producer.SendAsync(context.Background(), msg, func(mid pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
				if err != nil {
					logrus.Fatal(err)
				}
				if cmdArgs.printProgressIterateInterval > 0 && uid%cmdArgs.printProgressIterateInterval == 0 {
					logrus.Infof("mock progress: produce: %v", uid)
				}
				printWhenMockProduceDone(msg, mid, cmdArgs)
				wg.Done()
			})
		}(uid)
	}
	wg.Wait()
	logrus.Infof("end to mock produce messages into %v. sent: %v, sentAysnc: %v \n", mdlArgs.topic, cmdArgs.sendCount, cmdArgs.sendAsyncCount)
}

func printWhenMockProduceDone(msg *pulsar.ProducerMessage, mid pulsar.MessageID, cmdArgs *produceArgs) {
	switch cmdArgs.printMode {
	case 1:
		logrus.Printf("Uid: %v, produced msg: %v", msg.Properties["Index"], mid)
	case 2:
		logrus.Printf("Uid: %v, produced msg: %v", msg.Properties["Index"], supportUtil.FormatProducerMessage4Print(msg, mid))
	default:
		// print nothing
	}
}
