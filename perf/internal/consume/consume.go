package consume

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/shenqianjin/soften-client-go/perf/internal"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/stats"
	"github.com/shenqianjin/soften-client-go/perf/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
	sutil "github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/sirupsen/logrus"
)

func PerfConsume(ctx context.Context, rtArgs *internal.RootArgs, cmdArgs *ConsumeArgs) {
	b, _ := json.MarshalIndent(rtArgs.ClientArgs, "", "  ")
	logrus.Info("Client config: ", string(b))
	b, _ = json.MarshalIndent(cmdArgs, "", "  ")
	logrus.Info("Consumer config: ", string(b))

	svc := newConsumer(rtArgs, cmdArgs)

	// create client
	client, err := util.NewClient(&rtArgs.ClientArgs)
	if err != nil {
		logrus.Fatal(err)
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
		manager := admin.NewRobustTopicManager(rtArgs.WebUrl)
		content := &bytes.Buffer{}
		for _, topic := range topics {
			if _, err2 := manager.Stats(topic); err2 != nil {
				fmt.Fprintf(content, "\nstats %v err: %v", topic, err2)
			}
		}
		if content.Len() > 0 {
			logrus.Fatalf(`failed to validate topic existence: %s`, content.String())
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
		logrus.Fatal(err)
	}
	defer listener.Close()

	// start monitoring: async
	go stats.ConsumeStats(ctx, svc.consumeStatCh, len(svc.concurrencyLimits))

	// start message listener
	err = listener.StartPremium(context.Background(), svc.internalHandle)
	if err != nil {
		logrus.Fatal(err)
	}

	//
	<-ctx.Done()
}
