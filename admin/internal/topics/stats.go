package topics

import (
	"encoding/json"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type statArgs struct {
	groundTopic string
	partitioned bool
	all         bool
}

func newStatCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &statArgs{}
	cmd := &cobra.Command{
		Use:   "stats ",
		Short: "Stats soften topic or topics by ground topic.",
		Long: "Stats soften topic or topics by ground topic.\n" +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>",
		Example: "(1) soften-admin topics stats public/default/test01\n" +
			"(2) soften-admin topics stats persistent://business/finance/equity -A",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			statTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().BoolVarP(&cmdArgs.partitioned, "partitioned", "P", false, constant.PartitionedUsage)
	cmd.Flags().BoolVar(&cmdArgs.all, "all", true, constant.AllUsage)

	return cmd
}

func statTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *statArgs) {
	namespaceTopic, err := util.ParseNamespaceTopic(cmdArgs.groundTopic)
	if err != nil {
		logrus.Fatalf("stats \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	} else if namespaceTopic.ShortTopic == "" {
		logrus.Fatalf("stats \"%s\" failed: %v\n", cmdArgs.groundTopic, "not found topic")
	}
	var topics []string
	if cmdArgs.all {
		// query topics from broker
		topics, err = queryTopicsFromBrokerByOptions(queryOptions{
			url:            rtArgs.WebUrl,
			namespaceTopic: *namespaceTopic,
			partitioned:    cmdArgs.partitioned,
		})
		if err != nil {
			logrus.Fatalf("stats \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
		}
	} else {
		// filter by options
		if mdlArgs.level != "" || mdlArgs.status != "" || mdlArgs.subscription != "" {
			topics = util.FormatTopics(namespaceTopic.FullName, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
		}
	}
	if err != nil {
		logrus.Fatalf("stats \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	}

	if len(topics) == 0 {
		logrus.Warn("Not Found")
	}
	// delete one by one
	manager := admin.NewPartitionedTopicManager(rtArgs.WebUrl)
	npManager := admin.NewNonPartitionedTopicManager(rtArgs.WebUrl)
	for _, topic := range topics {
		var err error
		var st interface{}
		if cmdArgs.partitioned {
			st, err = manager.Stats(topic)
		} else {
			st, err = npManager.Stats(topic)
		}
		if err != nil {
			logrus.Fatalf("stats \"%s\" failed: %v\n", topic, err)
		} else {
			stBytes, err := json.MarshalIndent(st, "", "    ")
			if err != nil {
				logrus.Fatalf("stats \"%s\" failed: %v\n", topic, err)
			}
			logrus.Infof("stats \"%s\" successfully: \n%v\n", topic, string(stBytes))
		}
	}
}
