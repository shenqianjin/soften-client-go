package topics

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type updateArgs struct {
	groundTopic string
	partitions  uint
	all         bool
}

func newUpdateCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &updateArgs{}
	cmd := &cobra.Command{
		Use:   "update ",
		Short: "Update partitions for soften topic or topics by ground topic.",
		Long: "Update partitions for soften topic or topics by ground topic.\n" +
			"It is only active for partitioned topics.\n" +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>",
		Example: "(1) soften-admin topics update public/default/test -p 12\n" +
			"(2) soften-admin topics update persistent://business/finance/equity -Ap 24",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			updateTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().UintVarP(&cmdArgs.partitions, "partitions", "p", 0, constant.PartitionsUsage4Update)
	cmd.Flags().BoolVar(&cmdArgs.all, "all", true, constant.AllUsage)

	return cmd
}

func updateTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *updateArgs) {
	if cmdArgs.partitions <= 0 {
		logrus.Fatal("please specify the partitions (with -p or --partitions options) " +
			"and make sure it is more than the original value")
	}
	namespaceTopic, err := util.ParseNamespaceTopic(cmdArgs.groundTopic)
	if err != nil {
		logrus.Fatalf("update \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	}
	if namespaceTopic.ShortTopic == "" {
		logrus.Fatalf("update \"%s\" failed: %v\n", cmdArgs.groundTopic, "invalid topic name")
	}
	var topics []string
	if cmdArgs.all {
		// query topics from broker
		topics, err = queryTopicsFromBrokerByOptions(queryOptions{
			url:            rtArgs.WebUrl,
			namespaceTopic: *namespaceTopic,
			partitioned:    true,
		})
		if err != nil {
			logrus.Fatalf("update \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
		}
	} else {
		// filter by options
		if mdlArgs.level != "" || mdlArgs.status != "" || mdlArgs.subscription != "" {
			topics = util.FormatTopics(namespaceTopic.FullName, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
		}
	}

	if len(topics) == 0 {
		logrus.Warn("Not Found")
	}
	// update partitions
	manager := admin.NewPartitionedTopicManager(rtArgs.WebUrl)
	for _, topic := range topics {
		err := manager.Update(topic, cmdArgs.partitions)
		if err != nil {
			logrus.Warnf("updated \"%s\" failed: %v\n", topic, err)
		} else {
			logrus.Infof("updated \"%s\" successfully, partitions is %v now\n", topic, cmdArgs.partitions)
		}
	}
}
