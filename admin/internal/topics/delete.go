package topics

import (
	"errors"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type deleteArgs struct {
	groundTopic string
	partitioned bool
	all         bool
}

func newDeleteCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &deleteArgs{}
	cmd := &cobra.Command{
		Use:   "delete ",
		Short: "Delete soften topic or topics by ground topic.",
		Long: "Delete soften topic or topics by ground topic.\n" +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>",
		Example: "(1) soften-admin topics delete public/default/test\n" +
			"(2) soften-admin topics delete persistent://business/finance/equity -A",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			deleteTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().BoolVarP(&cmdArgs.partitioned, "partitioned", "P", false, constant.PartitionedUsage)
	cmd.Flags().BoolVarP(&cmdArgs.all, "all", "A", false, constant.AllUsage)

	return cmd
}

func deleteTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *deleteArgs) {
	namespaceTopic, err := util.ParseNamespaceTopic(cmdArgs.groundTopic)
	if err != nil {
		logrus.Fatalf("list \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	} else if namespaceTopic.ShortTopic == "" {
		logrus.Fatalf("list \"%s\" failed: %v\n", cmdArgs.groundTopic, "not found topic")
	}
	var topics []string
	if cmdArgs.all {
		// query topics from broker
		topics, err = queryTopicsFromBrokerByOptions(queryOptions{
			url:            rtArgs.WebUrl,
			namespaceTopic: *namespaceTopic,
			partitioned:    cmdArgs.partitioned,
		})
		if err == nil && len(topics) == 0 {
			err = errors.New("topic not existed")
		}
		if err != nil {
			logrus.Fatalf("delete \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
		}
	} else {
		// filter by options
		if mdlArgs.level != "" || mdlArgs.status != "" || mdlArgs.subscription != "" {
			topics = util.FormatTopics(namespaceTopic.FullName, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
		}
	}
	if err != nil {
		logrus.Fatalf("delete \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	}

	if len(topics) == 0 {
		logrus.Warn("Not Found")
	}
	// delete one by one
	manager := admin.NewRobustTopicManager(rtArgs.WebUrl)
	for _, topic := range topics {
		var err error
		err = manager.Delete(topic)
		if err != nil {
			logrus.Warnf("deleted \"%s\" failed: %v\n", topic, err)
		} else {
			logrus.Infof("deleted \"%s\" successfully\n", topic)
		}
	}
}
