package topics

import (
	"fmt"
	"sort"
	"strings"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
)

type listArgs struct {
	namespaceOrTopic string
	partitioned      bool
	all              bool
}

func newListCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &listArgs{}
	cmd := &cobra.Command{
		Use:   "list ",
		Short: "List soften topic or topics by namespace or ground topic.",
		Long: "List soften topic or topics by namespace or ground topic.\n" +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <schema>://<tenant>/<namespace>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>",
		Example: "(1) soften-admin topics list public/default\n" +
			"(2) soften-admin topics list test03 -P\n" +
			"(3) soften-admin topics list persistent://business/finance/equity -P",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.namespaceOrTopic = args[0]
			listTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}

	flags := cmd.Flags()
	// parse partition
	flags.BoolVarP(&cmdArgs.partitioned, "partitioned", "P", false, constant.PartitionedUsage)
	cmd.Flags().BoolVarP(&cmdArgs.all, "all", "a", false, constant.AllUsage)

	return cmd
}

func listTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *listArgs) {
	namespaceTopic, err := util.ParseNamespaceTopic(cmdArgs.namespaceOrTopic)
	if err != nil {
		logrus.Fatalf("list \"%s\" failed: %v\n", cmdArgs.namespaceOrTopic, err)
	}
	topics, err := queryTopicsFromBrokerByOptions(queryOptions{
		url:            rtArgs.WebUrl,
		namespaceTopic: *namespaceTopic,
		partitioned:    cmdArgs.partitioned,
	})
	if err != nil {
		logrus.Fatalf("list \"%s\" failed: %v\n", cmdArgs.namespaceOrTopic, err)
	}
	// filter by options
	if !cmdArgs.all && namespaceTopic.ShortTopic != "" {
		//if mdlArgs.level != "" || mdlArgs.status != "" || mdlArgs.subscription != "" {
		matchedTopics := make([]string, 0)
		expectedTopics := util.FormatTopics(namespaceTopic.FullName, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
		for _, t := range expectedTopics {
			if slices.Contains(topics, t) {
				matchedTopics = append(matchedTopics, t)
			}
		}
		topics = matchedTopics
		//}
	}

	if len(topics) == 0 {
		logrus.Warn("Not Found")
	}
	sort.Strings(topics)
	for _, topic := range topics {
		fmt.Println(topic)
	}
}

type queryOptions struct {
	url            string
	namespaceTopic util.NamespaceTopic
	partitioned    bool
}

func queryTopicsFromBrokerByOptions(options queryOptions) ([]string, error) {
	// query topics from broker
	var queriedTopics []string
	var err error
	if options.partitioned {
		queriedTopics, err = admin.NewPartitionedTopicManager(options.url).List(options.namespaceTopic.Namespace)
	} else {
		queriedTopics, err = admin.NewNonPartitionedTopicManager(options.url).List(options.namespaceTopic.Namespace)
	}
	if err != nil {
		logrus.Fatalln(err)
	}

	// match by ground topic (may namespace as well)
	matchedTopics := make([]string, 0)
	for _, topic := range queriedTopics {
		if strings.HasPrefix(topic, options.namespaceTopic.FullName) {
			matchedTopics = append(matchedTopics, topic)
		}
	}

	// exclude partitioned sub topics for non-partitioned option
	if !options.partitioned {
		newTopics := make([]string, 0)
		for _, topic := range matchedTopics {
			if !util.IsPartitionedSubTopic(topic) {
				newTopics = append(newTopics, topic)
			}
		}
		matchedTopics = newTopics
	}

	return matchedTopics, err
}
