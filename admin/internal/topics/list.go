package topics

import (
	"fmt"
	"sort"
	"strings"

	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
)

type listArgs struct {
	groundTopic string
	partitioned bool
	all         bool
}

func newListCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &listArgs{}
	cmd := &cobra.Command{
		Use:   "list ",
		Short: "List soften topic or topics (namespace or ground topic).",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			listTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}

	flags := cmd.Flags()
	// parse partition
	flags.BoolVarP(&cmdArgs.partitioned, "partitioned", "P", false, util.PartitionedUsage)
	flags.BoolVarP(&cmdArgs.all, "all", "A", false, util.AllUsage)

	return cmd
}

func listTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *listArgs) {
	topics, err := queryTopicsFromBrokerByOptions(queryOptions{
		url:         rtArgs.Url,
		groundTopic: cmdArgs.groundTopic,
		partitioned: cmdArgs.partitioned,
	})
	if err != nil {
		logrus.Fatalf("list \"%s\" failed: %v\n", cmdArgs.groundTopic, err)
	}
	// filter by options
	if !cmdArgs.all {
		if mdlArgs.level != "" || mdlArgs.status != "" || mdlArgs.subscription != "" {
			matchedTopics := make([]string, 0)
			expectedTopics := util.FormatTopics(cmdArgs.groundTopic, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
			for _, t := range expectedTopics {
				if slices.Contains(topics, t) {
					matchedTopics = append(matchedTopics, t)
				}
			}
			topics = matchedTopics
		}
	}

	sort.Strings(topics)
	for _, topic := range topics {
		fmt.Println(topic)
	}
}

type queryOptions struct {
	url string

	groundTopic string
	partitioned bool
}

func queryTopicsFromBrokerByOptions(options queryOptions) ([]string, error) {
	namespace := "public/default"
	// remove schema
	if ti := strings.Index(options.groundTopic, "://"); ti > 0 {
		namespace = options.groundTopic[ti+3:]
	}
	// remove raw topic name
	if strings.Contains(namespace, "/") {
		segments := strings.Split(namespace, "/")
		if len(segments) > 2 {
			namespace = strings.Join(segments[0:1], "/")
		}
	}
	// query topics from broker
	var queriedTopics []string
	var err error
	if options.partitioned {
		queriedTopics, err = admin.NewPartitionedTopicManager(options.url).List(namespace)
	} else {
		queriedTopics, err = admin.NewNonPartitionedTopicManager(options.url).List(namespace)
	}
	if err != nil {
		logrus.Fatalln(err)
	}

	// match by ground topic (may namespace as well)
	matchedTopics := make([]string, 0)
	for _, topic := range queriedTopics {
		if strings.Contains(topic, options.groundTopic) {
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
