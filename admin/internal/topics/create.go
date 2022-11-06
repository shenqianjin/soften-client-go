package topics

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/namespaces"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/constant"
	"github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/shenqianjin/soften-client-go/admin/internal/tenants"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type createArgs struct {
	groundTopic string
	partitioned bool
	partitions  uint

	autoCreateTenant    bool
	autoCreateNamespace bool
}

func newCreateCommand(rtArgs *internal.RootArgs, mdlArgs *topicsArgs) *cobra.Command {
	cmdArgs := &createArgs{}
	cmd := &cobra.Command{
		Use:   "create ",
		Short: "Create soften topic or topics based-on ground topic.",
		Long: "Create soften topic or topics based-on ground topic.\n" +
			"\n" +
			"Exact 1 argument like the below format is necessary: \n" +
			"  <schema>://<tenant>/<namespace>/<topic>\n" +
			"  <tenant>/<namespace>/<topic>\n" +
			"  <topic>",
		Example: "(1) soften-admin topics create test\n" +
			"(2) soften-admin topics create public/default/test -P\n" +
			"(3) soften-admin topics create persistent://business/finance/equity -Pp 8",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.groundTopic = args[0]
			createTopics(rtArgs, mdlArgs, cmdArgs)
		},
	}

	// parse partition
	cmd.Flags().BoolVarP(&cmdArgs.partitioned, "partitioned", "P", false, constant.PartitionedUsage)
	cmd.Flags().UintVarP(&cmdArgs.partitions, "partitions", "p", 1, constant.PartitionsUsage4Create)
	cmd.Flags().BoolVar(&cmdArgs.autoCreateTenant, "auto-create-tenant", false, "auto create tenant if missing")
	cmd.Flags().BoolVar(&cmdArgs.autoCreateNamespace, "auto-create-namespace", false, "auto create tenant if missing")

	return cmd
}

func createTopics(rtArgs *internal.RootArgs, mdlArgs *topicsArgs, cmdArgs *createArgs) {
	namespaceTopic, err := util.ParseNamespaceTopic(cmdArgs.groundTopic)
	if err != nil {
		logrus.Fatal(err)
	}
	if cmdArgs.autoCreateTenant {
		tenants.CreateIfNotExist(rtArgs.Url, namespaceTopic.Tenant)
	}
	if cmdArgs.autoCreateNamespace {
		namespaces.CreateIfNotExist(rtArgs.Url, namespaceTopic.Namespace)
	}
	// format and validate topics
	topics := util.FormatTopics(cmdArgs.groundTopic, mdlArgs.level, mdlArgs.status, mdlArgs.subscription)
	// validate partitions
	if cmdArgs.partitioned {
		if cmdArgs.partitions < 1 {
			logrus.Fatalf("the number of partitioned topics must be equal or larger than 1")
		}
	} else {
		if cmdArgs.partitions != 1 {
			logrus.Fatalf("the number of non-partitioned topics must be 1")
		}
	}
	// create
	manager := admin.NewRobustTopicManager(rtArgs.Url)
	for _, topic := range topics {
		var err error
		err = manager.Create(topic, cmdArgs.partitions)
		if err != nil {
			logrus.Warnf("created \"%s\" failed: %v\n", topic, err)
		} else {
			logrus.Infof("created \"%s\" successfully\n", topic)
		}
	}
}
