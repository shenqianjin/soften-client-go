package messages

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/util"
	"github.com/spf13/cobra"
)

type recallArgs struct {
	srcTopic  string
	destTopic string

	condition string
}

func newRecallCommand(rtArgs internal.RootArgs) *cobra.Command {
	cmdArgs := &recallArgs{}
	cmd := &cobra.Command{
		Use:   "recall ",
		Short: "delete soften topic or topics",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs.srcTopic = args[0]
			cmdArgs.destTopic = args[1]
			recallMessages(rtArgs, cmdArgs)
		},
	}
	// parse variables
	cmd.Flags().StringVarP(&cmdArgs.condition, "conditions", "c", "", util.ConditionsUsage)

	return cmd
}

func recallMessages(rtArgs internal.RootArgs, cmdArgs *recallArgs) {
	//manager := admin.NewTopicManager(rtArgs.Url)

	/*if len(destTopics) != 1 {
		err := errors.New("more than onre destination topics parsed from your dest options")
		fmt.Printf("recall \"%s\" to \"%s\" failed: %v\n", cmdArgs.srcGroundTopic, cmdArgs.destGroundTopic, err)
	}
	for _, topic := range srcTopics {
		// recall
		var err error
		if err != nil {
			fmt.Printf("recall \"%s\" to \"%s\" failed: %v\n", topic, err)
		} else {
			fmt.Printf("recall \"%s\" to \"%s\" successfully\n", topic)
		}
	}*/
}

func recallMessagesByContitions() {

}
