package messages

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/spf13/cobra"
)

type messagesArgs struct {
	BrokerUrl string
}

func NewModuleCmd(rootArgs *internal.RootArgs) *cobra.Command {
	moduleArgs := &messagesArgs{}
	cmd := &cobra.Command{
		Use:   "messages ",
		Short: "sub command to manage messages",
	}

	// parser variables
	flags := cmd.PersistentFlags()
	flags.StringVar(&moduleArgs.BrokerUrl, "broker-url", "pulsar://localhost:6650", "broker url")

	// add action commands
	cmd.AddCommand(newRecallCommand(rootArgs, moduleArgs))
	cmd.AddCommand(newIterateCommand(rootArgs, moduleArgs))

	return cmd
}
