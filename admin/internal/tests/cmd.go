package tests

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/spf13/cobra"
)

type testsArgs struct {
	BrokerUrl string
	topic     string
}

func NewModuleCmd(rootArgs *internal.RootArgs) *cobra.Command {
	moduleArgs := &testsArgs{}
	cmd := &cobra.Command{
		Use:   "tests ",
		Short: "Tests module to process messages",
	}

	// parser variables
	flags := cmd.PersistentFlags()
	flags.StringVar(&moduleArgs.BrokerUrl, "broker-url", "pulsar://localhost:6650", "broker url")

	// add action commands
	cmd.AddCommand(newProduceCommand(rootArgs, moduleArgs))

	return cmd
}
