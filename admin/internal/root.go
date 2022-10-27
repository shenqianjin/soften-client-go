package internal

import (
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/spf13/cobra"
)

type RootArgs struct {
	Debug bool
	Url   string
}

func NewRootCmd() (*cobra.Command, RootArgs) {
	cmdArgs := RootArgs{}
	cmd := &cobra.Command{
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config.DebugMode = cmdArgs.Debug
		},
		Use: "soften-admin",
	}

	// parser variables
	flags := cmd.PersistentFlags()
	flags.BoolVarP(&cmdArgs.Debug, "debug", "d", false, "enable debug mode")
	flags.StringVarP(&cmdArgs.Url, "url", "u", "http://localhost:8080", "pulsar broker http url")
	return cmd, cmdArgs
}
