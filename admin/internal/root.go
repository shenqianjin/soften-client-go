package internal

import (
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type RootArgs struct {
	Debug  bool
	WebUrl string
}

func NewRootCmd() (*cobra.Command, *RootArgs) {
	// init
	initLogger()
	// cmd
	cmdArgs := &RootArgs{}
	cmd := &cobra.Command{
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config.DebugMode = false
			if cmdArgs.Debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
		},
		Use: "soften-admin ",
		Short: "Manage resources such as topics, messages and polices of pulsar messaging system.\n" +
			"These commands is related to soften client, generally",
	}

	// parser variables
	flags := cmd.PersistentFlags()
	flags.BoolVarP(&cmdArgs.Debug, "debug", "d", false, "enable debug mode")
	flags.StringVarP(&cmdArgs.WebUrl, "web-url", "u", "http://localhost:8080", "pulsar broker http url")
	return cmd, cmdArgs
}

func initLogger() {
	formatter := &logrus.TextFormatter{
		FullTimestamp:          true,
		TimestampFormat:        "2006-01-02T15:04:05.000000Z07:00",
		DisableLevelTruncation: true,
		PadLevelText:           true,
	}
	logrus.SetFormatter(formatter)
}
