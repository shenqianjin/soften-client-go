package main

import (
	"github.com/shenqianjin/soften-client-go/admin/internal"
	"github.com/shenqianjin/soften-client-go/admin/internal/messages"
	"github.com/shenqianjin/soften-client-go/admin/internal/tests"
	"github.com/shenqianjin/soften-client-go/admin/internal/topics"
	"github.com/sirupsen/logrus"
)

func main() {
	// root commands
	rootCmd, rootArgs := internal.NewRootCmd()

	// add module commands
	rootCmd.AddCommand(topics.NewModuleCmd(rootArgs))
	rootCmd.AddCommand(messages.NewModuleCmd(rootArgs))
	rootCmd.AddCommand(tests.NewModuleCmd(rootArgs))

	// execute
	err := rootCmd.Execute()
	if err != nil {
		logrus.Fatalf("executing command error=%+v\n", err)
	}
}
