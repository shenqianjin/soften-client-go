package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"

	"github.com/shenqianjin/soften-client-go/soften/log"

	"github.com/sirupsen/logrus"
)

func main() {
	// init commands
	rootCmd, perf, cliArgs := newPerfCommand()
	rootCmd.AddCommand(newProducerCommand(perf, cliArgs))
	rootCmd.AddCommand(newConsumerCommand(perf, cliArgs))
	rootCmd.AddCommand(newProduceConsumeCommand(perf, cliArgs))

	// start performer
	go perf.Start()

	// start execute
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "executing command error=%+v\n", err)
		os.Exit(1)
	}
}

func initLogger(debug bool) {
	formatter := &logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006/01/02 15:04:05.000000",
	}
	softenLogFormatter := log.NewTextFormatter(formatter)
	//softenLogFormatter := formatter

	logrus.SetFormatter(softenLogFormatter)
	level := logrus.InfoLevel
	if debug {
		level = logrus.DebugLevel
	}
	logrus.SetReportCaller(true)
	logrus.SetLevel(level)
}
