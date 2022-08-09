package main

import (
	"sync"

	"github.com/spf13/cobra"
)

func newPerfCommand() (*cobra.Command, *performer, *clientArgs) {
	var perf *performer
	pfArgs := &perfArgs{}
	cliArgs := &clientArgs{}
	rootCmd := &cobra.Command{
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			initLogger(pfArgs.flagDebug)
		},

		Use: "soften-perf-go",
	}
	flags := rootCmd.PersistentFlags()
	// load args here
	flagLoader.loadPerfFlags(flags, pfArgs, cliArgs)
	perf = newPerformer(pfArgs)
	return rootCmd, perf, cliArgs
}

func newConsumerCommand(perf *performer, cliArgs *clientArgs) *cobra.Command {
	cArgs := &consumeArgs{}
	cmd := &cobra.Command{
		Use:   "consume <topic>",
		Short: "Consume from topic",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cArgs.Topic = args[0]
			c := newConsumer(cliArgs, cArgs)
			c.perfConsume(perf.initStopCh())
		},
	}
	flags := cmd.Flags()
	// load flags here
	flagLoader.loadConsumeFlags(flags, cArgs)

	return cmd
}

func newProducerCommand(perf *performer, cliArgs *clientArgs) *cobra.Command {
	pArgs := &produceArgs{}
	cmd := &cobra.Command{
		Use:   "produce ",
		Short: "Produce on a topic and measure performance",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p := newProducer(cliArgs, pArgs)
			pArgs.Topic = args[0]
			p.perfProduce(perf.initStopCh())
		},
	}
	flags := cmd.Flags()
	// load flags here
	flagLoader.loadProduceFlags(flags, pArgs)
	return cmd
}

func newProduceConsumeCommand(perf *performer, cliArgs *clientArgs) *cobra.Command {
	pArgs := &produceArgs{}
	cArgs := &consumeArgs{}
	cmd := &cobra.Command{
		Use:   "produce-consume ",
		Short: "Both produce and consume on a topic and measure performance",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p := newProducer(cliArgs, pArgs)
			c := newConsumer(cliArgs, cArgs)
			group := sync.WaitGroup{}
			group.Add(2)
			// produce
			go func() {
				pArgs.Topic = args[0]
				p.perfProduce(perf.initStopCh())
				group.Done()
			}()
			// consume
			go func() {
				cArgs.Topic = args[0]
				c.perfConsume(perf.initStopCh())
				group.Done()
			}()
			group.Wait()
		},
	}
	flags := cmd.Flags()
	// load flags here
	flagLoader.loadProduceFlags(flags, pArgs)
	flagLoader.loadConsumeFlags(flags, cArgs)
	return cmd
}
