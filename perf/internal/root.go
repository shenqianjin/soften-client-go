package internal

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/shenqianjin/soften-client-go/perf/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/support/log"
	"github.com/shenqianjin/soften-client-go/soften/support/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type RootArgs struct {
	util.ClientArgs

	ProfilePort    int
	PrometheusPort int
	Debug          bool

	Ctx context.Context
}

func NewRootCommand() (*cobra.Command, *RootArgs) {
	rtArgs := &RootArgs{}
	cmd := &cobra.Command{
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config.DebugMode = false
			// context
			var cancelFunc context.CancelFunc
			rtArgs.Ctx, cancelFunc = context.WithCancel(context.Background())
			// log
			initLogger(rtArgs.Debug)
			// start metrics
			go RunMetrics(rtArgs.Ctx, rtArgs.PrometheusPort)
			// start profile
			go RunProfiling(rtArgs.Ctx, rtArgs.ProfilePort)
			// Handle SIGINT and SIGTERM.
			go func() {
				ch := make(chan os.Signal)
				signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
				<-ch
				cancelFunc()
				logrus.Println("soften performing exiting")
			}()
		},

		Use: "soften-perf",
	}
	// load args here
	cmd.PersistentFlags().IntVar(&rtArgs.ProfilePort, "profile-port", 6060, "Port to expose profiling info, use -1 to disable")
	cmd.PersistentFlags().IntVar(&rtArgs.PrometheusPort, "metrics-port", 8000, "Port to use to export metrics for Prometheus. Use -1 to disable.")
	cmd.PersistentFlags().BoolVar(&rtArgs.Debug, "debug", false, "enable debug output")
	// consume client
	cmd.PersistentFlags().StringVarP(&rtArgs.ClientArgs.BrokerUrl, "broker-url", "u", "pulsar://localhost:6650", "The Pulsar service URL")
	cmd.PersistentFlags().StringVar(&rtArgs.ClientArgs.TokenFile, "token-file", "", "file path to the Pulsar JWT file")
	cmd.PersistentFlags().StringVar(&rtArgs.ClientArgs.TLSTrustCertFile, "trust-cert-file", "", "file path to the trusted certificate file")
	// auto create topic flag
	cmd.PersistentFlags().BoolVar(&rtArgs.ClientArgs.AutoCreateTopic, "auto-create-topic", false, "switch to create topic automatically. fatal will happen if your broker disables allowAutoTopicCreation")
	cmd.PersistentFlags().StringVar(&rtArgs.ClientArgs.WebUrl, "web-url", "http://localhost:8080", "The Pulsar HTTP service URL. It is optional if enable --auto-create-topic")

	return cmd, rtArgs
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

func RunMetrics(ctx context.Context, port int) {
	if port > 0 {
		mux := http.NewServeMux()
		s := http.Server{
			Addr:    "0.0.0.0:" + strconv.Itoa(port),
			Handler: mux,
		}

		mux.Handle("/metrics", promhttp.Handler())
		if err := s.ListenAndServe(); err != nil {
			logrus.Fatalf("Unable to start prometheus metrics server. err: %v", err)
		}

		<-ctx.Done()
		logrus.Infof("Shutting down metrics server")
		_ = s.Shutdown(context.Background())
	}
}

func RunProfiling(cxt context.Context, port int) {
	if port > 0 {
		if err := serveProfiling(cxt, "0.0.0.0:"+strconv.Itoa(port)); err != nil && err != http.ErrServerClosed {
			logrus.WithError(err).Error("Unable to start debug profiling server")
		}
	}
}

// use `http://addr/debug/pprof` to access the browser
// use `go tool pprof http://addr/debug/pprof/profile` to get pprof file(cpu info)
// use `go tool pprof http://addr/debug/pprof/heap` to get inuse_space file
func serveProfiling(ctx context.Context, addr string) error {
	s := http.Server{
		Addr:    addr,
		Handler: http.DefaultServeMux, // /debug/pprof 相关的handler import -> init 写到了这里
	}

	fmt.Printf("Starting pprof server at: %s\n", addr)
	fmt.Printf("  use `http://%s/debug/pprof` to access the browser\n", addr)
	fmt.Printf("  use `go tool pprof http://%s/debug/pprof/profile` to get pprof file(cpu info)\n", addr)
	fmt.Printf("  use `go tool pprof http://%s/debug/pprof/heap` to get inuse_space file\n", addr)
	fmt.Println()

	if err := s.ListenAndServe(); err != nil {
		return err
	}

	<-ctx.Done()
	logrus.Infof("Shutting down pprof server")
	_ = s.Shutdown(context.Background())
	return nil
}
