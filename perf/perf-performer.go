package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"

	"github.com/shenqianjin/soften-client-go/soften/support/promhttp"
	//"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

type perfArgs struct {
	flagDebug      bool
	profilePort    int
	PrometheusPort int
}

type performer struct {
	pArgs *perfArgs
	//stopCh <-chan struct{}
}

func newPerformer(pArgs *perfArgs) *performer {
	p := &performer{pArgs: pArgs}
	//p.stopCh = p.initStopCh()
	return p
}

func (p *performer) initStopCh() <-chan struct{} {
	stop := make(chan struct{})
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		close(stop)
	}()
	return stop
}

func (p *performer) Start() {
	p.RunMetrics()
	p.RunProfiling()
}

func (p *performer) RunMetrics() {
	if p.pArgs.PrometheusPort > 0 {
		if err := p.serverMetrics("0.0.0.0:"+strconv.Itoa(p.pArgs.PrometheusPort), p.initStopCh()); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Error("Unable to start prometheus metrics server")
		}
	}
}

func (p *performer) serverMetrics(addr string, stop <-chan struct{}) error {
	mux := http.NewServeMux()
	s := http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-stop
		log.Infof("Shutting down metrics server")
		_ = s.Shutdown(context.Background())
	}()

	log.Info("Starting Prometheus metrics at http://localhost:", p.pArgs.PrometheusPort, "/metrics")
	mux.Handle("/metrics", promhttp.Handler())
	return s.ListenAndServe()
	//http.ListenAndServe(":"+strconv.Itoa(p.pArgs.PrometheusPort), mux)
}

func (p *performer) RunProfiling() {
	if p.pArgs.profilePort > 0 {
		if err := p.serveProfiling("0.0.0.0:"+strconv.Itoa(p.pArgs.profilePort), p.initStopCh()); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Error("Unable to start debug profiling server")
		}
	}
}

// use `http://addr/debug/pprof` to access the browser
// use `go tool pprof http://addr/debug/pprof/profile` to get pprof file(cpu info)
// use `go tool pprof http://addr/debug/pprof/heap` to get inuse_space file
func (p *performer) serveProfiling(addr string, stop <-chan struct{}) error {
	s := http.Server{
		Addr:    addr,
		Handler: http.DefaultServeMux, // /debug/pprof 相关的handler import -> init 写到了这里
	}
	go func() {
		<-stop
		log.Infof("Shutting down pprof server")
		_ = s.Shutdown(context.Background())
	}()

	fmt.Printf("Starting pprof server at: %s\n", addr)
	fmt.Printf("  use `http://%s/debug/pprof` to access the browser\n", addr)
	fmt.Printf("  use `go tool pprof http://%s/debug/pprof/profile` to get pprof file(cpu info)\n", addr)
	fmt.Printf("  use `go tool pprof http://%s/debug/pprof/heap` to get inuse_space file\n", addr)
	fmt.Println()

	return s.ListenAndServe()
}
