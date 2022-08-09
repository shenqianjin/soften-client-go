package main

import (
	"io/ioutil"
	"os"

	"github.com/shenqianjin/soften-client-go/soften/config"

	"github.com/shenqianjin/soften-client-go/soften"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

type clientArgs struct {
	ServiceURL       string
	TokenFile        string
	TLSTrustCertFile string
}

func newClient(clientArgs *clientArgs) (soften.Client, error) {
	clientOpts := config.ClientConfig{
		URL: clientArgs.ServiceURL,
	}
	//log.SetLevel(log.DebugLevel)

	if clientArgs.TokenFile != "" {
		// read JWT from the file
		tokenBytes, err := ioutil.ReadFile(clientArgs.TokenFile)
		if err != nil {
			log.WithError(err).Errorf("failed to read Pulsar JWT from a file %s", clientArgs.TokenFile)
			os.Exit(1)
		}
		clientOpts.Authentication = pulsar.NewAuthenticationToken(string(tokenBytes))
	}

	if clientArgs.TLSTrustCertFile != "" {
		clientOpts.TLSTrustCertsFilePath = clientArgs.TLSTrustCertFile
	}
	return soften.NewClient(clientOpts)
}
