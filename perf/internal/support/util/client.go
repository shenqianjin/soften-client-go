package util

import (
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/sirupsen/logrus"
)

type ClientArgs struct {
	BrokerUrl        string
	TokenFile        string
	TLSTrustCertFile string

	AutoCreateTopic bool
	WebUrl          string
}

func NewClient(clientArgs *ClientArgs) (soften.Client, error) {
	clientOpts := config.ClientConfig{
		URL: clientArgs.BrokerUrl,
	}
	if clientArgs.TokenFile != "" {
		// read JWT from the file
		tokenBytes, err := os.ReadFile(clientArgs.TokenFile)
		if err != nil {
			logrus.WithError(err).Errorf("failed to read Pulsar JWT from a file %s", clientArgs.TokenFile)
			os.Exit(1)
		}
		clientOpts.Authentication = pulsar.NewAuthenticationToken(string(tokenBytes))
	}

	if clientArgs.TLSTrustCertFile != "" {
		clientOpts.TLSTrustCertsFilePath = clientArgs.TLSTrustCertFile
	}
	return soften.NewClient(clientOpts)
}
