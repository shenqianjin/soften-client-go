package util

import "github.com/shenqianjin/soften-client-go/soften/internal"

func ParseTopicName(topic string) (string, error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return "", err
	} else {
		return parsedTopic.Name, nil
	}
}
