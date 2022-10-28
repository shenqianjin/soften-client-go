package util

import "github.com/shenqianjin/soften-client-go/soften/internal"

func ParseTopicName(topic string) (string, error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	return parsedTopic.Name, err
}
