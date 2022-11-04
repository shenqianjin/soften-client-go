package util

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/meta"
)

func ParseTopicName(topic string) (string, error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return "", err
	} else {
		return parsedTopic.Name, nil
	}
}

func FormatTopics(groundTopic string, levels []string, statuses []string, subscription string) ([]string, error) {
	topics := make([]string, 0)
	if groundTopic == "" {
		return topics, errors.New("ground topic is empty")
	}
	if len(levels) < 1 {
		return topics, errors.New("levels is empty")
	}
	// validate subscription and statuses
	if subscription == "" {
		for _, s := range statuses {
			status, err := message.StatusOf(s)
			if err != nil {
				return topics, err
			}
			if status == message.StatusPending ||
				status == message.StatusBlocking ||
				status == message.StatusRetrying ||
				status == message.StatusDead {
				return topics, errors.New(fmt.Sprintf("subscription is missing for %s status topic", s))
			}
		}
	}
	// start to format topics
	for _, l := range levels {
		level, err := message.LevelOf(l)
		if err != nil {
			return topics, err
		}
		for _, s := range statuses {
			status, err1 := message.StatusOf(s)
			if err1 != nil {
				return topics, err1
			}
			subRequired := false
			if status == message.StatusDead {
				if level == message.L1 {
					subRequired = true
				} else {
					// skip non-L1 for dead status
					continue
				}
			} else if status == message.StatusPending ||
				status == message.StatusBlocking ||
				status == message.StatusRetrying {
				subRequired = true
			}
			if subRequired {
				topic := groundTopic + level.TopicSuffix() + "-" + subscription + status.TopicSuffix()
				topics = append(topics, topic)
			} else {
				topic := groundTopic + level.TopicSuffix() + status.TopicSuffix()
				topics = append(topics, topic)
			}
		}
	}
	return topics, nil
}

func FormatDeadTopic(groundTopic string, subscription string) (string, error) {
	if groundTopic == "" {
		return "", errors.New("ground topic is empty")
	}

	if subscription == "" {
		return "", errors.New("subscription is missing for dead status topic")
	}
	return groundTopic + message.L1.TopicSuffix() + "-" + subscription + message.StatusDead.TopicSuffix(), nil
}

// ------ logger util ------

func ParseLogEntry(ctx context.Context, logger log.Logger) log.Entry {
	reqId := meta.GetMeta(ctx, meta.KeyReqId)
	var logEntry log.Entry = logger
	if reqId != "" {
		logEntry = logEntry.WithField(meta.KeyReqId, reqId)
	}
	return logEntry
}
