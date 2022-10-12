package internal

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/shenqianjin/soften-client-go/soften/message"
)

const (
	publicTenant           = "public"
	defaultNamespace       = "default"
	partitionedTopicSuffix = "-partition-"
)

func formatTopic(topic string) string {
	// The topic name can be in two different forms, one is fully qualified topic name,
	// the other one is short topic name
	if !strings.Contains(topic, "://") {
		// The short topic name can be:
		// - <topic>
		// - <tenant>/<namespace>/<topic>
		// - <tenant>/<cluster>/<namespace>/<topic>
		parts := strings.Split(topic, "/")
		if len(parts) == 3 || len(parts) == 4 {
			topic = "persistent://" + topic
		} else if len(parts) == 1 {
			topic = "persistent://" + publicTenant + "/" + defaultNamespace + "/" + parts[0]
		} else {
			panic("Invalid short topic name '" + topic +
				"', it should be in the format of <tenant>/<namespace>/<topic> or <topic>")
		}
	}
	return topic
}

func getPartitionIndex(topic string) (int, error) {
	if strings.Contains(topic, partitionedTopicSuffix) {
		idx := strings.LastIndex(topic, "-") + 1
		return strconv.Atoi(topic[idx:])
	}
	return -1, nil
}

func formatLevels(levelStr string) (levels []string) {
	if levelStr == "" {
		levels = []string{message.L1.String()}
	} else {
		segments := strings.Split(levelStr, ",")
		for _, seg := range segments {
			l := strings.TrimSpace(seg)
			if _, err := message.LevelOf(l); err != nil {
				panic(err)
			}
			levels = append(levels, l)
		}
	}
	return levels
}

func formatStatuses(statusStr string) (statuses []string) {
	if statusStr == "" {
		statuses = []string{message.StatusReady.String()}
	} else {
		segments := strings.Split(statusStr, ",")
		for _, seg := range segments {
			s := strings.TrimSpace(seg)
			if _, err := message.StatusOf(s); err != nil {
				panic(err)
			}
			statuses = append(statuses, s)
		}
	}
	return
}

func formatSubs(subStr string) (subs []string) {
	if subStr == "" {
		subs = []string{}
	} else {
		segments := strings.Split(subStr, ",")
		for _, seg := range segments {
			s := strings.TrimSpace(seg)
			subs = append(subs, s)
		}
	}
	return
}

func formatTopics(groundTopic string, levelStr, statusStr string, subscription string) []string {
	topics := make([]string, 0)
	levels := formatLevels(levelStr)
	statuses := formatStatuses(statusStr)
	subs := formatSubs(subscription)
	// validate subs and statuses
	if len(subs) <= 0 {
		for _, s := range statuses {
			status, err := message.StatusOf(s)
			if err != nil {
				panic(err)
			}
			if status == message.StatusPending ||
				status == message.StatusBlocking ||
				status == message.StatusRetrying ||
				status == message.StatusDead {
				fmt.Printf("Error: subscription is necessary to create %s status topic\n", s)
				os.Exit(1)
			}
		}
	}
	for _, l := range levels {
		level, err := message.LevelOf(l)
		if err != nil {
			panic(err)
		}
		for _, s := range statuses {
			status, err := message.StatusOf(s)
			if err != nil {
				panic(err)
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
				for _, sub := range subs {
					topic := groundTopic + level.TopicSuffix() + "-" + sub + status.TopicSuffix()
					topics = append(topics, formatTopic(topic))

				}
			} else {
				topic := groundTopic + level.TopicSuffix() + status.TopicSuffix()
				topics = append(topics, formatTopic(topic))
			}
		}
	}
	return topics
}

func isL1Topic(topic string) bool {
	if topic == "" {
		panic("invalid topic name")
	}
	for _, l := range message.LevelValues() {
		if l == message.L1 {
			continue
		}
		if strings.HasSuffix(topic, l.TopicSuffix()) {
			return false
		}
	}
	return true
}

func isReadyTopic(topic string) bool {
	if topic == "" {
		panic("invalid topic name")
	}
	for _, s := range message.StatusValues() {
		if s == message.StatusReady {
			continue
		}
		if strings.HasSuffix(topic, s.TopicSuffix()) {
			return false
		}
	}
	return true
}

func isPartitionedSubTopic(topic string) bool {
	if topic == "" {
		panic("invalid topic name")
	}
	if index, err := getPartitionIndex(topic); err == nil {
		return index >= 0
	}
	return false
}
