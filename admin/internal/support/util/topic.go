package util

import (
	"errors"
	"strconv"
	"strings"

	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/soften/support/util"
	"github.com/sirupsen/logrus"
)

const (
	partitionedTopicSuffix = "-partition-"
)

type NamespaceTopic struct {
	Schema         string
	Tenant         string
	Namespace      string
	shortNamespace string
	ShortTopic     string
	FullName       string
}

func ParseNamespaceTopic(namespaceOrTopic string) (*NamespaceTopic, error) {
	if namespaceOrTopic == "" {
		return nil, errors.New("empty namespace or topic is invalid")
	}
	schema := "persistent"
	// remove schema
	if ti := strings.Index(namespaceOrTopic, "://"); ti > 0 {
		schema = namespaceOrTopic[0:ti]
		namespaceOrTopic = namespaceOrTopic[ti+3:]
	}
	// parse topic and namespace
	shortTopic := ""
	namespace := ""
	tenant := ""
	shortNamespace := ""
	segments := strings.Split(namespaceOrTopic, "/")
	switch len(segments) {
	case 1:
		namespace = "public/default"
		tenant = "public"
		shortNamespace = "default"
		shortTopic = namespaceOrTopic
	case 2:
		namespace = namespaceOrTopic
		tenant = segments[0]
		shortNamespace = segments[1]
	case 3:
		namespace = strings.Join(segments[0:2], "/")
		tenant = segments[0]
		shortNamespace = segments[1]
		shortTopic = segments[2]
	default:
		namespace = strings.Join(segments[0:2], "/")
		tenant = segments[0]
		shortNamespace = segments[1]
		shortTopic = strings.Join(segments[0:2], "/")
	}
	nt := &NamespaceTopic{
		Schema:         schema,
		Tenant:         tenant,
		Namespace:      namespace,
		shortNamespace: shortNamespace,
		ShortTopic:     shortTopic,
		FullName:       schema + "://" + namespace,
	}
	if shortTopic != "" {
		nt.FullName = nt.FullName + "/" + shortTopic
	}
	return nt, nil
}

func FormatTopics(groundTopic string, levelStr, statusStr string, subscription string) []string {
	topics := make([]string, 0)
	levels := parseLevels(levelStr)
	statuses := ParseStatuses(statusStr)
	subs := ParseSubs(subscription)
	// validate subs and statuses

	topics, err := util.FormatTopics(groundTopic, levels, statuses, subs...)
	if err != nil {
		logrus.Fatal(err)
	}
	return topics
}

func IsPartitionedSubTopic(topic string) bool {
	if topic == "" {
		logrus.Fatal("invalid topic name")
	}
	if index, err := getPartitionIndex(topic); err == nil {
		return index >= 0
	}
	return false
}

func parseLevels(levelStr string) (levels message.Levels) {
	if levelStr == "" {
		levels = message.Levels{message.L1}
	} else {
		segments := strings.Split(levelStr, ",")
		for _, seg := range segments {
			l := strings.TrimSpace(seg)
			if lv, err := message.LevelOf(l); err != nil {
				logrus.Fatal(err)
			} else {
				levels = append(levels, lv)
			}
		}
	}
	return levels
}

func ParseStatuses(statusStr string) (statuses message.Statuses) {
	if statusStr == "" {
		statuses = message.Statuses{message.StatusReady}
	} else {
		segments := strings.Split(statusStr, ",")
		for _, seg := range segments {
			s := strings.TrimSpace(seg)
			if st, err := message.StatusOf(s); err != nil {
				logrus.Fatal(err)
			} else {
				statuses = append(statuses, st)
			}
		}
	}
	return
}

func ParseSubs(subStr string) (subs []string) {
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

func parseTopic(topic string) string {
	if parsedTopic, err := util.ParseTopicName(topic); err != nil {
		logrus.Fatal(err)
		return "" // only for compile error
	} else {
		return parsedTopic
	}
}

func getPartitionIndex(topic string) (int, error) {
	if strings.Contains(topic, partitionedTopicSuffix) {
		idx := strings.LastIndex(topic, "-") + 1
		return strconv.Atoi(topic[idx:])
	}
	return -1, nil
}
