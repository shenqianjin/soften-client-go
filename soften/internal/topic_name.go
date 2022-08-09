package internal

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// TopicName abstract a struct contained in a Topic
type TopicName struct {
	Domain    string
	Tenant    string
	Namespace string
	Topic     string
	Name      string
	Partition int
}

const (
	publicTenant           = "public"
	defaultNamespace       = "default"
	partitionedTopicSuffix = "-partition-"
)

// ParseTopicName parse the given topic name and return TopicName.
func ParseTopicName(topic string) (*TopicName, error) {
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
			return nil, errors.New(
				"Invalid short topic name '" + topic +
					"', it should be in the format of <tenant>/<namespace>/<topic> or <topic>")
		}
	}

	tn := &TopicName{}

	// The fully qualified topic name can be in two different forms:
	// new:    persistent://tenant/namespace/topic
	// legacy: persistent://tenant/cluster/namespace/topic
	parts := strings.SplitN(topic, "://", 2)
	domain := parts[0]
	if domain != "persistent" && domain != "non-persistent" {
		return nil, errors.New("Invalid topic domain: " + domain)
	}
	tn.Domain = domain

	rest := parts[1]
	var err error

	// The rest of the name can be in different forms:
	// new:    tenant/namespace/<localName>
	// legacy: tenant/cluster/namespace/<localName>
	// Examples of localName:
	// 1. some/name/xyz//
	// 2. /xyz-123/feeder-2
	parts = strings.SplitN(rest, "/", 4)
	tn.Tenant = parts[0]
	if len(parts) == 3 {
		// New topic name without cluster name
		tn.Namespace = parts[0] + "/" + parts[1]
		tn.Topic = parts[2]
	} else if len(parts) == 4 {
		// Legacy topic name that includes cluster name
		tn.Namespace = fmt.Sprintf("%s/%s/%s", parts[0], parts[1], parts[2])
		tn.Topic = parts[3]
	} else {
		return nil, errors.New("Invalid topic name: " + topic)
	}

	tn.Name = topic
	tn.Partition, err = getPartitionIndex(topic)
	if err != nil {
		return nil, err
	}

	return tn, nil
}

func (tn *TopicName) TopicNameWithoutPartitionPart() string {
	if tn.Partition < 0 {
		return tn.Name
	}
	idx := strings.LastIndex(tn.Name, partitionedTopicSuffix)
	if idx > 0 {
		return tn.Name[:idx]
	}
	return tn.Name
}

func getPartitionIndex(topic string) (int, error) {
	if strings.Contains(topic, partitionedTopicSuffix) {
		idx := strings.LastIndex(topic, "-") + 1
		return strconv.Atoi(topic[idx:])
	}
	return -1, nil
}

func (tn *TopicName) IsV2TopicName() bool {
	parts := strings.Split(tn.Namespace, "/")
	// Legacy topic name that includes cluster name
	// tn.Namespace = fmt.Sprintf("%s/%s/%s", parts[0], parts[1], parts[2])
	return len(parts) != 3
}

func (tn *TopicName) GetTopicRestPath() string {
	return fmt.Sprintf("%s/%s/%s", tn.Domain, tn.Namespace, url.QueryEscape(tn.Topic))
}
