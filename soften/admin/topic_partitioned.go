package admin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type partitionedTopicManager struct {
	*baseTopicManger
}

// ------ partitioned implementation ------

func NewPartitionedTopicManager(url string) *partitionedTopicManager {
	baseManger := newBaseTopicManger(url)
	return newPartitionedTopicManagerWithBaseManager(baseManger)
}

func newPartitionedTopicManagerWithBaseManager(baseManger *baseTopicManger) *partitionedTopicManager {
	return &partitionedTopicManager{
		baseTopicManger: baseManger,
	}
}

func (m *partitionedTopicManager) Create(topic string, partitions uint) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/partitions"
	partitionsJsonBytes, err := json.Marshal(partitions)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), bytes.NewReader(partitionsJsonBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *partitionedTopicManager) Delete(topic string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/partitions"
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *partitionedTopicManager) Update(topic string, partitions uint) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/partitions"
	partitionsJsonBytes, err := json.Marshal(partitions)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), bytes.NewReader(partitionsJsonBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *partitionedTopicManager) List(namespace string) (topics []string, err error) {
	pathPattern := "%s/admin/v2/%s/partitioned"
	if !strings.Contains(namespace, "://") {
		namespace = "persistent://" + namespace
	}
	restPath := strings.Replace(namespace, "://", "/", 1)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, restPath), http.NoBody)
	if err != nil {
		return topics, err
	}
	err = callWithRet(m.httpclient, req, &topics)
	return topics, err
}

func (m *partitionedTopicManager) Stats(topic string) (stats PartitionedTopicStats, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}
	pathPattern := "%s/admin/v2/%s/partitioned-stats"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return stats, err
	}
	err = callWithRet(m.httpclient, req, &stats)
	return stats, err
}

func (m *partitionedTopicManager) CreateMissedPartitions(topic string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/createMissedPartitions"
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *partitionedTopicManager) GetMetadata(topic string) (meta PartitionedTopicStatsMetadata, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}
	pathPattern := "%s/admin/v2/%s/partitions"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return meta, err
	}
	err = callWithRet(m.httpclient, req, &meta)
	return meta, err
}
