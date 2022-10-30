package admin

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ topic admin impl ------

type nonPartitionedTopicManager struct {
	*baseTopicManger
}

func NewNonPartitionedTopicManager(url string) *nonPartitionedTopicManager {
	httpClient := &http.Client{}
	manager := &nonPartitionedTopicManager{
		baseTopicManger: &baseTopicManger{
			url:        url,
			httpclient: httpClient,
		},
	}
	return manager
}

// ------ non-partitioned topic implementation ------

func (m *nonPartitionedTopicManager) Create(topic string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s"
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *nonPartitionedTopicManager) Delete(topic string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s"
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *nonPartitionedTopicManager) List(namespace string) (topics []string, err error) {
	pathPattern := "%s/admin/v2/%s"
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

func (m *nonPartitionedTopicManager) Stats(topic string) (stats TopicStats, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}
	pathPattern := "%s/admin/v2/%s/stats"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return stats, err
	}
	err = callWithRet(m.httpclient, req, &stats)
	return stats, err
}
