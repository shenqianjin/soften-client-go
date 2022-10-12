package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strings"

	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ topic admin interface ------

type BaseTopicAdmin interface {
	Stats(topic string) (stats TopicStats, err error)
	StatsInternal(topic string) (stats TopicStatsInternal, err error)
	Unload(topic string) error
}

type NonPartitionedTopicAdmin interface {
	Create(topic string) error
	Delete(topic string) error
	List(namespace string) (topics []string, err error)
}

type PartitionedTopicAdmin interface {
	PartitionedCreate(topic string, partitions uint) error
	PartitionedCreateMissedPartitions(topic string) error
	PartitionedDelete(topic string) error
	PartitionedUpdate(topic string, partitions uint) error
	PartitionedList(namespace string) (topics []string, err error)
	PartitionedStats(topic string) (stats PartitionedTopicStats, err error)
	GetMetadata(topic string) (meta PartitionedTopicStatsMetadata, err error)
}

// ------ topic admin impl ------

type topicAdminImpl struct {
	httpclient *http.Client
	url        string
}

func NewTopicManager(url string) *topicAdminImpl {
	manager := &topicAdminImpl{
		url:        url,
		httpclient: &http.Client{},
	}
	return manager
}

func (m *topicAdminImpl) callWithRet(c *http.Client, req *http.Request, ret interface{}) error {
	if config.DebugMode {
		reqBytes, dumpErr := httputil.DumpRequestOut(req, true)
		fmt.Println(string(reqBytes), dumpErr)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	if config.DebugMode {
		respBytes, dumpErr := httputil.DumpResponse(resp, resp.ContentLength > 0)
		fmt.Println(string(respBytes), dumpErr)
	}
	// success
	if resp.StatusCode/100 == 2 {
		if ret != nil {
			if err1 := json.NewDecoder(resp.Body).Decode(ret); err1 != nil {
				return err1
			}
		}
		return nil
	}
	// failed
	if resp.ContentLength != 0 {
		if respData, err1 := ioutil.ReadAll(resp.Body); err1 != nil {
			err = err1
		} else {
			err = errors.New(fmt.Sprintf("%s => %s", resp.Status, string(respData)))
		}
	} else {
		err = errors.New(fmt.Sprintf("%s", resp.Status))
	}
	return err
}

// ------ base implementation ------

func (m *topicAdminImpl) StatsInternal(topic string) (stats TopicStatsInternal, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}
	pathPattern := "%s/admin/v2/%s/internalStats"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return stats, err
	}
	err = m.callWithRet(m.httpclient, req, &stats)
	return stats, err
}

func (m *topicAdminImpl) Unload(topic string) error {
	pathPattern := "%s/admin/v2/%s/unload"
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, topic), http.NoBody)
	if err != nil {
		return err
	}
	err = m.callWithRet(m.httpclient, req, nil)
	return err
}

// ------ non-partitioned topic implementation ------

func (m *topicAdminImpl) Create(topic string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s"
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = m.callWithRet(m.httpclient, req, nil)
	return err
}

func (m *topicAdminImpl) Delete(topic string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s"
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = m.callWithRet(m.httpclient, req, nil)
	return err
}

func (m *topicAdminImpl) List(namespace string) (topics []string, err error) {
	pathPattern := "%s/admin/v2/%s"
	if !strings.Contains(namespace, "://") {
		namespace = "persistent://" + namespace
	}
	restPath := strings.Replace(namespace, "://", "/", 1)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, restPath), http.NoBody)
	if err != nil {
		return topics, err
	}
	err = m.callWithRet(m.httpclient, req, &topics)
	return topics, err
}

func (m *topicAdminImpl) Stats(topic string) (stats TopicStats, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}
	pathPattern := "%s/admin/v2/%s/stats"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return stats, err
	}
	err = m.callWithRet(m.httpclient, req, &stats)
	return stats, err
}

// ------ partitioned implementation ------

func (m *topicAdminImpl) PartitionedCreate(topic string, partitions uint) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/partitions"
	json, err := json.Marshal(partitions)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), bytes.NewReader(json))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	err = m.callWithRet(m.httpclient, req, nil)
	return err
}

func (m *topicAdminImpl) PartitionedDelete(topic string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/partitions"
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = m.callWithRet(m.httpclient, req, nil)
	return err
}

func (m *topicAdminImpl) PartitionedUpdate(topic string, partitions uint) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/partitions"
	json, err := json.Marshal(partitions)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), bytes.NewReader(json))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	err = m.callWithRet(m.httpclient, req, nil)
	return err
}

func (m *topicAdminImpl) PartitionedList(namespace string) (topics []string, err error) {
	pathPattern := "%s/admin/v2/%s/partitioned"
	if !strings.Contains(namespace, "://") {
		namespace = "persistent://" + namespace
	}
	restPath := strings.Replace(namespace, "://", "/", 1)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, restPath), http.NoBody)
	if err != nil {
		return topics, err
	}
	err = m.callWithRet(m.httpclient, req, &topics)
	return topics, err
}

func (m *topicAdminImpl) PartitionedStats(topic string) (stats PartitionedTopicStats, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}
	pathPattern := "%s/admin/v2/%s/partitioned-stats"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return stats, err
	}
	err = m.callWithRet(m.httpclient, req, &stats)
	return stats, err
}

func (m *topicAdminImpl) PartitionedCreateMissedPartitions(topic string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/createMissedPartitions"
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = m.callWithRet(m.httpclient, req, nil)
	return err
}

func (m *topicAdminImpl) GetMetadata(topic string) (meta PartitionedTopicStatsMetadata, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}
	pathPattern := "%s/admin/v2/%s/partitions"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return meta, err
	}
	err = m.callWithRet(m.httpclient, req, &meta)
	return meta, err
}
