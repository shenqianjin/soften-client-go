package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type topicManagerImpl struct {
	httpclient *http.Client
	url        string
}

func NewTopicManager(url string) *topicManagerImpl {
	manager := &topicManagerImpl{
		url:        url,
		httpclient: &http.Client{},
	}
	return manager
}

func (m *topicManagerImpl) Unload(topic string) error {
	pathPattern := "%s/admin/v2/%s/unload"
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, topic), http.NoBody)
	if err != nil {
		return err
	}
	err = m.callWithRet(m.httpclient, req, nil)
	return err
}

func (m *topicManagerImpl) Delete(topic string) error {
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

func (m *topicManagerImpl) Stats(topic string) (stats TopicStats, err error) {
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

func (m *topicManagerImpl) StatsInternal(topic string) (stats TopicInternalStats, err error) {
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

func (m *topicManagerImpl) callWithRet(c *http.Client, req *http.Request, ret interface{}) error {
	if internal.DebugMode {
		reqbytes, dumpErr := httputil.DumpRequestOut(req, true)
		fmt.Println(string(reqbytes), dumpErr)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	if internal.DebugMode {
		respBytes, dumpErr := httputil.DumpResponse(resp, resp.ContentLength > 0)
		fmt.Println(string(respBytes), dumpErr)
	}
	// success
	if resp.StatusCode/100 == 2 {
		if ret != nil {
			if err := json.NewDecoder(resp.Body).Decode(ret); err != nil {
				fmt.Printf("failed to unmarshal resp body, err: %v\n", err)
				return err
			}
		}
		return nil
	}
	// failed
	if resp.ContentLength != 0 {
		if respData, err := ioutil.ReadAll(resp.Body); err != nil {
			fmt.Printf("failed to unmarshal resp body, err: %v\n", err)
			return err
		} else {
			err = errors.New(fmt.Sprintf("failed to call pulsar: %s => %s", resp.Status, respData))
		}
	} else {
		err = errors.New(fmt.Sprintf("failed to call pulsar: %s", resp.Status))
	}
	return err
}
