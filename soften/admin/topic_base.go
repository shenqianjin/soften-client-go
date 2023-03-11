package admin

import (
	"fmt"
	"net/http"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type baseTopicManger struct {
	httpclient *http.Client
	url        string
}

func newBaseTopicManger(url string) *baseTopicManger {
	httpClient := &http.Client{}
	return &baseTopicManger{
		url:        url,
		httpclient: httpClient,
	}
}

// ------ base implementation ------

func (m *baseTopicManger) StatsInternal(topic string) (stats TopicStatsInternal, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}
	pathPattern := "%s/admin/v2/%s/internalStats"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return stats, err
	}
	err = callWithRet(m.httpclient, req, &stats)
	return stats, err
}

func (m *baseTopicManger) Unload(topic string) (err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}

	pathPattern := "%s/admin/v2/%s/unload"
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *baseTopicManger) SetMessageTTL(topic string, ttl uint64) (err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return
	}

	pathPattern := "%s/admin/v2/%s/messageTTL?messageTTL=%d"
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath(), ttl), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}
