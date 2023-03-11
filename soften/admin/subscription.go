package admin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type subscriptionManager struct {
	httpclient *http.Client
	url        string
}

// ------ subscription implementation ------

func NewSubscriptionManager(url string) *subscriptionManager {
	httpClient := &http.Client{}
	manager := &subscriptionManager{
		url:        url,
		httpclient: httpClient,
	}
	return manager
}

func (m *subscriptionManager) List(topic string) (subs []string, err error) {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return subs, err
	}
	pathPattern := "%s/admin/v2/%s/subscriptions"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath()), http.NoBody)
	if err != nil {
		return subs, err
	}
	err = callWithRet(m.httpclient, req, &subs)
	return subs, err
}

func (m *subscriptionManager) Subscribe(topic, sub string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/subscription/%s"

	// earliest position
	// @see https://github.com/apache/pulsar/blob/master/managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/PositionImpl.java#L31-L32
	earliest := struct {
		ledgerId int
		entryId  int
	}{-1, -1}
	jsonBytes, err := json.Marshal(earliest)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath(), sub), bytes.NewReader(jsonBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *subscriptionManager) Unsubscribe(topic, sub string) error {
	parsedTopic, err := internal.ParseTopicName(topic)
	if err != nil {
		return err
	}
	pathPattern := "%s/admin/v2/%s/subscription/%s"
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(pathPattern, m.url, parsedTopic.GetTopicRestPath(), sub), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}
