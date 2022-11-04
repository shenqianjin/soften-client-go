package admin

import (
	"fmt"
	"net/http"
)

type namespaceManager struct {
	httpclient *http.Client
	url        string
}

func NewNamespaceManager(url string) NamespaceManager {
	httpClient := &http.Client{}
	return &namespaceManager{
		httpclient: httpClient,
		url:        url,
	}
}

func (m *namespaceManager) Create(namespace string) error {
	pathPattern := "%s/admin/v2/namespaces/%s"
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, namespace), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *namespaceManager) Delete(namespace string) error {
	pathPattern := "%s/admin/v2/namespaces/%s"
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(pathPattern, m.url, namespace), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *namespaceManager) List(tenant string) ([]string, error) {
	pathPattern := "%s/admin/v2/namespaces/%s"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url, tenant), http.NoBody)
	if err != nil {
		return nil, err
	}
	var namespaces []string
	err = callWithRet(m.httpclient, req, &namespaces)
	return namespaces, err

}
