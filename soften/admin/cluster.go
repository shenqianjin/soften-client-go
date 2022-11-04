package admin

import (
	"fmt"
	"net/http"
)

type clusterManager struct {
	httpclient *http.Client
	url        string
}

func NewClusterManager(url string) ClusterManager {
	httpClient := &http.Client{}
	return &clusterManager{
		httpclient: httpClient,
		url:        url,
	}
}

func (m *clusterManager) List() ([]string, error) {
	pathPattern := "%s/admin/v2/clusters"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url), http.NoBody)
	if err != nil {
		return nil, err
	}
	var clusters []string
	err = callWithRet(m.httpclient, req, &clusters)
	return clusters, err
}
