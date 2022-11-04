package admin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type tenantManager struct {
	httpclient *http.Client
	url        string
}

func NewTenantManager(url string) TenantManager {
	httpClient := &http.Client{}
	return &tenantManager{
		httpclient: httpClient,
		url:        url,
	}
}

func (m *tenantManager) Create(tenant string, info TenantInfo) error {
	pathPattern := "%s/admin/v2/tenants/%s"
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(pathPattern, m.url, tenant), bytes.NewReader(infoBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *tenantManager) Delete(tenant string) error {
	pathPattern := "%s/admin/v2/tenants/%s"
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(pathPattern, m.url, tenant), http.NoBody)
	if err != nil {
		return err
	}
	err = callWithRet(m.httpclient, req, nil)
	return err
}

func (m *tenantManager) List() ([]string, error) {
	pathPattern := "%s/admin/v2/tenants"
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(pathPattern, m.url), http.NoBody)
	if err != nil {
		return nil, err
	}
	var tenants []string
	err = callWithRet(m.httpclient, req, &tenants)
	return tenants, err
}
