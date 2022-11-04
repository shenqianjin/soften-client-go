package tenants

import (
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

func CreateIfNotExist(url string, tenant string) {
	manager := admin.NewTenantManager(url)
	if tenants, err := manager.List(); err != nil {
		logrus.Fatal(err)
	} else if !slices.Contains(tenants, tenant) {
		cManager := admin.NewClusterManager(url)
		clusters, err := cManager.List()
		if err != nil {
			logrus.Fatal(err)
		}
		if err := manager.Create(tenant, admin.TenantInfo{AllowedClusters: clusters}); err != nil {
			logrus.Fatal(err)
		}
		logrus.Infof("automatically created tenant: \"%s\"", tenant)
	}
}
