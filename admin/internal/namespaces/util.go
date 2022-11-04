package namespaces

import (
	"github.com/shenqianjin/soften-client-go/admin/internal/support/util"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

func CreateIfNotExist(url string, namespace string) {
	manager := admin.NewNamespaceManager(url)
	namespaceTopic, err := util.ParseNamespaceTopic(namespace)
	if err != nil {
		logrus.Fatal(err)
	}
	if namespaces, err := manager.List(namespaceTopic.Tenant); err != nil {
		logrus.Fatal(err)
	} else if !slices.Contains(namespaces, namespaceTopic.Namespace) {
		if err := manager.Create(namespaceTopic.Namespace); err != nil {
			logrus.Fatal(err)
		}
		logrus.Infof("automatically created namespace: \"%s\"", namespaceTopic.Namespace)
	}
}
