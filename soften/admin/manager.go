package admin

import (
	"github.com/shenqianjin/soften-client-go/soften/internal/admin"
)

type TopicManager interface {
	Unload(topic string) error
	Delete(topic string) error
	Stats(topic string) (stats admin.TopicStats, err error)
	StatsInternal(topic string) (stats admin.TopicInternalStats, err error)
}

func NewAdminManager(url string) TopicManager {
	return admin.NewTopicManager(url)
}
