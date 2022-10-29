package admin

type TopicManager interface {
	Create(topic string) error
	Unload(topic string) error
	Delete(topic string) error
	Stats(topic string) (stats TopicStats, err error)
	StatsInternal(topic string) (stats TopicStatsInternal, err error)
}

func NewAdminManager(url string) TopicManager {
	return NewTopicManager(url)
}
