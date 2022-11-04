package admin

// ------ topic admin interface ------

type BaseTopicAdmin interface {
	Unload(topic string) error
	StatsInternal(topic string) (stats TopicStatsInternal, err error)
}

type NonPartitionedTopicManager interface {
	Create(topic string) error
	Delete(topic string) error
	List(namespace string) (topics []string, err error)
	Stats(topic string) (stats TopicStats, err error)
}

type PartitionedTopicManger interface {
	Create(topic string, partitions uint) error
	CreateMissedPartitions(topic string) error
	Delete(topic string) error
	Update(topic string, partitions uint) error
	List(namespace string) (topics []string, err error)
	Stats(topic string) (stats PartitionedTopicStats, err error)
	GetMetadata(topic string) (meta PartitionedTopicStatsMetadata, err error)
}

type RobustTopicManager interface {
	Unload(topic string) error
	StatsInternal(topic string) (stats TopicStatsInternal, err error)

	// Create non-partitioned or partitioned topic, partitions == 0 means non-partitioned
	Create(topic string, partitions uint) error
	Delete(topic string) error
	List(namespace string) (topics []string, err error)
	Stats(topic string) (stats TopicStats, err error)
}

// ------ namespace api ------

type NamespaceManager interface {
	Create(namespace string) error
	Delete(namespace string) error
	List(tenant string) ([]string, error)
}

// ------ tenant api ------

type TenantManager interface {
	Create(tenant string, info TenantInfo) error
	Delete(tenant string) error
	List() ([]string, error)
}

// ------ cluster api ------

type ClusterManager interface {
	List() ([]string, error)
}
