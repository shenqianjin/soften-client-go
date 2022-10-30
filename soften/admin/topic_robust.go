package admin

import "strings"

type Option interface {
}

const (
	Err404NotFound = "404 Not Found"
)

type robustTopicManger struct {
	nonPartitionedManger    *nonPartitionedTopicManager
	partitionedTopicManager *partitionedTopicManager
}

func NewRobustTopicManager(url string) RobustTopicManager {
	return &robustTopicManger{
		nonPartitionedManger:    NewNonPartitionedTopicManager(url),
		partitionedTopicManager: NewPartitionedTopicManager(url),
	}
}

func (m *robustTopicManger) Unload(topic string) error {
	return m.nonPartitionedManger.Unload(topic)
}

func (m *robustTopicManger) StatsInternal(topic string) (stats TopicStatsInternal, err error) {
	return m.nonPartitionedManger.StatsInternal(topic)
}

func (m *robustTopicManger) Create(topic string, partitions uint) error {
	if partitions > 0 {
		return m.partitionedTopicManager.Create(topic, partitions)
	} else {
		return m.nonPartitionedManger.Create(topic)
	}
}

func (m *robustTopicManger) Delete(topic string) error {
	// try non-partitioned firstly
	err := m.nonPartitionedManger.Delete(topic)
	if err != nil && strings.Contains(err.Error(), Err404NotFound) {
		// try partitioned if 404
		return m.partitionedTopicManager.Delete(topic)
	}
	return err
}

func (m *robustTopicManger) List(namespace string) ([]string, error) {
	// try non-partitioned firstly
	topics, err := m.nonPartitionedManger.List(namespace)
	if err != nil && strings.Contains(err.Error(), Err404NotFound) {
		// try partitioned if 404
		return m.partitionedTopicManager.List(namespace)
	}
	return topics, err
}

func (m *robustTopicManger) Stats(topic string) (TopicStats, error) {
	// try non-partitioned firstly
	stats, err := m.nonPartitionedManger.Stats(topic)
	if err != nil && strings.Contains(err.Error(), Err404NotFound) {
		// try partitioned if 404
		ps, err1 := m.partitionedTopicManager.Stats(topic)
		return ps.TopicStats, err1
	}
	return stats, err
}
