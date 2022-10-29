package util

import (
	"strings"

	"github.com/shenqianjin/soften-client-go/soften/message"
)

var (
	LevelUsage = "levels for your ground topic\n" +
		"separate with ',' if more than one\n" +
		"levels supports [" + allLevels + "]"

	StatusUsage = "status for your ground topic on your specified levels\n" +
		"separate with ',' if more than one\n" +
		"supports [" + allStatuses + "]\n" +
		"note that 'Dead' status defaults active on only ground (L1) level"

	PartitionsUsage4Create = "the number of partitions of your topic or topics\n" +
		"non-partitioned topic will be created if it is not specified or less than 1"

	PartitionsUsage4Update = "the number of partitions to update to on your topic or topics\n" +
		"new partitions must more than the existing value"

	PartitionedUsage = "partitioned mode to process, or process non-partitioned"

	SubscriptionUsage = "subscription name on the ground topic\n" +
		"separate with ',' if more than one"

	ConditionsUsage = "conditions to execute\n" +
		"an expression is a one-liner that returns a bool value\n" +
		"separate with ',' and equal to 'or' if more than one\n" +
		"see https://github.com/antonmedv/expr/blob/master/docs/Language-Definition.md for grammar"

	PrintProgressIterateIntervalUsage = "iterate interval to print progress"
	PrintProgressRecallIntervalUsage  = "recall interval to print progress"
	PrintModeUsage                    = "mode of print these matched messages\n" +
		"0: print nothing; 1: print id, payload, publish time and event time; 2 print id only"

	StartPublishTimeUsage = "start publish time to check\n" +
		"it must be RFC3339Nano format '2006-01-02T15:04:05.999999999Z07:00'"

	StartEventTimeUsage = "start event time to check\n" +
		"processing will be ignored if the event time of messages is zero\n" +
		"it must be RFC3339Nano format '2006-01-02T15:04:05.999999999Z07:00'"

	BatchEnableUsage = "enable publish message in async, or default is send in sync"

	PublishMaxTimesUsage = "publish max times to backoff is send/sendAsync failed. default 0 means to try infinitely"
)

var allLevels = func() string {
	levels := make([]string, len(message.LevelValues()))
	for index, l := range message.LevelValues() {
		levels[index] = l.String()
	}
	return strings.Join(levels, ",")
}()

var allStatuses = func() string {
	statuses := make([]string, 0)
	for _, s := range message.StatusValues() {
		if s == message.StatusDone || s == message.StatusDiscard {
			continue
		}
		statuses = append(statuses, s.String())
	}
	return strings.Join(statuses, ",")
}()
