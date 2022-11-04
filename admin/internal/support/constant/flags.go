package constant

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
		"the value should always be a positive number\n" +
		"it must be 1 if non-partitioned mode is specified"

	PartitionedUsage = "partitioned mode or non-partitioned mode to process"

	PartitionsUsage4Update = "the number of partitions to update to on your topic or topics\n" +
		"new partitions must more than the existing value"

	SubscriptionUsage = "subscription name on the ground topic\n" +
		"separate with ',' if more than one"

	SingleSubscriptionUsage = "subscription name for subscribing your topic\n"

	ConditionsUsage = "conditions to execute\n" +
		"an expression is a one-liner that returns a bool value\n" +
		"separate with '\\n' and these lines mean 'or' logic for matching\n" +
		"see https://github.com/antonmedv/expr/blob/master/docs/Language-Definition.md for grammar"

	AllUsage = "process on all topics including all levels, statuses and subscriptions"

	PrintProgressIterateIntervalUsage = "iterate interval to print progress"
	PrintProgressRecallIntervalUsage  = "recall interval to print progress"
	PrintModeUsage                    = "mode of print these matched messages\n" +
		"0: print nothing; 1 print id only; 2: print id, payload, publish time and event time; "

	StartPublishTimeUsage = "start publish time to check\n" +
		"it must be RFC3339Nano format '2006-01-02T15:04:05.999999999Z07:00' or 'now'"

	EndPublishTimeUsage = "end publish time to check\n" +
		"it must be RFC3339Nano format '2006-01-02T15:04:05.999999999Z07:00' or 'now'"

	StartEventTimeUsage = "start event time to check\n" +
		"processing will be ignored if the event time of messages is zero\n" +
		"it must be RFC3339Nano format '2006-01-02T15:04:05.999999999Z07:00' or 'now'"

	EndEventTimeUsage = "end event time to check\n" +
		"it must be RFC3339Nano format '2006-01-02T15:04:05.999999999Z07:00' or 'now'"

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
