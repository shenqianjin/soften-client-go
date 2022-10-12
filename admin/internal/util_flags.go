package internal

import (
	"strings"

	"github.com/shenqianjin/soften-client-go/soften/message"
)

var (
	levelUsage = "levels for your ground topic\n" +
		"separate with ',' if more than one\n" +
		"levels supports [" + allLevels + "]"

	statusUsage = "status for your ground topic on your specified levels\n" +
		"separate with ',' if more than one\n" +
		"supports [" + allStatuses + "]\n" +
		"note that 'Dead' status defaults active on only ground (L1) level"

	partitionsUsage4Create = "the number of partitions of your topic or topics\n" +
		"non-partitioned topic will be created if it is not specified or less than 1"

	partitionsUsage4Update = "the number of partitions to update to on your topic or topics\n" +
		"new partitions must more than the existing value"

	partitionedUsage = "partitioned mode to process, or process non-partitioned"

	subscriptionUsage = "subscription name on the ground topic\n" +
		"separate with ',' if more than one"

	conditionsUsage = "conditions to execute\n" +
		"an expression is a one-liner that returns a bool value\n" +
		"see https://github.com/antonmedv/expr/blob/master/docs/Language-Definition.md for grammar"
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
