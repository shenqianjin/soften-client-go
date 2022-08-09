package topic

import (
	"errors"
	"fmt"
	"math"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type Levels []internal.TopicLevel

var (
	S2  = internal.TopicLevel("S2")
	S1  = internal.TopicLevel("S1")
	L3  = internal.TopicLevel("L3")
	L2  = internal.TopicLevel("L2")
	L1  = internal.DefaultGroundTopicLevelL1
	B1  = internal.TopicLevel("B1")
	B2  = internal.TopicLevel("B2")
	DLQ = internal.DefaultDeadTopicLevelDLQ
)

func LevelValues() Levels {
	values := Levels{
		S1, S2,
		L1, L2, L3,
		B1, B2,
		DLQ,
	}
	return values
}

// LevelOf convert level type from string to internal.TopicLevel
func LevelOf(level string) (internal.TopicLevel, error) {
	for _, v := range LevelValues() {
		if v.String() == level {
			return v, nil
		}
	}
	return "", errors.New(fmt.Sprintf("invalid (or not supported) topic level: %s", level))

}

func Exists(level internal.TopicLevel) bool {
	for _, lvl := range LevelValues() {
		if lvl == level {
			return true
		}
	}
	return false
}

func HighestLevel() internal.TopicLevel {
	var level internal.TopicLevel
	order := math.MinInt
	for _, v := range LevelValues() {
		if order < v.OrderOf() {
			level = v
			order = v.OrderOf()
		}
	}
	return level
}

func LowestLevel() internal.TopicLevel {
	var level internal.TopicLevel
	order := math.MaxInt
	for _, v := range LevelValues() {
		if order > v.OrderOf() {
			level = v
			order = v.OrderOf()
		}
	}
	return level
}
