package message

import (
	"errors"
	"fmt"
	"math"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

type Levels []internal.TopicLevel

var (
	S3 = internal.TopicLevel("S3")
	S2 = internal.TopicLevel("S2")
	S1 = internal.TopicLevel("S1")

	L8 = internal.TopicLevel("L8")
	L7 = internal.TopicLevel("L7")
	L6 = internal.TopicLevel("L6")
	L5 = internal.TopicLevel("L5")
	L4 = internal.TopicLevel("L4")
	L3 = internal.TopicLevel("L3")
	L2 = internal.TopicLevel("L2")
	L1 = internal.DefaultGroundTopicLevelL1

	B1 = internal.TopicLevel("B1")
	B2 = internal.TopicLevel("B2")
	B3 = internal.TopicLevel("B3")

	// D1 supposes to be active when producer checks to dead status.
	// it is only a special level which is not expected to be subscribed.
	D1 = internal.TopicLevel("D1")
)

func LevelValues() Levels {
	values := Levels{
		L1, L2, L3, L4, L5, L6, L7, L8,
		B1, B2, B3,
		D1,
		S1, S2, S3,
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
