package internal

import (
	"fmt"
	"strconv"
	"strings"
)

// ------ topic level ------

type TopicLevel string

var (
	DefaultGroundTopicLevelL1 = TopicLevel("L1")
	DefaultDeadTopicLevelDLQ  = TopicLevel("DLQ")
)

func (lvl TopicLevel) String() string {
	return string(lvl)
}

// OrderOf calculate order of the TopicLevel.
// Prefixes L and S define as positive integer values, Prefixes B and DLQ defines negative integer values.
//
// order = base + factor * suffix-no.
// base constants are -> L: 0, S: 100, B: 0, DLQ: -100
// factor constants are -> L: 1, S: 1, B: -1; DLQ: -1
//
// Examples:
// S1 ~ Sn     <=> 101 ~ 100+n
// L1 ~ Ln     <=> 1 ~ n
// B1 ~ Bn     <=> -1 ~ -n
// DLQ         <=> -100
// DLQ1 ~ DLQn <=> -101 ~ -(100+n)
func (lvl TopicLevel) OrderOf() int {
	if lvl == DefaultDeadTopicLevelDLQ {
		return -100
	}
	suffix := string(lvl)[len(string(lvl))-1:]
	baseFactor := 1 // default for Lx
	baseOrder := 0  // default for Lx

	if strings.HasPrefix(string(lvl), "L") {

	} else if strings.HasPrefix(string(lvl), "S") {
		baseFactor = 1
		baseOrder = 100
	} else if strings.HasPrefix(string(lvl), "B") {
		baseFactor = -1
		baseOrder = 0
	} else if strings.HasPrefix(string(lvl), "DLQ") {
		baseFactor = -1
		baseOrder = 100
	} else {
		panic(fmt.Sprintf("invalid topic level: %v", lvl))
	}
	no := 0
	if suffixNo, err := strconv.Atoi(suffix); err == nil {
		no = suffixNo
	}
	return baseFactor * (baseOrder + no)
}

func (lvl TopicLevel) TopicSuffix() string {
	if lvl == "" {
		panic("invalid blank topic level")
	}
	if lvl == DefaultGroundTopicLevelL1 {
		return ""
	} else {
		return "-" + string(lvl)
	}
}

// ------ topic level parser ------

var TopicLevelParser = topicLevelParser{}

type topicLevelParser struct {
}

func (p topicLevelParser) FormatList(levels []TopicLevel) string {
	if len(levels) <= 0 {
		return ""
	}
	ls := make([]string, len(levels))
	for i := 0; i < len(levels); i++ {
		ls[i] = levels[i].String()
	}
	return strings.Join(ls, ", ")
}

// ------ Balance Strategy Symbol ------

type BalanceStrategy string

func (e BalanceStrategy) String() string {
	return string(e)
}
