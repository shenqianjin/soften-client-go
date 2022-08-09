package backoff

import (
	"errors"
	"fmt"
	"strings"
)

type DelayUnit struct {
	abbr  string
	delay uint
}

var (
	DelayUnitSecond = &DelayUnit{"s", 1}
	DelayUnitMinute = &DelayUnit{"m", 60}
	DelayUnitHour   = &DelayUnit{"h", 60 * 60}
)

func ValueOf(abbr string) (*DelayUnit, error) {
	lowerAbbr := strings.ToLower(abbr)
	for _, v := range Values() {
		if v.abbr == lowerAbbr {
			return v, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("invalid delay unit: %s", abbr))

}

func (e *DelayUnit) Delay() uint {
	return e.delay
}

func (e *DelayUnit) String() string {
	return e.abbr
}

func Values() []*DelayUnit {
	return []*DelayUnit{DelayUnitSecond, DelayUnitMinute, DelayUnitHour}
}
