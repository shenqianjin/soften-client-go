package backoff

import (
	"errors"
	"fmt"
	"strconv"
)

// ------ abbr backoff policy ------

type abbrBackoffDelayPolicy struct {
	backoffDelays []uint
}

func (p abbrBackoffDelayPolicy) Next(redeliveryTimes int) uint {
	if redeliveryTimes < 0 {
		redeliveryTimes = 0
	}
	if redeliveryTimes >= len(p.backoffDelays) {
		return p.backoffDelays[len(p.backoffDelays)-1]
	}
	return p.backoffDelays[redeliveryTimes]
}

func NewAbbrBackoffDelayPolicy(delays []string) (*abbrBackoffDelayPolicy, error) {
	if len(delays) == 0 {
		return nil, errors.New("backoffDelays is empty")
	}
	backoffDelays := make([]uint, len(delays))
	for index, delay := range delays {
		last := delay[len(delay)-1:]
		if unit, err := ValueOf(last); err != nil {
			return nil, err
		} else if d, err := strconv.Atoi(delay[0 : len(delay)-1]); err != nil {
			return nil, errors.New(fmt.Sprintf("invalid in backOffDelays: %s", delay))
		} else {
			backoffDelays[index] = uint(d) * unit.Delay()
		}
	}
	return &abbrBackoffDelayPolicy{backoffDelays: backoffDelays}, nil
}
