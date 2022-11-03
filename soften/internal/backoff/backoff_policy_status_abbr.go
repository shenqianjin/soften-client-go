package backoff

// ------ abbr status backoff policy ------

type abbrStatusBackoffPolicy struct {
	backoffPolicy *abbrBackoffDelayPolicy
}

func (p *abbrStatusBackoffPolicy) Next(redeliveryTimes int, statusReconsumeTimes int) uint {
	return p.backoffPolicy.Next(statusReconsumeTimes)
}

func NewAbbrStatusBackoffDelayPolicy(delays []string) (*abbrStatusBackoffPolicy, error) {
	backoffPolicy, err := NewAbbrBackoffDelayPolicy(delays)
	if err != nil {
		return nil, err
	}
	return &abbrStatusBackoffPolicy{backoffPolicy: backoffPolicy}, nil
}
