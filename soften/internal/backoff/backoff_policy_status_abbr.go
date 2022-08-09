package backoff

// ------ abbr status backoff policy ------

type abbrStatusBackoffPolicy struct {
	backoffPolicy *abbrBackoffPolicy
}

func (p *abbrStatusBackoffPolicy) Next(redeliveryTimes int, statusReconsumeTimes int) uint {
	return p.backoffPolicy.Next(statusReconsumeTimes)
}

func NewAbbrStatusBackoffPolicy(delays []string) (*abbrStatusBackoffPolicy, error) {
	backoffPolicy, err := NewAbbrBackoffPolicy(delays)
	if err != nil {
		return nil, err
	}
	return &abbrStatusBackoffPolicy{backoffPolicy: backoffPolicy}, nil
}
