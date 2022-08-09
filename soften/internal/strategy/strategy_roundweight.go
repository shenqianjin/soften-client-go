package strategy

import (
	"errors"
	"fmt"
)

// ------ RoundWeight ------

type roundWeightStrategy struct {
	indexes []int
	curr    int
}

func NewRoundWeightStrategy(weights []uint) (*roundWeightStrategy, error) {
	indexes := make([]int, 0)
	total := uint(0)
	for index, weight := range weights {
		if weight > defaultMaxWeight {
			return nil, errors.New(fmt.Sprintf("invalid weight: %d. weight cannot exceed %d", weight, defaultMaxWeight))
		}
		for i := 0; i < int(weight); i++ {
			indexes = append(indexes, index)
		}
		total += weight
	}
	if total == 0 {
		return nil, errors.New(fmt.Sprintf("invalid weights: %d. weight total must be larger than 0", weights))
	}
	return &roundWeightStrategy{indexes: indexes, curr: 0}, nil
}

func (s *roundWeightStrategy) Next(excludes ...int) int {
	if len(excludes) > 0 {
		excludeMap := make(map[int]bool, len(excludes))
		for _, index := range excludes {
			excludeMap[index] = true
		}
		for {
			next := s.indexes[s.curr]
			s.curr = (s.curr + 1) % len(s.indexes)
			if ex, ok := excludeMap[next]; ok && ex {
				continue
			}
			return next
		}
	}
	next := s.indexes[s.curr]
	s.curr = (s.curr + 1) % len(s.indexes)
	return next
}
