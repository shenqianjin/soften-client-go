package strategy

import (
	"errors"
	"fmt"
)

// ------ RoundRobin ------

type roundRobinStrategy struct {
	indexes []int
	curr    int
}

func NewRoundRobinStrategy(destLen int) (*roundRobinStrategy, error) {
	if destLen <= 0 {
		return nil, errors.New(fmt.Sprintf("invalid round robin len: %d", destLen))
	}
	indexes := make([]int, destLen)
	for index := 0; index < destLen; index++ {
		indexes[index] = index
	}
	return &roundRobinStrategy{indexes: indexes, curr: 0}, nil
}

func (s *roundRobinStrategy) Next(excludes ...int) int {
	if len(excludes) > 0 {
		excludeMap := make(map[int]bool, len(excludes))
		for _, index := range excludes {
			excludeMap[index] = true
		}
		for {
			next := s.indexes[s.curr]
			s.curr = (s.curr + 1) % len(s.indexes)
			if ex, ok := excludeMap[next]; !ok || !ex {
				return next
			}
		}
	}
	next := s.indexes[s.curr]
	s.curr = (s.curr + 1) % len(s.indexes)
	return next
}
