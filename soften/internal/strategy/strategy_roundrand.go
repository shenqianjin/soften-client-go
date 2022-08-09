package strategy

import (
	"errors"
	"fmt"
	"math/rand"
)

// ------ RoundRand ------

type roundRandStrategy struct {
	total      uint
	indexesMap map[int]int // index到owner的下标映射

	nextIndexes []int
}

func NewRoundRandStrategy(weights []uint) (*roundRandStrategy, error) {
	total := uint(0)
	count := 0
	indexesMap := make(map[int]int)
	for index, weight := range weights {
		if weight > defaultMaxWeight {
			return nil, errors.New(fmt.Sprintf("invalid weight: %d. weight cannot exceed %d", weight, defaultMaxWeight))
		}
		for i := 0; i < int(weight); i++ {
			indexesMap[count] = index
			count++
		}
		total += weight
	}
	if total == 0 {
		return nil, errors.New(fmt.Sprintf("invalid weights: %d. weight total must be larger than 0", weights))
	}
	return &roundRandStrategy{total: total, indexesMap: indexesMap}, nil
}

func (s *roundRandStrategy) Next(excludes ...int) int {
	if len(s.nextIndexes) == 0 {
		s.nextIndexes = rand.Perm(int(s.total))
	}
	if len(excludes) > 0 {
		excludeMap := make(map[int]bool, len(excludes))
		for _, index := range excludes {
			excludeMap[index] = true
		}
		for i, next := range s.nextIndexes {
			if !excludeMap[next] {
				s.nextIndexes = s.nextIndexes[i:]
				return s.indexesMap[next]
			}
		}
		return 0
	} else {
		next := s.nextIndexes[0]
		s.nextIndexes = s.nextIndexes[1:]
		return s.indexesMap[next]
	}
}
