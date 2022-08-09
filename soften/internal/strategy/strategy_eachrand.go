package strategy

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// ------ EachRand ------

type eachRandStrategy struct {
	total      uint
	prefixSums []uint
}

func NewEachRandStrategy(weights []uint) (*eachRandStrategy, error) {
	total := uint(0)
	prefixSums := make([]uint, len(weights))
	for index, weight := range weights {
		if weight > defaultMaxWeight {
			return nil, errors.New(fmt.Sprintf("invalid weight: %d. weight cannot exceed %d", weight, defaultMaxWeight))
		}
		prefixSums[index] = total
		total += weight
	}
	if total <= 0 {
		return nil, errors.New(fmt.Sprintf("invalid weights: %d. weight total must be larger than 0", weights))
	}
	return &eachRandStrategy{total: total, prefixSums: prefixSums}, nil
}

func (s *eachRandStrategy) Next(excludes ...int) int {
	r := uint(rand.Intn(int(s.total)))
	var excludeMap map[int]bool
	if len(excludes) > 0 {
		if excludeMap == nil {
			excludeMap = make(map[int]bool, len(excludes))
			for _, exIndex := range excludes {
				excludeMap[exIndex] = true
			}
		}
	}
	for index := 0; index < len(s.prefixSums)-1; index++ {
		if r >= s.prefixSums[index] && r < s.prefixSums[index+1] {
			if excludeMap == nil {
				return index
			}
			if ex, ok := excludeMap[index]; !ok || !ex {
				return index
			}
		}
	}
	return 0
}

// ------ helper ------

func init() {
	rand.Seed(time.Now().UnixNano())
}
