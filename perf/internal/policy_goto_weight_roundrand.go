package internal

import (
	"math/rand"

	"github.com/shenqianjin/soften-client-go/soften/handler"
)

type GotoPolicy interface {
	Next() interface{}
}

type roundRandWeightGotoPolicy struct {
	choiceCh chan interface{}

	total      uint
	indexesMap map[int]interface{} // index到owner的下标映射
}

func NewRoundRandWeightGotoPolicy(weightMap map[string]uint64) *roundRandWeightGotoPolicy {
	total := uint(0)
	count := 0
	indexesMap := make(map[int]interface{})
	for key, weight := range weightMap {
		for i := 0; i < int(weight); i++ {
			indexesMap[count] = key
			count++
		}
		total += uint(weight)
	}

	policy := &roundRandWeightGotoPolicy{
		choiceCh:   make(chan interface{}, 16),
		total:      total,
		indexesMap: indexesMap,
	}

	if len(policy.indexesMap) > 1 && policy.total > 0 {
		go policy.generate()
	}

	return policy
}

func (p *roundRandWeightGotoPolicy) generate() {
	for {
		nextIndexes := rand.Perm(int(p.total))
		for _, index := range nextIndexes {
			p.choiceCh <- p.indexesMap[index]
		}
	}
}

func (p *roundRandWeightGotoPolicy) Next() interface{} {
	if len(p.indexesMap) > 1 {
		return <-p.choiceCh
	}
	return handler.GotoDone
}
