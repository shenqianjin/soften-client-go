package config

import (
	"errors"
	"fmt"

	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/internal/strategy"
)

// ------ balanceStrategy enums ------

const (
	BalanceStrategyEachRand    = internal.BalanceStrategy("EachRand")    // 每次随机选择
	BalanceStrategyRoundRand   = internal.BalanceStrategy("RoundRand")   // 每伦随机选择
	BalanceStrategyRoundWeight = internal.BalanceStrategy("RoundWeight") // 根据权重轮训
	BalanceStrategyRoundRobin  = internal.BalanceStrategy("RoundRobin")  // 轮训(权重无关)
)

func BalanceStrategyValues() []internal.BalanceStrategy {
	return []internal.BalanceStrategy{BalanceStrategyEachRand, BalanceStrategyRoundRand, BalanceStrategyRoundWeight, BalanceStrategyRoundRobin}
}

// ------ balance strategy factory ------

func BuildStrategy(str internal.BalanceStrategy, weights []uint) (strategy.IBalanceStrategy, error) {
	switch str {
	case BalanceStrategyEachRand:
		return strategy.NewEachRandStrategy(weights)
	case BalanceStrategyRoundRand:
		return strategy.NewRoundRandStrategy(weights)
	case BalanceStrategyRoundRobin:
		return strategy.NewRoundRobinStrategy(len(weights))
	case BalanceStrategyRoundWeight:
		return strategy.NewRoundWeightStrategy(weights)
	default:
		return nil, errors.New(fmt.Sprintf("invalid balance strategy: %s", str))
	}
}
