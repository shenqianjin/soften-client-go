package util

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/shenqianjin/soften-client-go/perf/internal/support/param"
	"github.com/shenqianjin/soften-client-go/soften/decider"
)

func ParseUint64Array(str string) []uint64 {
	nums := make([]uint64, 0)
	if str != "" {
		numsStr := strings.Split(str, ",")
		for _, ns := range numsStr {
			if n, err := strconv.ParseUint(ns, 10, 32); err != nil {
				panic(err)
			} else if n < 0 {
				panic("negative rate is invalid")
			} else {
				nums = append(nums, n)
			}
		}
	}
	return nums
}

func ParseGotoWeightMap(gotoParam param.GotoParam) map[string]uint64 {
	gotoWeights := ParseUint64Array(gotoParam.Weight)
	if len(gotoWeights) != 10 {
		panic(fmt.Sprintf("invalid goto weight option: %v", gotoParam.Weight))
	}
	gotoWeightMap := make(map[string]uint64)
	if weight := gotoWeights[GotoIndexes[decider.GotoDone.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoDone.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoDiscard.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoDiscard.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoDead.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoDead.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoPending.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoPending.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoBlocking.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoBlocking.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoRetrying.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoRetrying.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoUpgrade.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoUpgrade.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoDegrade.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoDegrade.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoShift.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoShift.String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[decider.GotoTransfer.String()]]; weight > 0 {
		gotoWeightMap[decider.GotoTransfer.String()] = weight
	}
	//
	if gotoParam.DoneWeight > 0 {
		gotoWeightMap[decider.GotoDone.String()] = gotoParam.DoneWeight
	}
	if gotoParam.DiscardWeight > 0 {
		gotoWeightMap[decider.GotoDiscard.String()] = gotoParam.DiscardWeight
	}
	if gotoParam.DeadWeight > 0 {
		gotoWeightMap[decider.GotoDead.String()] = gotoParam.DeadWeight
	}
	if gotoParam.PendingWeight > 0 {
		gotoWeightMap[decider.GotoPending.String()] = gotoParam.PendingWeight
	}
	if gotoParam.BlockingWeight > 0 {
		gotoWeightMap[decider.GotoBlocking.String()] = gotoParam.BlockingWeight
	}
	if gotoParam.RetryingWeight > 0 {
		gotoWeightMap[decider.GotoRetrying.String()] = gotoParam.RetryingWeight
	}
	if gotoParam.UpgradeWeight > 0 {
		gotoWeightMap[decider.GotoUpgrade.String()] = gotoParam.UpgradeWeight
	}
	if gotoParam.DegradeWeight > 0 {
		gotoWeightMap[decider.GotoDegrade.String()] = gotoParam.DegradeWeight
	}
	if gotoParam.ShiftWeight > 0 {
		gotoWeightMap[decider.GotoShift.String()] = gotoParam.ShiftWeight
	}
	if gotoParam.TransferWeight > 0 {
		gotoWeightMap[decider.GotoTransfer.String()] = gotoParam.TransferWeight
	}
	return gotoWeightMap
}

// ------ helper ------

var GotoIndexes = map[string]int{
	decider.GotoDone.String():     0,
	decider.GotoDiscard.String():  1,
	decider.GotoDead.String():     2,
	decider.GotoPending.String():  3,
	decider.GotoBlocking.String(): 4,
	decider.GotoRetrying.String(): 5,
	decider.GotoUpgrade.String():  6,
	decider.GotoDegrade.String():  7,
	decider.GotoShift.String():    8,
	decider.GotoTransfer.String(): 9,
}
