package util

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/shenqianjin/soften-client-go/perf/internal/support/param"
	"github.com/shenqianjin/soften-client-go/soften/handler"
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
	if weight := gotoWeights[GotoIndexes[handler.StatusDone.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusDone.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusDiscard.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusDiscard.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusDead.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusDead.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusPending.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusPending.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusBlocking.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusBlocking.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusRetrying.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusRetrying.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusUpgrade.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusUpgrade.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusDegrade.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusDegrade.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusShift.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusShift.GetGoto().String()] = weight
	}
	if weight := gotoWeights[GotoIndexes[handler.StatusTransfer.GetGoto().String()]]; weight > 0 {
		gotoWeightMap[handler.StatusTransfer.GetGoto().String()] = weight
	}
	//
	if gotoParam.DoneWeight > 0 {
		gotoWeightMap[handler.StatusDone.GetGoto().String()] = gotoParam.DoneWeight
	}
	if gotoParam.DiscardWeight > 0 {
		gotoWeightMap[handler.StatusDiscard.GetGoto().String()] = gotoParam.DiscardWeight
	}
	if gotoParam.DeadWeight > 0 {
		gotoWeightMap[handler.StatusDead.GetGoto().String()] = gotoParam.DeadWeight
	}
	if gotoParam.PendingWeight > 0 {
		gotoWeightMap[handler.StatusPending.GetGoto().String()] = gotoParam.PendingWeight
	}
	if gotoParam.BlockingWeight > 0 {
		gotoWeightMap[handler.StatusBlocking.GetGoto().String()] = gotoParam.BlockingWeight
	}
	if gotoParam.RetryingWeight > 0 {
		gotoWeightMap[handler.StatusRetrying.GetGoto().String()] = gotoParam.RetryingWeight
	}
	if gotoParam.UpgradeWeight > 0 {
		gotoWeightMap[handler.StatusUpgrade.GetGoto().String()] = gotoParam.UpgradeWeight
	}
	if gotoParam.DegradeWeight > 0 {
		gotoWeightMap[handler.StatusDegrade.GetGoto().String()] = gotoParam.DegradeWeight
	}
	if gotoParam.ShiftWeight > 0 {
		gotoWeightMap[handler.StatusShift.GetGoto().String()] = gotoParam.ShiftWeight
	}
	if gotoParam.TransferWeight > 0 {
		gotoWeightMap[handler.StatusTransfer.GetGoto().String()] = gotoParam.TransferWeight
	}
	return gotoWeightMap
}

// ------ helper ------

var GotoIndexes = map[string]int{
	handler.StatusDone.GetGoto().String():     0,
	handler.StatusDiscard.GetGoto().String():  1,
	handler.StatusDead.GetGoto().String():     2,
	handler.StatusPending.GetGoto().String():  3,
	handler.StatusBlocking.GetGoto().String(): 4,
	handler.StatusRetrying.GetGoto().String(): 5,
	handler.StatusUpgrade.GetGoto().String():  6,
	handler.StatusDegrade.GetGoto().String():  7,
	handler.StatusShift.GetGoto().String():    8,
	handler.StatusTransfer.GetGoto().String(): 9,
}
