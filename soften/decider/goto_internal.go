package decider

import (
	"errors"
	"fmt"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ decide goto enums ------

const (
	GotoBlocking = internal.DecideGoto("Blocking")
	GotoPending  = internal.DecideGoto("Pending")
	GotoRetrying = internal.DecideGoto("Retrying")
	GotoDead     = internal.DecideGoto("Dead")
	GotoDone     = internal.DecideGoto("Done")
	GotoDiscard  = internal.DecideGoto("Discard")
	GotoUpgrade  = internal.DecideGoto("Upgrade")  // 升级
	GotoDegrade  = internal.DecideGoto("Degrade")  // 降级
	GotoShift    = internal.DecideGoto("Shift")    // 变换级别
	GotoTransfer = internal.DecideGoto("Transfer") // 转移队列
)

func GotoOf(handleGoto string) (internal.DecideGoto, error) {
	for _, v := range GotoValues() {
		if v.String() == handleGoto {
			return v, nil
		}
	}
	return "", errors.New(fmt.Sprintf("invalid handle goto: %s", handleGoto))
}

func GotoValues() []internal.DecideGoto {
	values := []internal.DecideGoto{
		GotoDone, GotoDead, GotoDiscard,
		GotoBlocking, GotoPending, GotoRetrying,
		GotoShift, GotoUpgrade, GotoDegrade,
		GotoTransfer,
	}
	return values
}
