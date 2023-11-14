package internal

import (
	"errors"
	"fmt"
)

// ------ handle goto ------

type DecideGoto string

func (e DecideGoto) String() string {
	return string(e)
}

// ------ decide goto enums ------

const (
	GotoBlocking = DecideGoto("Blocking")
	GotoPending  = DecideGoto("Pending")
	GotoRetrying = DecideGoto("Retrying")
	GotoDead     = DecideGoto("Dead")
	GotoDone     = DecideGoto("Done")
	GotoDiscard  = DecideGoto("Discard")
	GotoUpgrade  = DecideGoto("Upgrade")  // 升级
	GotoDegrade  = DecideGoto("Degrade")  // 降级
	GotoShift    = DecideGoto("Shift")    // 变换级别
	GotoTransfer = DecideGoto("Transfer") // 转移队列
)

func GotoOf(handleGoto string) (DecideGoto, error) {
	for _, v := range GotoValues() {
		if v.String() == handleGoto {
			return v, nil
		}
	}
	return "", errors.New(fmt.Sprintf("invalid handle goto: %s", handleGoto))
}

func GotoValues() []DecideGoto {
	values := []DecideGoto{
		GotoDone, GotoDead, GotoDiscard,
		GotoBlocking, GotoPending, GotoRetrying,
		GotoShift, GotoUpgrade, GotoDegrade,
		GotoTransfer,
	}
	return values
}
