package handler

import (
	"errors"
	"fmt"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

const (
	GotoBlocking = internal.HandleGoto("Blocking")
	GotoPending  = internal.HandleGoto("Pending")
	GotoRetrying = internal.HandleGoto("Retrying")
	GotoDead     = internal.HandleGoto("Dead")
	GotoDone     = internal.HandleGoto("Done")
	GotoDiscard  = internal.HandleGoto("Discard")
	GotoUpgrade  = internal.HandleGoto("Upgrade")
	GotoDegrade  = internal.HandleGoto("Degrade")
	// Reroute 只能通过checkpoint实现，不能在 HandleResult 中显示指定为 Goto
	//GotoReroute = internal.HandleGoto("Reroute")
)

func GotoOf(handleGoto string) (internal.HandleGoto, error) {
	for _, v := range GotoValues() {
		if v.String() == handleGoto {
			return v, nil
		}
	}
	return "", errors.New(fmt.Sprintf("invalid handle goto: %s", handleGoto))
}

func GotoValues() []internal.HandleGoto {
	values := []internal.HandleGoto{
		GotoDone, GotoDead, GotoDiscard,
		GotoBlocking, GotoPending, GotoRetrying,
		GotoUpgrade, GotoDegrade,
	}
	return values
}
