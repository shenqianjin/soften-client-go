package decider

import (
	"errors"
	"fmt"
	"time"

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

// ------ Goto Decision interface [used by interceptors] ------

type Decision interface {
	GetGoto() internal.DecideGoto
	GetGotoExtra() GotoExtra
	GetErr() error
}

// ------ Goto Extra information ------

type GotoExtra struct {
	// Optional: specifies which topic does the application wants to transfer the message to.
	// It only works for GotoTransfer.
	//
	// It is required if the topic is missing in transfer policy configuration.
	//
	// It is not recommended to specify topic name casually, the application should try its best
	// to keep all topics matching with soften topic patterns.
	Topic string

	// Optional: specifies which level does the application wants to shift the message to.
	// It only works for GotoShift, GotoUpgrade and GotoDegrade.
	//
	// It is required if the level is missing in shift, upgrade or degrade policy configuration.
	Level internal.TopicLevel

	// Optional: specifies the consume-time which the message can be consumed after it be routed.
	// It is only works for persistent HandleStatus.
	//
	// Please note it is not a good idea if you specify different consume-time on different message for a same topic.
	// the implementation of Soften WithConsumeTime is sleep to wait until it is the WithConsumeTime of top message on a topic.
	// If you specified a time before the WithConsumeTime of the top message, it will be consumed after the WithConsumeTime
	// of the top message as well.
	ConsumeTime time.Time
}
