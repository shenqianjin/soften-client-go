package handler

import (
	"errors"
	"fmt"
	"time"

	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ handle goto enums ------

var (
	StatusBlocking = newPersistentHandleStatus(decider.GotoBlocking)
	StatusPending  = newPersistentHandleStatus(decider.GotoPending)
	StatusRetrying = newPersistentHandleStatus(decider.GotoRetrying)
	StatusDead     = newPersistentHandleStatus(decider.GotoDead)
	StatusDone     = newHandleStatus(decider.GotoDone)
	StatusDiscard  = newHandleStatus(decider.GotoDiscard)
	StatusUpgrade  = newLeveledHandleStatus(decider.GotoUpgrade)
	StatusDegrade  = newLeveledHandleStatus(decider.GotoDegrade)
	StatusShift    = newLeveledHandleStatus(decider.GotoShift)
	StatusTransfer = newTransferHandleStatus(decider.GotoTransfer)

	StatusAuto = handleStatus{} // handle message failure
)

func StatusOf(status string) (HandleStatus, error) {
	for _, v := range StatusValues() {
		if v.GetGoto().String() == status {
			return v, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("invalid handle status: %s", status))
}

func StatusValues() []HandleStatus {
	values := []HandleStatus{
		StatusBlocking, StatusPending, StatusRetrying,
		StatusDead, StatusDone, StatusDiscard,
		StatusUpgrade, StatusDegrade, StatusShift,
		StatusTransfer,
	}
	return values
}

// ------ handle status ------

type HandleStatus interface {
	GetGoto() internal.DecideGoto
	GetCheckTypes() []checker.CheckType
	GetErr() error
	GetGotoExtra() decider.GotoExtra
}

// ------ handleStatus impl ------

type handleStatus struct {
	decideGoto      internal.DecideGoto // 状态转移至新状态: 默认为空; 与当前状态一致时，参数无效。优先级比 checkTypes 高。
	checkTypes      []checker.CheckType // 事后检查类型列表: 默认 DefaultPostCheckTypesInTurn; 指定时，使用指定类型列表。优先级比 decideGoto 低。
	err             error               // 后置校验器如果需要依赖处理错误，通过该参数传递。框架层在处理的过程不会更新err内容。
	decideGotoExtra decider.GotoExtra
}

func newHandleStatus(decideGoto internal.DecideGoto) handleStatus {
	return handleStatus{decideGoto: decideGoto}
}

func (h handleStatus) GetGoto() internal.DecideGoto {
	return h.decideGoto
}

func (h handleStatus) GetCheckTypes() []checker.CheckType {
	return h.checkTypes
}

func (h handleStatus) GetErr() error {
	return h.err
}
func (h handleStatus) GetGotoExtra() decider.GotoExtra {
	return h.decideGotoExtra
}

// WithCheckTypes specifies these post check types when handler executed without an end result.
func (h handleStatus) WithCheckTypes(types []checker.CheckType) handleStatus {
	return handleStatus{decideGoto: h.decideGoto, checkTypes: types, err: h.err, decideGotoExtra: h.decideGotoExtra}
}

// WithErr passes the error happened in handle func, if any of post checkers need it for some logic.
func (h handleStatus) WithErr(err error) handleStatus {
	return handleStatus{decideGoto: h.decideGoto, checkTypes: h.checkTypes, err: err, decideGotoExtra: h.decideGotoExtra}
}

// ------ extra handle status ------

type persistentHandleStatus struct {
	handleStatus
}

func newPersistentHandleStatus(decideGoto internal.DecideGoto) persistentHandleStatus {
	return persistentHandleStatus{handleStatus: newHandleStatus(decideGoto)}
}

// WithConsumeTime specifies the consume-time which the message can be consumed after it be routed.
// It only works for persistent HandleStatus.
//
// Please note it is not a good idea if you specify different consume-time on different message for a same topic.
// the implementation of Soften WithConsumeTime is sleep to wait until it is the WithConsumeTime of top message on a topic.
// If you specified a time before the WithConsumeTime of the top message, it will be consumed after the WithConsumeTime
// of the top message as well.
func (h persistentHandleStatus) WithConsumeTime(consumeTime time.Time) persistentHandleStatus {
	status := h.handleStatus
	status.decideGotoExtra.ConsumeTime = consumeTime
	return persistentHandleStatus{handleStatus: status}
}

// ------ leveled handle status ------

type leveledHandleStatus struct {
	persistentHandleStatus
}

func newLeveledHandleStatus(decideGoto internal.DecideGoto) leveledHandleStatus {
	return leveledHandleStatus{persistentHandleStatus: newPersistentHandleStatus(decideGoto)}
}

// WithLevel specifies which level does the application wants to shift the message to.
// It only works for StatusShift, StatusUpgrade and StatusDegrade.
func (h leveledHandleStatus) WithLevel(level internal.TopicLevel) leveledHandleStatus {
	status := h.persistentHandleStatus
	status.decideGotoExtra.Level = level
	return leveledHandleStatus{persistentHandleStatus: status}
}

// ------ transfer handle status ------

type transferHandleStatus struct {
	persistentHandleStatus
}

func newTransferHandleStatus(decideGoto internal.DecideGoto) transferHandleStatus {
	return transferHandleStatus{persistentHandleStatus: newPersistentHandleStatus(decideGoto)}
}

// WithTopic specifies which topic does the application wants to transfer the message to.
// It only works for StatusTransfer.
func (h transferHandleStatus) WithTopic(topic string) transferHandleStatus {
	status := h.persistentHandleStatus
	status.decideGotoExtra.Topic = topic
	return transferHandleStatus{persistentHandleStatus: status}
}
