package checker

import (
	"errors"
	"fmt"
)

var Validator = &validator{}

type validator struct {
}

func (v *validator) ValidateProduceCheckpoint(checkpoints []ProduceCheckpoint) (map[CheckType]*ProduceCheckpoint, error) {
	// 校验checker: checker可以在对应配置enable=false的情况下存在
	checkpointMap := make(map[CheckType]*ProduceCheckpoint)
	for index, checkOpt := range checkpoints {
		if checkOpt.CheckType == "" {
			return nil, errors.New(" internal.CheckType can not be empty")
		}
		if v.isProduceCheckType(checkOpt.CheckType) {
			if checkOpt.CheckFunc == nil {
				return nil, errors.New(fmt.Sprintf("CheckFunc can not be nil for input checkOption: %s", checkOpt.CheckType))
			}
		}
		checkpointMap[checkOpt.CheckType] = &checkpoints[index]
	}
	return checkpointMap, nil
}

func (v *validator) ValidateConsumeCheckpoint(checkpoints []ConsumeCheckpoint) (map[CheckType]*ConsumeCheckpoint, error) {
	// 校验checker: checker可以在对应配置enable=false的情况下存在
	checkpointMap := make(map[CheckType]*ConsumeCheckpoint)
	for index, checkOpt := range checkpoints {
		if checkOpt.CheckType == "" {
			return nil, errors.New(" internal.CheckType can not be empty")
		}
		if v.isPrevStatusCheckType(checkOpt.CheckType) {
			if checkOpt.Prev == nil {
				return nil, errors.New(fmt.Sprintf("PrevHandleCheckFunc can not be nil for input checkOption: %s", checkOpt.CheckType))
			}
		} else if v.isPostStatusCheckType(checkOpt.CheckType) {
			if checkOpt.Post == nil {
				return nil, errors.New(fmt.Sprintf("PostHandleCheckFunc can not be nil for input checkOption: %s", checkOpt.CheckType))
			}
		} else if v.isPrevRerouteCheckType(checkOpt.CheckType) {
			if checkOpt.Prev == nil {
				return nil, errors.New(fmt.Sprintf("prev check func can not be nil for input checkOption: %s", checkOpt.CheckType))
			}
		} else if v.isPostRerouteCheckType(checkOpt.CheckType) {
			if checkOpt.Post == nil {
				return nil, errors.New(fmt.Sprintf("post check func can not be nil for input checkOption: %s", checkOpt.CheckType))
			}
		}
		checkpointMap[checkOpt.CheckType] = &checkpoints[index]
	}
	return checkpointMap, nil
}

func (v *validator) findCheckpointByType(checkpointMap map[CheckType]*ConsumeCheckpoint, checkTypes ...CheckType) *ConsumeCheckpoint {
	for _, checkType := range checkTypes {
		if opt, ok := checkpointMap[checkType]; ok {
			return opt
		}
	}
	return nil
}

func (v *validator) isProduceCheckType(checkType CheckType) bool {
	for _, ct := range DefaultPrevSendCheckOrders() {
		if ct == checkType {
			return true
		}
	}
	return false
}

func (v *validator) isPrevStatusCheckType(checkType CheckType) bool {
	for _, ct := range DefaultPrevHandleCheckOrders() {
		if v.isPrevRerouteCheckType(ct) {
			continue
		}
		if ct == checkType {
			return true
		}
	}
	return false
}

func (v *validator) isPostStatusCheckType(checkType CheckType) bool {
	for _, ct := range DefaultPostHandleCheckOrders() {
		if v.isPostRerouteCheckType(ct) {
			continue
		}
		if ct == checkType {
			return true
		}
	}
	return false
}

func (v *validator) isPrevRerouteCheckType(checkType CheckType) bool {
	return checkType == CheckTypePrevReroute
}

func (v *validator) isPostRerouteCheckType(checkType CheckType) bool {
	return checkType == CheckTypePostReroute
}
