package checker

import (
	"errors"
	"fmt"
)

var Validator = &validator{}

type validator struct {
}

func (v *validator) ValidateProduceCheckpoint(checkpoints []ProduceCheckpoint) (map[CheckType][]*ProduceCheckpoint, error) {
	// 校验checker: checker可以在对应配置enable=false的情况下存在
	checkpointMap := make(map[CheckType][]*ProduceCheckpoint)
	for index, checkpoint := range checkpoints {
		if checkpoint.CheckType == "" {
			return nil, errors.New(" internal.CheckType can not be empty")
		}
		if v.isProduceCheckType(checkpoint.CheckType) {
			if checkpoint.CheckFunc == nil {
				return nil, errors.New(fmt.Sprintf("CheckFunc can not be nil for input checkOption: %s", checkpoint.CheckType))
			}
		}
		typedCheckpoints, ok := checkpointMap[checkpoint.CheckType]
		if !ok {
			typedCheckpoints = make([]*ProduceCheckpoint, 0)
		}
		if !v.containsProduceCheckpoint(typedCheckpoints, &checkpoints[index]) {
			typedCheckpoints = append(typedCheckpoints, &checkpoints[index])
			checkpointMap[checkpoint.CheckType] = typedCheckpoints
		}
	}
	return checkpointMap, nil
}

func (v *validator) containsProduceCheckpoint(checkpoints []*ProduceCheckpoint, target *ProduceCheckpoint) bool {
	for _, checkpoint := range checkpoints {
		if checkpoint.CheckType == target.CheckType &&
			fmt.Sprintf("%p", checkpoint.CheckFunc) == fmt.Sprintf("%p", target.CheckFunc) {
			return true
		}
	}
	return false
}

func (v *validator) ValidateConsumeCheckpoint(checkpoints []ConsumeCheckpoint) (map[CheckType][]*ConsumeCheckpoint, error) {
	// 校验checker: checker可以在对应配置enable=false的情况下存在
	checkpointMap := make(map[CheckType][]*ConsumeCheckpoint)
	for index, checkpoint := range checkpoints {
		if checkpoint.CheckType == "" {
			return nil, errors.New(" internal.CheckType can not be empty")
		}
		if v.isPrevStatusCheckType(checkpoint.CheckType) {
			if checkpoint.Prev == nil {
				return nil, errors.New(fmt.Sprintf("PrevHandleCheckFunc can not be nil for input checkOption: %s", checkpoint.CheckType))
			}
		} else if v.isPostStatusCheckType(checkpoint.CheckType) {
			if checkpoint.Post == nil {
				return nil, errors.New(fmt.Sprintf("PostHandleCheckFunc can not be nil for input checkOption: %s", checkpoint.CheckType))
			}
		} else if v.isPrevTransferCheckType(checkpoint.CheckType) {
			if checkpoint.Prev == nil {
				return nil, errors.New(fmt.Sprintf("prev check func can not be nil for input checkOption: %s", checkpoint.CheckType))
			}
		} else if v.isPostTransferCheckType(checkpoint.CheckType) {
			if checkpoint.Post == nil {
				return nil, errors.New(fmt.Sprintf("post check func can not be nil for input checkOption: %s", checkpoint.CheckType))
			}
		}
		typedCheckpoints, ok := checkpointMap[checkpoint.CheckType]
		if !ok {
			typedCheckpoints = make([]*ConsumeCheckpoint, 0)

		}
		if !v.containsConsumeCheckpoint(typedCheckpoints, &checkpoints[index]) {
			typedCheckpoints = append(typedCheckpoints, &checkpoints[index])
			checkpointMap[checkpoint.CheckType] = typedCheckpoints
		}
	}
	return checkpointMap, nil
}

func (v *validator) containsConsumeCheckpoint(checkpoints []*ConsumeCheckpoint, target *ConsumeCheckpoint) bool {
	for _, checkpoint := range checkpoints {
		if checkpoint.CheckType != target.CheckType {
			continue
		}
		if v.isPrevConsumeCheckType(target.CheckType) &&
			fmt.Sprintf("%p", checkpoint.Prev) == fmt.Sprintf("%p", target.Prev) {
			return true
		}
		if v.isPostConsumeCheckType(target.CheckType) &&
			fmt.Sprintf("%p", checkpoint.Post) == fmt.Sprintf("%p", target.Post) {
			return true
		}
	}
	return false
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

func (v *validator) isPrevConsumeCheckType(checkType CheckType) bool {
	for _, ct := range DefaultPrevHandleCheckOrders() {
		if ct == checkType {
			return true
		}
	}
	return false
}

func (v *validator) isPostConsumeCheckType(checkType CheckType) bool {
	for _, ct := range DefaultPostHandleCheckOrders() {
		if ct == checkType {
			return true
		}
	}
	return false
}

func (v *validator) isPrevStatusCheckType(checkType CheckType) bool {
	for _, ct := range DefaultPrevHandleCheckOrders() {
		if v.isPrevTransferCheckType(ct) {
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
		if v.isPostTransferCheckType(ct) {
			continue
		}
		if ct == checkType {
			return true
		}
	}
	return false
}

func (v *validator) isPrevTransferCheckType(checkType CheckType) bool {
	return checkType == CheckTypePrevTransfer
}

func (v *validator) isPostTransferCheckType(checkType CheckType) bool {
	return checkType == CheckTypePostTransfer
}
