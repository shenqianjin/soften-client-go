package limit

import (
	"testing"

	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/sirupsen/logrus"
)

func TestConsumeLimit_OPS_Ready(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			Ready:      &config.ReadyPolicy{ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))}},
			DeadEnable: config.False(),
			Done:       &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_OPS_Pending(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			Ready:         &config.ReadyPolicy{ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))}},
			PendingEnable: config.ToPointer(true),
			Pending:       &config.StatusPolicy{ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))}},
			DeadEnable:    config.False(),
			Done:          &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady, message.StatusPending},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_OPS_Blocking(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			Ready:          &config.ReadyPolicy{ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))}},
			BlockingEnable: config.ToPointer(true),
			Blocking:       &config.StatusPolicy{ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))}},
			DeadEnable:     config.False(),
			Done:           &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady, message.StatusBlocking},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_OPS_Retrying(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			Ready:          &config.ReadyPolicy{ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))}},
			RetryingEnable: config.ToPointer(true),
			Retrying:       &config.StatusPolicy{ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))}},
			DeadEnable:     config.False(),
			Done:           &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady, message.StatusRetrying},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_OPS_L1(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L1: &config.LevelPolicy{
			ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))},
			DeadEnable:   config.False(),
			Done:         &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L1},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_OPS_L2(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L2: &config.LevelPolicy{
			ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))},
			DeadEnable:   config.False(),
			Done:         &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L2},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_OPS_MultiLevels(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L2: &config.LevelPolicy{
			ConsumeLimit: &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))},
			DeadEnable:   config.False(),
			Done:         &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L2, message.L3, message.B2},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_OPS_ConsumerLevel_L2(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L2: &config.LevelPolicy{
			DeadEnable: config.False(),
			Done:       &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L2},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		consumerLimit:    &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))},
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}

func TestConsumeLimit_OPS_ConsumerLevel_MultiLevels(t *testing.T) {
	leveledPolicies := config.LevelPolicies{
		message.L2: &config.LevelPolicy{
			DeadEnable: config.False(),
			Done:       &config.DonePolicy{LogLevel: logrus.WarnLevel.String()},
		},
	}
	groundTopic := internal.GenerateTestTopic(internal.PrefixTestConsumeLimit)
	testCase := testConsumeLimitCase{
		groundTopic:      groundTopic,
		levels:           message.Levels{message.L2, message.L3, message.B2},
		statuses:         message.Statuses{message.StatusReady},
		leveledPolicies:  leveledPolicies,
		consumerLimit:    &config.LimitPolicy{MaxOPS: config.ToPointer(uint(30))},
		MsgCountPerTopic: 300,
		MaxOPS:           30,
	}
	testConsumeLimit(t, testCase)
}
