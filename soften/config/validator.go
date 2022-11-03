package config

import (
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/shenqianjin/soften-client-go/soften/internal"
	"github.com/shenqianjin/soften-client-go/soften/internal/backoff"
	"github.com/shenqianjin/soften-client-go/soften/message"
	"github.com/sirupsen/logrus"
)

// ------ configuration validator ------

var Validator = &validator{}

type validator struct {
}

func (v *validator) ValidateAndDefaultClientConfig(conf *ClientConfig) error {
	if conf.URL == "" {
		return errors.New("URL is blank")
	}
	// default logger
	if conf.Logger == nil {
		conf.Logger = log.NewLoggerWithLogrus(logrus.StandardLogger())
	}
	// default metrics cardinality
	if conf.MetricsCardinality == 0 {
		conf.MetricsCardinality = pulsar.MetricsCardinalityTopic
	}

	return nil
}

func (v *validator) ValidateAndDefaultConsumerConfig(conf *ConsumerConfig) error {
	// default WithLevel
	if conf.Level == "" && len(conf.Levels) == 0 {
		conf.Levels = []internal.TopicLevel{message.L1}
		conf.Level = conf.Levels[0]
	} else if len(conf.Levels) == 0 {
		conf.Levels = []internal.TopicLevel{conf.Level}
	} else if conf.Level == "" {
		conf.Level = conf.Levels[0]
	}
	if len(conf.Levels) > 0 && conf.Level != "" {
		if conf.Levels[0] != conf.Level {
			return errors.New("core level is not match between level and levels configuration")
		}
	}
	for _, level := range conf.Levels {
		if level == "" {
			return errors.New("level in levels configuration cannot be blank")
		}
	}
	// default leveled balance
	if conf.LevelBalanceStrategy == "" {
		conf.LevelBalanceStrategy = BalanceStrategyRoundRand
	}
	// default status balance strategy
	if conf.BalanceStrategy == "" {
		conf.BalanceStrategy = BalanceStrategyRoundRand
	}

	// default topics
	if len(conf.Topics) == 0 && conf.Topic == "" {
		return errors.New("no topic found in your configuration")
	} else if conf.Topic != "" {
		conf.Topics = []string{conf.Topic}
	}
	if len(conf.Topics) >= 1 && conf.Topic != "" {
		if conf.Topics[0] != conf.Topic {
			return errors.New("core topic is not match between topic and topics configuration")
		}
	}
	// default and valid main level policy
	if conf.LevelPolicy == nil {
		if len(conf.LevelPolicies) > 0 {
			conf.LevelPolicy = conf.LevelPolicies[conf.Level]
		} else {
			conf.LevelPolicy = &LevelPolicy{}
		}
	}
	if err := v.validateAndDefaultPolicyProps4MainLevel(conf.LevelPolicy, conf.Level); err != nil {
		return err
	}

	// validate and default other levels
	if len(conf.Levels) > 0 {
		// validate levels
		for _, level := range conf.Levels {
			if level == "" {
				return errors.New(fmt.Sprintf("exists blank level for levels: %v", conf.Levels))
			}
			if !message.Exists(level) {
				return errors.New(fmt.Sprintf("exists not supported level: %v for levels: %v", level, conf.Levels))
			}
			/*if level == topic.L1 {
				return errors.New(fmt.Sprintf("invalid extra multi-level (L1) in levels: %v", conf.Levels))
			}*/
		}
		// default and validate multi-level policies
		if conf.LevelPolicies == nil {
			conf.LevelPolicies = make(map[internal.TopicLevel]*LevelPolicy, len(conf.Levels))
		}
		// init main level policy
		conf.LevelPolicies[conf.Level] = conf.LevelPolicy
		// validate and default extra levels
		for _, level := range conf.Levels {
			// main level has already be valid in outer layer
			if level == conf.Level {
				continue
			}
			// init level policies
			if _, ok := conf.LevelPolicies[level]; !ok {
				conf.LevelPolicies[level] = &LevelPolicy{}
			}
			policy := conf.LevelPolicies[level]
			// default consume weight for multi-level
			if policy.ConsumeWeight == 0 {
				// default weight by default calculate algorithm
				policy.ConsumeWeight = defaultLeveledWeightFunc(level)
			}
			// default and valid main multi-level policy
			if err := v.validateAndDefaultPolicyProps4ExtraLevel(policy, conf.LevelPolicy); err != nil {
				return err
			}
		}
	}

	// default concurrency policy
	if conf.Concurrency == nil {
		conf.Concurrency = defaultConcurrencyPolicy
	} else if err := v.validateAndDefaultConcurrencyPolicy(conf.Concurrency, defaultConcurrencyPolicy); err != nil {
		return err
	}
	// default escape handler
	if conf.EscapeHandler == nil {
		if err := v.validateAndDefaultEscapeHandler(conf); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validateAndDefaultEscapeHandler(conf *ConsumerConfig) error {
	if conf.EscapeHandler != nil {
		return nil
	}
	switch conf.EscapeHandleType {
	case EscapeAsPanic:
	case EscapeAsAck:
	case EscapeAsNack:
	default:
		conf.EscapeHandleType = EscapeAsPanic
	}
	return nil
}

func (v *validator) validateAndDefaultPolicyProps4MainLevel(policy *LevelPolicy, mainLevel internal.TopicLevel) error {
	// default status switches
	if policy.BlockingEnable == nil {
		policy.BlockingEnable = new(bool)
	}
	if policy.RetryingEnable == nil {
		policy.RetryingEnable = new(bool)
	}
	if policy.PendingEnable == nil {
		policy.PendingEnable = new(bool)
	}
	if policy.DeadEnable == nil {
		policy.DeadEnable = new(bool)
		*policy.DeadEnable = true
	}
	if policy.DiscardEnable == nil {
		policy.DiscardEnable = new(bool)
		*policy.DiscardEnable = true
	}
	// default shift/transfer switches
	if policy.ShiftEnable == nil {
		policy.ShiftEnable = new(bool)
	}
	if policy.UpgradeEnable == nil {
		policy.UpgradeEnable = new(bool)
	}
	if policy.DegradeEnable == nil {
		policy.DegradeEnable = new(bool)
	}
	if policy.TransferEnable == nil {
		policy.TransferEnable = new(bool)
	}
	// default status Policy
	if err := v.validateAndDefaultReadyPolicy(policy.Ready, defaultStatusReadyPolicy); err != nil {
		return err
	}
	// default and valid pending policy
	if *policy.PendingEnable {
		if policy.Pending == nil {
			policy.Pending = defaultStatusPolicyPending
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Pending, defaultStatusPolicyPending); err != nil {
			return err
		}
	}
	if *policy.BlockingEnable {
		if policy.Blocking == nil {
			policy.Blocking = defaultStatusPolicyBlocking
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Blocking, defaultStatusPolicyBlocking); err != nil {
			return err
		}
	}
	if *policy.RetryingEnable {
		if policy.Retrying == nil {
			policy.Retrying = defaultStatusPolicyRetrying
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Retrying, defaultStatusPolicyRetrying); err != nil {
			return err
		}
	}
	// validate and default transfer policy
	if *policy.TransferEnable {
		if err := v.validateAndDefaultTransferPolicy(policy.Transfer, defaultTransferPolicy); err != nil {
			return err
		}
	}
	// validate and default upgrade policy
	if *policy.UpgradeEnable {
		if err := v.validateAndDefaultUpgradePolicy(mainLevel, policy.Upgrade, defaultUpgradePolicy); err != nil {
			return err
		}
	}
	// validate and default degrade policy
	if *policy.DegradeEnable {
		if err := v.validateAndDefaultDegradePolicy(mainLevel, policy.Degrade, defaultDegradePolicy); err != nil {
			return err
		}
	}
	// validate and default shift policy
	if *policy.ShiftEnable {
		if err := v.validateAndDefaultShiftPolicy(mainLevel, policy.Shift, defaultShiftPolicy); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validateAndDefaultPolicyProps4ExtraLevel(policy *LevelPolicy, mainPolicy *LevelPolicy) error {
	// default status switches
	if policy.BlockingEnable == nil {
		policy.BlockingEnable = new(bool)
		*policy.BlockingEnable = *mainPolicy.BlockingEnable
	}
	if policy.RetryingEnable == nil {
		policy.RetryingEnable = new(bool)
		*policy.RetryingEnable = *mainPolicy.RetryingEnable
	}
	if policy.PendingEnable == nil {
		policy.PendingEnable = new(bool)
		*policy.PendingEnable = *mainPolicy.PendingEnable
	}
	if policy.DeadEnable == nil {
		policy.DeadEnable = new(bool)
		*policy.DeadEnable = *mainPolicy.DeadEnable
	}
	if policy.DiscardEnable == nil {
		policy.DiscardEnable = new(bool)
		*policy.DiscardEnable = *mainPolicy.DiscardEnable
	}
	// default shift/transfer switches
	if policy.ShiftEnable == nil {
		policy.ShiftEnable = new(bool)
		*policy.ShiftEnable = *mainPolicy.ShiftEnable
	}
	if policy.UpgradeEnable == nil {
		policy.UpgradeEnable = new(bool)
		*policy.UpgradeEnable = *mainPolicy.UpgradeEnable
	}
	if policy.DegradeEnable == nil {
		policy.DegradeEnable = new(bool)
		*policy.DegradeEnable = *mainPolicy.DegradeEnable
	}
	if policy.TransferEnable == nil {
		policy.TransferEnable = new(bool)
		*policy.TransferEnable = *mainPolicy.TransferEnable
	}
	// default status Policy
	if policy.Ready == nil {
		policy.Ready = mainPolicy.Ready
	}
	if err := v.validateAndDefaultReadyPolicy(policy.Ready, mainPolicy.Ready); err != nil {
		return err
	}
	// default and valid pending policy
	if *policy.PendingEnable {
		if policy.Pending == nil {
			policy.Pending = mainPolicy.Pending
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Pending, mainPolicy.Pending); err != nil {
			return err
		}
	}
	if *policy.BlockingEnable {
		if policy.Blocking == nil {
			policy.Blocking = mainPolicy.Blocking
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Blocking, mainPolicy.Blocking); err != nil {
			return err
		}
	}
	if *policy.RetryingEnable {
		if policy.Retrying == nil {
			policy.Retrying = mainPolicy.Retrying
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Retrying, mainPolicy.Retrying); err != nil {
			return err
		}
	}
	// validate and default transfer policy
	if *policy.TransferEnable {
		if err := v.validateAndDefaultTransferPolicy(policy.Transfer, mainPolicy.Transfer); err != nil {
			return err
		}
	}
	// validate and default upgrade policy
	if *policy.UpgradeEnable {
		if policy.Upgrade == nil {
			policy.Upgrade = mainPolicy.Upgrade
		}
		if err := v.validateAndDefaultUpgradePolicy(message.L1, policy.Upgrade, mainPolicy.Upgrade); err != nil {
			return err
		}
	}
	// validate and default degrade policy
	if *policy.DegradeEnable {
		if policy.Degrade == nil {
			policy.Degrade = mainPolicy.Degrade
		}
		if err := v.validateAndDefaultDegradePolicy(message.L1, policy.Degrade, mainPolicy.Degrade); err != nil {
			return err
		}
	}
	// validate and default shift policy
	if *policy.ShiftEnable {
		if policy.Shift == nil {
			policy.Shift = mainPolicy.Shift
		}
		if err := v.validateAndDefaultShiftPolicy(message.L1, policy.Shift, mainPolicy.Shift); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validateAndDefaultShiftPolicy(mainLevel internal.TopicLevel, configuredPolicy *ShiftPolicy, defaultPolicy *ShiftPolicy) error {
	if configuredPolicy == nil {
		configuredPolicy = &ShiftPolicy{}
		configuredPolicy = defaultPolicy
		return nil
	}
	/*if configuredPolicy == nil {
		return errors.New(fmt.Sprintf("missing degrade configuredPolicy when degrade is enable for consume level: %v", consumeLevel))
	}
	if configuredPolicy.WithLevel == "" {
		return errors.New(fmt.Sprintf("degrade level is blank for consume level: %v", consumeLevel))
	}*/
	if configuredPolicy.Level != "" {
		if !message.Exists(configuredPolicy.Level) {
			return errors.New(fmt.Sprintf("not supported topic level: %v for main level: %v", configuredPolicy.Level, mainLevel))
		}
		if configuredPolicy.Level == mainLevel {
			return errors.New(fmt.Sprintf("destination level cannot be same with the main level: %v", mainLevel))
		}
	}
	// default publish policy
	v.defaultPublishPolicy(configuredPolicy.PublishPolicy, newDefaultPublishPolicy())

	return nil
}

func (v *validator) validateAndDefaultTransferPolicy(configuredPolicy *TransferPolicy, defaultPolicy *TransferPolicy) error {
	if configuredPolicy == nil {
		configuredPolicy = defaultPolicy
		return nil
	}
	// count 默认0 无需更改
	// default publish policy
	v.defaultPublishPolicy(configuredPolicy.PublishPolicy, newDefaultPublishPolicy())
	return nil
}

func (v *validator) validateAndDefaultUpgradePolicy(mainLevel internal.TopicLevel, configuredPolicy *ShiftPolicy, defaultPolicy *ShiftPolicy) error {
	if err := v.validateAndDefaultShiftPolicy(mainLevel, configuredPolicy, defaultPolicy); err != nil {
		return err
	}
	if configuredPolicy.Level != "" {
		if configuredPolicy.Level.OrderOf() <= mainLevel.OrderOf() {
			return errors.New(fmt.Sprintf("upgrade level [%v] cannot be lower or equal than the main level [%v]",
				configuredPolicy.Level, mainLevel))
		}
	}
	return nil
}

func (v *validator) validateAndDefaultDegradePolicy(mainLevel internal.TopicLevel, configuredPolicy *ShiftPolicy, defaultPolicy *ShiftPolicy) error {
	if err := v.validateAndDefaultShiftPolicy(mainLevel, configuredPolicy, defaultPolicy); err != nil {
		return err
	}
	if configuredPolicy.Level != "" {
		if configuredPolicy.Level.OrderOf() >= mainLevel.OrderOf() {
			return errors.New(fmt.Sprintf("degrade level [%v] cannot be higher or equal than the main level [%v]",
				configuredPolicy.Level, mainLevel))
		}
	}
	return nil
}

func (v *validator) ValidateAndDefaultProducerConfig(conf *ProducerConfig) error {
	if conf.Topic == "" {
		return errors.New("topic is blank")
	}
	// default status switches
	if conf.DeadEnable == nil {
		conf.DeadEnable = new(bool)
	}
	if conf.DiscardEnable == nil {
		conf.DiscardEnable = new(bool)
	}
	// default shift/transfer switches
	if conf.ShiftEnable == nil {
		conf.ShiftEnable = new(bool)
	}
	if conf.UpgradeEnable == nil {
		conf.UpgradeEnable = new(bool)
	}
	if conf.DegradeEnable == nil {
		conf.DegradeEnable = new(bool)
	}
	if conf.TransferEnable == nil {
		conf.TransferEnable = new(bool)
	}

	// default level
	if conf.Level == "" {
		conf.Level = message.L1
	}
	// default backoff policy
	v.defaultPublishPolicy(conf.Publish, newDefaultPublishPolicy())

	// validate dead: default dead to D1
	if *conf.DeadEnable {
		if conf.Dead == nil {
			conf.Dead = defaultShiftDeadPolicy
		}
		if err := v.validateAndDefaultShiftPolicy(conf.Level, conf.Dead, defaultShiftDeadPolicy); err != nil {
			return err
		}
	}
	// validate upgrade: default nothing
	if *conf.UpgradeEnable {
		if conf.Upgrade == nil {
			conf.Upgrade = defaultUpgradePolicy
		}
		if err := v.validateAndDefaultUpgradePolicy(conf.Level, conf.Upgrade, defaultUpgradePolicy); err != nil {
			return err
		}
	}
	// validate degrade: default nothing
	if *conf.DegradeEnable {
		if conf.Degrade == nil {
			conf.Degrade = defaultDegradePolicy
		}
		if err := v.validateAndDefaultDegradePolicy(conf.Level, conf.Degrade, defaultDegradePolicy); err != nil {
			return err
		}
	}
	// validate shift: default nothing
	if *conf.ShiftEnable {
		if conf.Shift == nil {
			conf.Shift = defaultShiftPolicy
		}
		if err := v.validateAndDefaultShiftPolicy(conf.Level, conf.Shift, defaultShiftPolicy); err != nil {
			return err
		}
	}
	// validate and default transfer
	if *conf.TransferEnable {
		if conf.Transfer == nil {
			conf.Transfer = defaultTransferPolicy
		}
	}
	return nil
}

func (v *validator) validateAndDefaultReadyPolicy(configuredPolicy *ReadyPolicy, defaultPolicy *ReadyPolicy) error {
	if configuredPolicy == nil {
		configuredPolicy = defaultPolicy
		return nil
	}
	if configuredPolicy.ConsumeWeight == 0 {
		configuredPolicy.ConsumeWeight = defaultPolicy.ConsumeWeight
	}
	return nil
}

func (v *validator) validateAndDefaultStatusPolicy(configuredPolicy *StatusPolicy, defaultPolicy *StatusPolicy) error {
	if configuredPolicy == nil {
		configuredPolicy = defaultPolicy
		return nil
	}
	if configuredPolicy.ConsumeWeight == 0 {
		configuredPolicy.ConsumeWeight = defaultPolicy.ConsumeWeight
	}
	if configuredPolicy.ConsumeMaxTimes == 0 {
		configuredPolicy.ConsumeMaxTimes = defaultPolicy.ConsumeMaxTimes
	}
	if configuredPolicy.BackoffDelays == nil && configuredPolicy.BackoffPolicy == nil {
		configuredPolicy.BackoffDelays = defaultPolicy.BackoffDelays
		configuredPolicy.BackoffPolicy = defaultPolicy.BackoffPolicy
	}
	if configuredPolicy.ReentrantDelay == 0 {
		configuredPolicy.ReentrantDelay = defaultPolicy.ReentrantDelay
	}
	if configuredPolicy.ReentrantMaxTimes == 0 {
		configuredPolicy.ReentrantMaxTimes = defaultPolicy.ReentrantMaxTimes
	}
	// default policy
	if configuredPolicy.BackoffPolicy == nil && configuredPolicy.BackoffDelays != nil {
		if backoffPolicy, err := backoff.NewAbbrStatusBackoffPolicy(configuredPolicy.BackoffDelays); err != nil {
			return err
		} else {
			configuredPolicy.BackoffDelays = nil // release unnecessary reference
			configuredPolicy.BackoffPolicy = backoffPolicy
		}
	}
	// default publish policy
	v.defaultPublishPolicy(configuredPolicy.PublishPolicy, newDefaultPublishPolicy())

	return nil
}

func (v *validator) validateAndDefaultConcurrencyPolicy(configuredPolicy *ConcurrencyPolicy, defaultPolicy *ConcurrencyPolicy) error {
	if configuredPolicy == nil {
		configuredPolicy = defaultPolicy
		return nil
	}
	if configuredPolicy.CorePoolSize <= 0 {
		configuredPolicy.CorePoolSize = defaultPolicy.CorePoolSize
	}
	if configuredPolicy.MaximumPoolSize <= 0 {
		configuredPolicy.MaximumPoolSize = defaultPolicy.MaximumPoolSize
	}
	if configuredPolicy.KeepAliveTime <= 0 {
		configuredPolicy.KeepAliveTime = defaultPolicy.KeepAliveTime
	}
	if configuredPolicy.PanicHandler == nil {
		configuredPolicy.PanicHandler = defaultPolicy.PanicHandler
	}
	return nil
}

func (v *validator) defaultPublishPolicy(configuredPolicy *PublishPolicy, defaultPolicy *PublishPolicy) error {
	// default publish policy
	if configuredPolicy == nil {
		configuredPolicy = defaultPolicy
		return nil
	}
	// enable max time
	if configuredPolicy.BackoffMaxTimes > 0 {
		if configuredPolicy.BackoffPolicy == nil {
			backoffDelays := defaultPolicy.BackoffDelays
			if len(configuredPolicy.BackoffDelays) > 0 {
				backoffDelays = configuredPolicy.BackoffDelays
			}
			if backoffPolicy, err := backoff.NewAbbrBackoffPolicy(backoffDelays); err != nil {
				return err
			} else {
				configuredPolicy.BackoffDelays = nil // release unnecessary reference
				configuredPolicy.BackoffPolicy = backoffPolicy
			}
		}
	}
	return nil
}
