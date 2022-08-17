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
				if weight, ok2 := defaultLeveledWeights[level]; ok2 {
					policy.ConsumeWeight = weight
				} else {
					policy.ConsumeWeight = defaultLeveledConsumeWeightMain
				}
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
	// default status Policy
	if policy.Ready == nil {
		policy.Ready = defaultStatusPolicyReady
	}
	if err := v.validateAndDefaultStatusPolicy(policy.Ready, defaultStatusPolicyReady); err != nil {
		return err
	}
	policy.Ready.TransferLevel = mainLevel
	// default and valid pending policy
	if policy.PendingEnable {
		if policy.Pending == nil {
			policy.Pending = defaultStatusPolicyPending
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Pending, defaultStatusPolicyPending); err != nil {
			return err
		}
		policy.Pending.TransferLevel = mainLevel
	}
	if policy.BlockingEnable {
		if policy.Blocking == nil {
			policy.Blocking = defaultStatusPolicyBlocking
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Blocking, defaultStatusPolicyBlocking); err != nil {
			return err
		}
		policy.Blocking.TransferLevel = mainLevel
	}
	if policy.RetryingEnable {
		if policy.Retrying == nil {
			policy.Retrying = defaultStatusPolicyRetrying
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Retrying, defaultStatusPolicyRetrying); err != nil {
			return err
		}
		policy.Retrying.TransferLevel = mainLevel
	}
	if policy.TransferEnable {
		if policy.Transfer == nil {
			policy.Transfer = defaultTransferPolicy
		}
	}
	// validate and default upgrade policy
	if policy.UpgradeEnable {
		if err := v.validateAndDefaultUpgradePolicy(message.L1, policy.Upgrade, defaultUpgradePolicy); err != nil {
			return err
		}
	}
	// validate and default degrade policy
	if policy.DegradeEnable {
		if err := v.validateAndDefaultDegradePolicy(message.L1, policy.Degrade, defaultDegradePolicy); err != nil {
			return err
		}
	}
	// validate and default shift policy
	if policy.ShiftEnable {
		if err := v.validateAndDefaultShiftPolicy(message.L1, policy.Shift, defaultShiftPolicy); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validateAndDefaultPolicyProps4ExtraLevel(policy *LevelPolicy, mainPolicy *LevelPolicy) error {
	// default status Policy
	if policy.Ready == nil {
		policy.Ready = mainPolicy.Ready
	}
	if err := v.validateAndDefaultStatusPolicy(policy.Ready, mainPolicy.Ready); err != nil {
		return err
	}
	// default and valid pending policy
	if policy.PendingEnable {
		if policy.Pending == nil {
			policy.Pending = mainPolicy.Pending
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Pending, defaultStatusPolicyPending); err != nil {
			return err
		}
	}
	if policy.BlockingEnable {
		if policy.Blocking == nil {
			policy.Blocking = mainPolicy.Blocking
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Blocking, defaultStatusPolicyBlocking); err != nil {
			return err
		}
	}
	if policy.RetryingEnable {
		if policy.Retrying == nil {
			policy.Retrying = mainPolicy.Retrying
		}
		if err := v.validateAndDefaultStatusPolicy(policy.Retrying, defaultStatusPolicyRetrying); err != nil {
			return err
		}
	}
	if policy.TransferEnable {
		if policy.Transfer == nil {
			policy.Transfer = mainPolicy.Transfer
		}
	}
	// validate and default upgrade policy
	if policy.UpgradeEnable {
		if policy.Upgrade == nil {
			policy.Shift = mainPolicy.Upgrade
		}
		if err := v.validateAndDefaultUpgradePolicy(message.L1, policy.Upgrade, defaultUpgradePolicy); err != nil {
			return err
		}
	}
	// validate and default degrade policy
	if policy.DegradeEnable {
		if policy.Degrade == nil {
			policy.Shift = mainPolicy.Degrade
		}
		if err := v.validateAndDefaultDegradePolicy(message.L1, policy.Degrade, defaultDegradePolicy); err != nil {
			return err
		}
	}
	// validate and default shift policy
	if policy.ShiftEnable {
		if policy.Shift == nil {
			policy.Shift = mainPolicy.Shift
		}
		if err := v.validateAndDefaultShiftPolicy(message.L1, policy.Shift, defaultShiftPolicy); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validateAndDefaultShiftPolicy(consumeLevel internal.TopicLevel, configuredPolicy *ShiftPolicy, defaultPolicy *ShiftPolicy) error {
	if configuredPolicy == nil {
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
			return errors.New(fmt.Sprintf("not supported topic level: %v for consume level: %v", configuredPolicy.Level, consumeLevel))
		}
	}
	return nil
}

func (v *validator) validateAndDefaultUpgradePolicy(consumeLevel internal.TopicLevel, configuredPolicy *ShiftPolicy, defaultPolicy *ShiftPolicy) error {
	v.validateAndDefaultShiftPolicy(consumeLevel, configuredPolicy, defaultPolicy)
	if configuredPolicy.Level != "" {
		if configuredPolicy.Level.OrderOf() <= consumeLevel.OrderOf() {
			return errors.New(fmt.Sprintf("upgrade level [%v] cannot be lower or equal than the consume level [%v]",
				configuredPolicy.Level, consumeLevel))
		}
	}
	return nil
}

func (v *validator) validateAndDefaultDegradePolicy(consumeLevel internal.TopicLevel, configuredPolicy *ShiftPolicy, defaultPolicy *ShiftPolicy) error {
	v.validateAndDefaultShiftPolicy(consumeLevel, configuredPolicy, defaultPolicy)
	if configuredPolicy.Level != "" {
		if configuredPolicy.Level.OrderOf() >= consumeLevel.OrderOf() {
			return errors.New(fmt.Sprintf("degrade level [%v] cannot be higher or equal than the consume level [%v]",
				configuredPolicy.Level, consumeLevel))
		}
	}
	return nil
}

func (v *validator) ValidateAndDefaultProducerConfig(conf *ProducerConfig) error {
	if conf.Topic == "" {
		return errors.New("topic is blank")
	}
	// default Transfer policy
	if conf.TransferEnable {
		if conf.Transfer == nil {
			conf.Transfer = defaultTransferPolicy
		}
	}
	// default level
	if conf.Level == "" {
		conf.Level = message.L1
	}
	// default backoff policy
	if conf.BackoffMaxTimes > 0 && conf.BackoffPolicy == nil {
		// delay resend after 1 second
		backoffDelays := []string{"1s"}
		if conf.BackoffDelays != nil {
			backoffDelays = conf.BackoffDelays
			conf.BackoffDelays = nil // release unnecessary reference
		}
		if backoffPolicy, err := backoff.NewAbbrBackoffPolicy(backoffDelays); err != nil {
			return err
		} else {
			conf.BackoffPolicy = backoffPolicy
		}
	}
	//
	// validate upgrade: default nothing
	if conf.UpgradeEnable {
		if err := v.validateAndDefaultUpgradePolicy(message.L1, conf.Upgrade, defaultUpgradePolicy); err != nil {
			return err
		}
	}
	// validate degrade: default nothing
	if conf.DegradeEnable {
		if err := v.validateAndDefaultDegradePolicy(message.L1, conf.Degrade, defaultDegradePolicy); err != nil {
			return err
		}
	}
	// validate shift: default nothing
	if conf.ShiftEnable {
		if err := v.validateAndDefaultShiftPolicy(message.L1, conf.Shift, defaultShiftPolicy); err != nil {
			return err
		}
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
	// default transfer level as main level (not current level)
	if configuredPolicy.TransferLevel == "" {
		configuredPolicy.TransferLevel = defaultPolicy.TransferLevel
	}
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
	return nil
}
