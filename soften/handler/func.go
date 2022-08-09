package handler

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

// ------ consumer biz handle interfaces ------

// HandleFunc is the regular processing flow, and it is recommended.
// the message will be acknowledged when the return is true; If it returns false,
// the message will be unacknowledged to the main partition, then route to retrying
// partition if the retrying module is enabled. finally, it goto dead letter partition
// when all retrying times exceed the maximum.
type HandleFunc func(msg pulsar.Message) (success bool, err error)

// PremiumHandleFunc allows the result contains any goto destination such as Done, Retrying,
// Dead, Pending, Blocking, Degrade and Upgrade.
// different goto destination will deliver current message to the corresponding destination-topic.
// Please note the process will be regressed to regular module when the handled goto destination
// status is not enough to do its flow, e.g. HandleStatusPending is returned when the pending module
// is not enabled in the listener configuration.
type PremiumHandleFunc func(msg pulsar.Message) HandleStatus
