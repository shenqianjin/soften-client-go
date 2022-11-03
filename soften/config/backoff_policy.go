package config

type BackoffDelayPolicy interface {
	Next(redeliveryTimes int) uint
}

type StatusBackoffDelayPolicy interface {
	Next(redeliveryTimes int, statusRedeliveryTimes int) uint
}
