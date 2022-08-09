package config

type BackoffPolicy interface {
	Next(redeliveryTimes int) uint
}

type StatusBackoffPolicy interface {
	Next(redeliveryTimes int, statusRedeliveryTimes int) uint
}
