package message

import "github.com/shenqianjin/soften-client-go/soften/internal"

const (
	XPropertyOriginMessageID   = "X-ORIGIN-MESSAGE-ID"   // 源消息ID
	XPropertyOriginTopic       = "X-ORIGIN-TOPIC"        // 源消息Topic
	XPropertyOriginPublishTime = "X-ORIGIN-PUBLISH-TIME" // 消费发布时间
	XPropertyOriginLevel       = "X-ORIGIN-LEVEL"        // 源消息级别
	XPropertyOriginStatus      = "X-ORIGIN-STATUS"       // 源消息状态

	XPropertyReconsumeTime = "X-Reconsume-Time" // 消费时间
	XPropertyReentrantTime = "X-Reentrant-Time" // 重入时间

	XPropertyPreviousMessageLevel          = "X-Previous-Level"                   // 前一次消息级别
	XPropertyPreviousMessageStatus         = "X-Previous-Status"                  // 前一次消息状态
	XPropertyReconsumeTimes                = "X-Reconsume-Times"                  // 总重试消费次数
	XPropertyReentrantStartRedeliveryCount = "X-Reentrant-Start-Redelivery-Count" // 当前状态开始的消费次数

	XPropertyPendingReconsumeTimes  = "X-Pending-Reconsume-Times"  // Pending 状态消费次数
	XPropertyPendingReentrantTimes  = "X-Pending-Reentrant-Times"  // Pending 状态重入次数
	XPropertyBlockingReconsumeTimes = "X-Blocking-Reconsume-Times" // Blocking 状态消费次数
	XPropertyBlockingReentrantTimes = "X-Blocking-Reentrant-Times" // Blocking 状态重入次数
	XPropertyRetryingReconsumeTimes = "X-Retrying-Reconsume-Times" // Retrying 状态消费次数
	XPropertyRetryingReentrantTimes = "X-Retrying-Reentrant-Times" // Retrying 状态重入次数
	XPropertyReadyReconsumeTimes    = "X-Ready-Reconsume-Times"    // Ready 状态消费次数
	XPropertyReadyReentrantTimes    = "X-Ready-Reentrant-Times"    // Ready 状态重入次数
	XPropertyDeadReconsumeTimes     = "X-Dead-Reconsume-Times"     // Dead 状态消费次数
	XPropertyDeadReentrantTimes     = "X-Dead-Reentrant-Times"     // Dead 状态重入次数
)

var (
	statusConsumeTimesMap = map[internal.MessageStatus]string{
		StatusPending:  XPropertyPendingReconsumeTimes,
		StatusBlocking: XPropertyBlockingReconsumeTimes,
		StatusRetrying: XPropertyRetryingReconsumeTimes,
		StatusReady:    XPropertyReadyReconsumeTimes,
		StatusDead:     XPropertyDeadReconsumeTimes,
	}

	statusReentrantTimesMap = map[internal.MessageStatus]string{
		StatusPending:  XPropertyPendingReentrantTimes,
		StatusBlocking: XPropertyBlockingReentrantTimes,
		StatusRetrying: XPropertyRetryingReentrantTimes,
		StatusReady:    XPropertyReadyReentrantTimes,
		StatusDead:     XPropertyDeadReentrantTimes,
	}
)
