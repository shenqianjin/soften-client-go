## Soften 消息客户端使用指南

### 基础使用
基本使用请参考 [soften](../README.md)

### 消息状态图
![img.png](../spec/message-status.jpeg)

### 执行流程
![img.png](../spec/soften-overview.jpg)

### 扩展检查决定消息状态

<table>
    <!-- header -->
    <tr>
        <th>检查阶段</th>
        <th>目标消息状态</th>
        <th>检查包装器</th>
        <th>解释</th>
    </tr>
    <!-- body -->
    <!-- produce cases -->
    <tr align="left">
        <th rowspan="8">Produce</th><th >Discard</th><th>checker.PrevSendDiscard</th><th>将消息丢弃</th>
    </tr>
    <tr align="left"><th>Pending</th><th>checker.PrevSendPending</th><th>路由消息到Pending队列</th></tr>
    <tr align="left"><th>Blocking</th><th>checker.PrevSendBlocking</th><th>路由消息到Blocking队列</th></tr>
    <tr align="left"><th>Retrying</th><th>checker.PrevSendRetrying</th><th>路由消息到Retrying队列</th></tr>
    <tr align="left"><th>Dead</th><th>checker.PrevSendDead</th><th>路由消息到死信队列</th></tr>
    <tr align="left"><th>Upgrade</th><th>checker.PrevSendUpgrade</th><th>升级消息到预配置的升级级别中</th></tr>
    <tr align="left"><th>Degrade</th><th>checker.PrevSendDegrade</th><th>降级消息到预配置的降级级别中</th></tr>
    <tr align="left"><th>Route</th><th>checker.PrevSendRoute</th><th>路由消息到检查结果指定的队列中</th></tr>
    <!-- consume cases -->
    <tr align="left">
        <th rowspan="8">Prev-Handle</th><th >Discard</th><th>checker.PrevHandleDiscard;<br>checker.PostHandleDiscard</th><th>将消息丢弃</th>
    </tr>
    <tr align="left"><th>Pending</th><th>checker.PrevHandlePending;<br>checker.PostHandlePending</th><th>路由消息到Pending队列</th></tr>
    <tr align="left"><th>Blocking</th><th>checker.PrevHandleBlocking;<br>checker.PostHandleBlocking</th><th>路由消息到Blocking队列</th></tr>
    <tr align="left"><th>Retrying</th><th>checker.PrevHandleRetrying;<br>checker.PostHandleRetrying</th><th>路由消息到Retrying队列</th></tr>
    <tr align="left"><th>Dead</th><th>checker.PrevHandleDead;<br>checker.PostHandleDead</th><th>路由消息到死信队列</th></tr>
    <tr align="left"><th>Upgrade</th><th>checker.PrevHandleUpgrade;<br>checker.PostHandleUpgrade</th><th>升级消息到预配置的升级级别中</th></tr>
    <tr align="left"><th>Degrade</th><th>checker.PrevHandleDegrade;<br>checker.PostHandleDegrade</th><th>降级消息到预配置的降级级别中</th></tr>
    <tr align="left"><th>Route</th><th>checker.PrevHandleReroute;<br>checker.PostHandleReroute</th><th>路由消息到检查结果指定的队列中</th></tr>
</table>

### 处理指定消息结果
<table>
    <!-- header -->
    <tr>
        <th>阶段</th>
        <th>目标消息状态</th>
        <th>处理结果</th>
        <th>解释</th>
    </tr>
    <!-- body -->
    <!-- handle goto cases -->
    <tr align="left">
        <th rowspan="8">Handle</th><th >Discard</th><th>handler.HandleStatusBuilder().Goto(handler.GotoDiscard).Build()</th><th>将消息丢弃</th>
    </tr>
    <tr align="left"><th>Pending</th><th>handler.HandleStatusBuilder().Goto(handler.GotoPending).Build()</th><th>路由消息到Pending队列</th></tr>
    <tr align="left"><th>Blocking</th><th>handler.HandleStatusBuilder().Goto(handler.GotoBlocking).Build()</th><th>路由消息到Blocking队列</th></tr>
    <tr align="left"><th>Retrying</th><th>handler.HandleStatusBuilder().Goto(handler.GotoRetrying).Build()</th><th>路由消息到Retrying队列</th></tr>
    <tr align="left"><th>Dead</th><th>handler.HandleStatusBuilder().Goto(handler.GotoDead).Build()</th><th>路由消息到死信队列</th></tr>
    <tr align="left"><th>Upgrade</th><th>handler.HandleStatusBuilder().Goto(handler.GotoUpgrade).Build()</th><th>升级消息到预配置的升级级别中</th></tr>
    <tr align="left"><th>Degrade</th><th>handler.HandleStatusBuilder().Goto(handler.GotoDegrade).Build()</th><th>降级消息到预配置的降级级别中</th></tr>
</table>

-----------



