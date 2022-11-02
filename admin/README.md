## Soften Admin 管理工具

```shell
soften-admin <modules> -h
```
|选项| 类型|说明|
|:---|:---|:---|
|-d, --debug|bool|是否开启debug日志模式|
|-u, --url|string|Pulsar Broker HTTP URL|
|-h, --help|bool||

### Topics 模块

```shell
soften-admin topics <command> -h
```
|选项| 类型|说明|
|:---|:---|:---|
|-l, --level|string|等级; 多个``,``分割; 不指定默认L1|
|-s, --status|string|状态; 多个``,``分割; 不指定默认Ready|
|-S, --subscription|string|订阅名称; 多个``,``分割; 不指定默认空|


#### create
- 命令
```shell
soften-admin topics create <ground_topic> [options]
```
|选项| 类型|说明|
|:---|:---|:---|
|-p, --partitions|uint|partition数量; 默认1|
|-P, --partitioned|bool|partitioned模式,否则为non-partitioned模式|
- 示例
```shell
// 创建TEST的主题组, 支持L1,L2级别, 支持Ready,Pending,Dead三种状态, 订阅者名字为  user-service
soften-admin topics create TEST -l L1,L2 -s Ready,Pending,Dead -S user-service
```
#### delete
```shell
soften-admin topics delete <ground_topic> [options]
```
- 示例
```shell
// 删除部分TEST的主题组, 条件为: L2级别, Pending状态, 订阅者名字: user-service
soften-admin topics create TEST -l L2 -s Pending -S user-service
```
|选项| 类型|说明|
|:---|:---|:---|
|-P, --partitioned|bool|partitioned模式,否则为non-partitioned模式|
|-A, --all|bool|处理所有主题(包含所有等级、状态和订阅); 不建议使用! |

#### update
```shell
soften-admin topics update <ground_topic> [options]
```
|选项| 类型|说明|
|:---|:---|:---|
|-p, --partitions|uint|partition数量; 默认1|
|-A, --all|bool|处理所有主题(包含所有等级、状态和订阅); 不建议使用! |


#### list
```shell
soften-admin topics list <namespace | ground_topic> [options]
```
|选项| 类型|说明|
|:---|:---|:---|
|-P, --partitioned|bool|partitioned模式,否则为non-partitioned模式|
|-A, --all|bool|处理所有主题(包含所有等级、状态和订阅); 不建议使用! |

### Messages 模块

```shell
soften-admin messages <command> -h
```
|选项| 类型|说明|
|:---|:---|:---|
|--broker-url|string|访问消息的Pulsar Broker URL|
|-c, --condition|string|处理消息命中的条件; 多个``,``分割; 不指定默认""|
|--start-publish-time|string|处理消息命中的开始发布时间; 消息扭转状态该值是变化的|
|--start-event-time|string|处理消息命中的开始事件时间; 业务设置的可选项|


#### iterate
```shell
soften-admin messages iterate <topic> [options]
```
|选项| 类型|说明|
|:---|:---|:---|
|--print-mode|uint|命中输出模式: 0 不输出; 1 输出ID; 2 输出 ID, Payload, Publish Time and Event Time|
|--print-progress-iterate-interval|uint|进度输出间隔事件,单位秒|

#### recall
```shell
soften-admin messages recall <topic> [options]
```
|选项| 类型|说明|
|:---|:---|:---|
|--print-progress-iterate-interval|uint|进度输出间隔事件,单位秒|
|-b, --publish-batch-enable|bool|是否使用Batch模式发布消息, 默认false|
|--publish-max-times|uint|发布消息最大尝试次数; 0表示不限制次数; 超过该次数时,程序异常退出|


#### tidy
```shell
soften-admin messages tidy <topic> [options]
```
|选项| 类型|说明|
|:---|:---|:---|
|--print-progress-iterate-interval|uint|进度输出间隔事件,单位秒|
|-b, --publish-batch-enable|bool|是否使用Batch模式发布消息, 默认false|
|--publish-max-times|uint|发布消息最大尝试次数; 0表示不限制次数; 超过该次数时,程序异常退出|
|-s, --subscription|string|订阅源主题的订阅名字; 如果跟业务订阅名相同; 注意相互之间的影响|
|--iterate-timeout|uint|终止条件:超过该时间没有新数据可供消费时终止|
|--match-timeout|uint|终止条件:超过该时间没有匹配到新消息时终止|
|--end-publish-time|string|终止条件:消息Publish时间不该值新时终止|
|--end-event-time|string|终止条件:消息Event时间不该值新时终止|
|--matched-to|string|匹配的消息规整至的目标主题|
|--matched-as-discard|bool|匹配的消息规直接丢弃, 不能与'--matched-to'同时存在|
|--unmatched-to|string|不匹配的消息规整至的目标主题|
|-unmatched-as-discard|bool|不配的消息规直接丢弃, 不能与'--unmatched-to'同时存在|
