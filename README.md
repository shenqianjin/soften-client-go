## Soften Go Client Library

Soften is a state-oriented, flexibility-tunable and equality-nursed messaging client for coordinating workloads 
of async business messages, especially a same business shared by hundreds of billions of users.

### Goal
- Isolate messages by statuses
- Tune flexibility such status/level weights, consume-concurrency, status/leveled retries, etc.
- Nurse equality for different users
- Hierarchy on performance for different user group
- Adaptively adjust rate or concurrency, weights, etc (TBD).

### Requirements
- Go 1.15+

### Usage
#### Installation
Download the library of Go client to local environment:
```go
go get -u "github.com/shenqianjin/soften-client-go"
```
#### Create Client
In order to interact with Pulsar, you'll first need a Client object. You can create a client object using the NewClient
function, passing in a ClientConfig object. Here's an example:
```go
client, err := soften.NewClient(config.ClientConfig{URL: "pulsar://localhost:6650"})
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```
#### Create Producer - Regular Mode
Pulsar producers publish messages to Pulsar topics. You can configure Go producers using a ProducerConfig object.
Here's an example:
```go
// create producer
producer, err := client.CreateProducer(pulsar.ProducerOptions{
    Topic: "topic-1",
})
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

// send messages
for i := 0; i < 10; i++ {
    if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
        Payload: []byte(fmt.Sprintf("hello-%d", i)),
    }); err != nil {
        log.Fatal(err)
    } else {
        log.Println("Published message: ", msgId)
    }
}
```
#### Create Producer - Advanced Mode
Soften provides checkpoints as optional choices before you send messages to your messaging middleware. 
Learn more about send checkpoints in [manual].

The following show you a case of send checkpoints:
- schedule VIP users' messages to a high level topic
- send Disable users' messages to dead letter topic
```go
producer, err := client.CreateProducer(config.ProducerConfig{
    Topic:             "topic-1",
    UpgradeEnable:     true,
    UpgradeTopicLevel: topic.L2,
    DeadEnable:        true,
}, checker.PrevSendUpgrade(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
    if "VIP" == msg.Properties["UserType"] {
        return checker.CheckStatusPassed
    }
    return checker.CheckStatusRejected
}), checker.PrevSendDead(func(msg *pulsar.ProducerMessage) checker.CheckStatus {
    if "Disable" == msg.Properties["UserStatus"] {
        return checker.CheckStatusPassed
    }
    return checker.CheckStatusRejected
}))
```
#### Create Listener - Regular Mode
Pulsar consumers subscribe to one or more Pulsar topics and listen for incoming messages produced on that topic/those topics.
You can configure Go consumers using a ConsumerConfig object. Here's a basic example:
```go
// create listen
listener, err := cli.CreateListener(config.ConsumerConfig{
    Topic:                       "topic-1",
    SubscriptionName:            "my-subscription",
    Type:                        pulsar.Shared,
})
if err != nil {
    log.Fatal(err)
}
// start to listen
messageHandle := func(msg pulsar.Message) (bool, error) {
    fmt.Printf("Received message  msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
    return true, nil
}
err = listener.Start(context.Background(), messageHandle)
if err != nil {
    log.Fatal(err)
}

time.Sleep(2 * time.Second)
```
#### Create Listener - Advanced Mode
Soften also provides checkpoints as optional choices before/after you consume messages in your listeners.
you can also specify goto status directly as the return of your handle function which you input into your
listener as well. Learn more about consume checkpoints in [manual] and goto status actions in [manual].


Here is an example of mixed consume checkpoints and goto actions to implement the following:
- previous handle checkpoint: schedule Junior users' messages to a low level topic - B1
- post handle checkpoint: send these messages which is handled with "Bad Request" error to dead letter topic
- goto decision: backoff these messages which is handled with "Internal Server Error" error to retrying topic, they will
- be re-consumed in a few seconds (depended on your RetryingPolicy configuration)
```go
listener, err := client.CreateListener(config.ConsumerConfig{
    Topic:                       "topic-1",
    SubscriptionName:            "my-subscription",
    Type:                        pulsar.Shared,
    SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
    DegradeEnable:               true,
    DegradeTopicLevel:           topic.B1,
    DeadEnable:                  true,
    RetryingEnable:              true,
}, checker.PrevHandleDegrade(func(msg pulsar.Message) checker.CheckStatus {
    if "Junior" == msg.Properties()["UserLevel"] {
        return checker.CheckStatusPassed
    }
    return checker.CheckStatusRejected
}), checker.PostHandleDead(func(msg pulsar.Message, err error) checker.CheckStatus {
    if strings.Contains(err.Error(), "Bad Request") {
        return checker.CheckStatusPassed
    }
    return checker.CheckStatusRejected
}))
if err != nil {
    log.Fatal(err)
}
defer listener.Close()

messageHandle := func(msg pulsar.Message) handler.HandleStatus {
    var err error
    fmt.Printf("Received message  msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
    // here do you business logic
    if err != nil {
        if strings.Contains(err.Error(), "Internal Server Error") {
            return handler.HandleStatusBuilder().Goto(handler.GotoRetrying).Build()
        }
        return handler.HandleStatusAuto
    }
    return handler.HandleStatusOk
}
err = listener.StartPremium(context.Background(), messageHandle)
```




### Contributing
Contributions are welcomed and greatly appreciated. See CONTRIBUTING.md for details on submitting patches
and the contribution workflow.
