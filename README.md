## Soften Go Client Library

Soften is a state-oriented, flexibility-tunable and equality-nursed messaging client for coordinating workloads 
of async business messages, especially a same business shared by hundreds of billions of users.

### Goal
- Isolate messages by statuses
- Tune flexibility such weights, concurrency, maximum, etc.
- Nurse equality for different users
- Hierarchy on performance for different user group

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
#### Create Producer
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

#### Create Listener
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




### Contributing
Contributions are welcomed and greatly appreciated. See CONTRIBUTING.md for details on submitting patches
and the contribution workflow.
