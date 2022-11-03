//go:build buisiness
// +build buisiness

package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/shenqianjin/soften-client-go/soften/admin"
	"github.com/shenqianjin/soften-client-go/soften/config"
	"github.com/shenqianjin/soften-client-go/test/internal"
	"github.com/stretchr/testify/assert"
)

//

type User struct {
	Uid     uint32  `json:"uid,omitempty"`
	Name    string  `json:"name,omitempty"`
	Age     uint    `json:"age,omitempty"`
	Spouse  *User   `json:"spouse,omitempty"`
	Friends []*User `json:"wife,omitempty"`
}

func TestGenerateUsers(t *testing.T) {
	groundTopic := "USER01"
	manager := admin.NewRobustTopicManager(internal.DefaultPulsarHttpUrl)

	// clean up topics
	internal.CleanUpTopics(t, manager, groundTopic)
	//defer internal.CleanUpTopics(t, manager, groundTopic)
	// create topic if not found in case broker closes auto creation
	internal.CreateTopicsIfNotFound(t, manager, []string{groundTopic}, 0)

	client := internal.NewClient(internal.DefaultPulsarUrl)
	defer client.Close()

	producer, err := client.CreateProducer(config.ProducerConfig{
		Topic: groundTopic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// send msg
	syncCount := 800
	uid := 1
	for ; uid <= syncCount; uid++ {
		msg := generateBusinessMsg(t, uint32(uid))
		msg.Properties["Index"] = strconv.Itoa(uid)
		mid, err := producer.Send(context.Background(), msg)
		fmt.Printf("sync message %v, msgId: %v\n", uid, mid)
		assert.Nil(t, err)
	}
	// send async msg
	asyncCount := 2200
	wg := sync.WaitGroup{}
	limit := uid + asyncCount
	for ; uid <= limit; uid++ {
		msg := generateBusinessMsg(t, uint32(uid))
		msg.Properties["Index"] = strconv.Itoa(uid)
		wg.Add(1)
		func(i int) {
			producer.SendAsync(context.Background(), msg, func(id pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
				fmt.Printf("sync message %v, msgId: %v\n", i, id)
				assert.Nil(t, err)
				wg.Done()
			})
		}(uid)
	}
	wg.Wait()
}

func generateBusinessMsg(t *testing.T, uid uint32) *pulsar.ProducerMessage {
	var user User
	if rand.Intn(5) < 2 {
		user = createAMarriedUser(uid)
	} else {
		user = createARandomUser(uid)
	}
	payload, err := json.Marshal(user)
	assert.Nil(t, err)
	msg := &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: make(map[string]string),
	}
	if uid%3 == 0 {
		msg.EventTime = time.Now()
	}
	return msg
}

func createAMarriedUser(uid uint32) User {
	user := createARandomUser(uid)
	spouse := createARandomUser(uid + uint32(10000))
	user.Spouse = &spouse
	return user
}

func createARandomUser(uid uint32) User {
	user := bornARandomPureUser(uid)
	// friends
	friendNum := rand.Intn(3)
	if friendNum <= 0 {
		return user
	}
	user.Friends = make([]*User, 0)
	for i := 0; i < friendNum; i++ {
		friend := bornARandomPureUser(uid + 100000000)
		user.Friends = append(user.Friends, &friend)
	}
	return user
}

func bornARandomPureUser(uid uint32) User {
	user := User{Uid: uid}
	user.Name = fmt.Sprintf("No%v-%v", uid, time.Now().Format("20060102T150405"))
	user.Age = uint(rand.Intn(100))
	return user
}
