package util

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func GenerateBusinessMsg(uid uint32) *pulsar.ProducerMessage {
	var user User
	if rand.Intn(5) < 2 {
		user = createAMarriedUser(uid)
	} else {
		user = createARandomUser(uid)
	}
	payload, err := json.Marshal(user)
	if err != nil {
		panic(err)
	}
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

// ------ helper ------

type User struct {
	Uid     uint32  `json:"uid,omitempty"`
	Name    string  `json:"name,omitempty"`
	Age     uint    `json:"age,omitempty"`
	Spouse  *User   `json:"spouse,omitempty"`
	Friends []*User `json:"friends,omitempty"`
}
