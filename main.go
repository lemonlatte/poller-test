package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bitmark-inc/bitmarkd/zmqutil"
	zmq "github.com/pebbe/zmq4"
)

func main() {
	pub, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		log.Fatal(err)
	}
	err = pub.Bind("inproc://pubsub-test")
	if err != nil {
		log.Fatal(err)
	}

	sub, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		log.Fatal(err)
	}
	err = sub.Connect("inproc://pubsub-test")
	if err != nil {
		log.Fatal(err)
	}
	sub.SetSubscribe("")

	push, err := zmq.NewSocket(zmq.PUSH)
	if err != nil {
		log.Fatal(err)
	}
	err = push.Bind("inproc://pullpush-test")
	if err != nil {
		log.Fatal(err)
	}

	pull, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		log.Fatal(err)
	}
	err = pull.Connect("inproc://pullpush-test")
	if err != nil {
		log.Fatal(err)
	}

	p := zmqutil.NewPoller()
	p.Add(sub, zmq.POLLIN)
	p.Add(pull, zmq.POLLIN)

	go func() {
		log.Print("Start publish messages")
		for j := 0; j < 5; j++ {
			for i := 1; i < 10000; i++ {
				pub.SendMessage(fmt.Sprintf("pub: %d"))
			}
			time.Sleep(time.Second * 2)
		}
		p.Remove(sub)
		sub.Close()
		pub.Close()
	}()

	go func() {
		log.Print("Start push messages")
		for {
			for i := 1; i < 10000; i++ {
				push.SendMessage(fmt.Sprintf("push: %d"))
			}
			time.Sleep(time.Second * 2)
		}
	}()

	counter := 0
	subCounter := 0
	pullCounter := 0

	go func() {
		for {
			log.Printf("Counter: %d, sub counter: %d, pull counter: %d",
				counter, subCounter, pullCounter)
			time.Sleep(time.Second * 1)
		}
	}()

	for {
		polled, err := p.Poll(time.Second * 10)
		if err != nil {
			log.Printf("Polling error: %s", err)
			continue
		}

		for _, p := range polled {
			t, err := p.Socket.GetType()
			// log.Printf("Socket Type: %s", t)

			if err != nil {
				log.Printf("Fail to get type: %s", err)
			}

			counter += 1
			switch t {
			case zmq.SUB:
				p.Socket.RecvMessage(0)
				subCounter += 1
			case zmq.PULL:
				p.Socket.RecvMessage(0)
				pullCounter += 1
			}
		}
	}
}
