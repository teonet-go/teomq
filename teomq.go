// Copyright 2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. This is a teonet golang packge wich creates and
// manages messages queue between message producers and consumers.
package teomq

import (
	"fmt"
	"sync"
	"time"

	"github.com/teonet-go/teonet"
)

var (
	consumerHello  = []byte("Consumer")
	consumerAnswer = []byte("Connected to broker")
)

// Broker is Teonet messages queue broker type.
type Broker struct {
	*teonet.Teonet
	*consumers
	*answers
	*queue
	wait
}
type wait struct {
	*sync.Mutex
	*sync.Cond
}

func (w *wait) init() {
	w.Mutex = new(sync.Mutex)
	w.Cond = sync.NewCond(w.Mutex)
}

// Consumer is Teonet messages queue consumer type.
type Consumer struct {
	*teonet.Teonet
}

// NewBroker creates a new Teonet MQueue Broker object.
func NewBroker(appShort string, attr ...interface{}) (br *Broker, err error) {
	br = new(Broker)
	br.wait.init()
	br.queue = newQueue()
	br.answers = newAnswers()
	br.consumers = newConsumers()
	br.Teonet, err = newTeonet(appShort, append(attr, br.reader)...)
	go br.process()
	return
}

// NewConsumer creates a new Teonet MQueue Consumer object.
func NewConsumer(appShort, broker string, attr ...interface{}) (co *Consumer, err error) {
	co = new(Consumer)
	co.Teonet, err = newTeonet(appShort, append(attr, co.reader)...)
	if err != nil {
		return
	}
	co.connectToBroker(broker)
	return
}

// newTeonet creates new teonet connection and connect to teonet.
func newTeonet(appShort string, attr ...interface{}) (teo *teonet.Teonet, err error) {

	// Create teonet connection
	teo, err = teonet.New(appShort, attr...)
	if err != nil {
		return
	}

	// Connect to teonet
	for teo.Connect() != nil {
		time.Sleep(1 * time.Second)
	}

	return
}

// connectToBroker connects consumer to brokers peer
func (co *Consumer) connectToBroker(addr string) (err error) {
	if err = co.ConnectTo(addr); err != nil {
		return
	}
	return
}

// reader is Consumer teonet channel reader connected to brokers peer
// and process incoming teonet messages
func (co *Consumer) reader(c *teonet.Channel, p *teonet.Packet, e *teonet.Event) bool {

	// On connected
	if e.Event == teonet.EventConnected {
		fmt.Printf("connected to %s\n", c)
		c.Send(consumerHello)
		return false
	}

	if e.Event == teonet.EventDisconnected {
		fmt.Printf("disconnected from %s\n", c)
		return false
	}

	// Skip not Data events
	if e.Event != teonet.EventData {
		return false
	}

	// In client mode get messages and ...
	if c.ClientMode() {

		// Print received message
		fmt.Printf("got from %s, \"%s\", len: %d, id: %d, tt: %6.3fms\n",
			c, p.Data(), len(p.Data()), p.ID(),
			float64(c.Triptime().Microseconds())/1000.0,
		)

		// Check consumerHello message from new consumer
		if len(p.Data()) == len(consumerAnswer) && string(p.Data()) == string(consumerAnswer) {
			fmt.Printf("connected to broker\n")
			// c.Channel()
			return true
		}

		// Send answer
		// TODO: this answer should processed on application level
		answer := []byte("Teonet answer to " + string(p.Data()))
		c.Send(answer)

	}

	return true
}

// reader is main teonet application reader for Broker object, it receive
// and process incoming teonet messages
func (br *Broker) reader(c *teonet.Channel, p *teonet.Packet, e *teonet.Event) bool {

	// Check channel disconnected
	if e.Event == teonet.EventDisconnected {
		if err := br.consumers.del(c); err == nil {
			fmt.Printf("consumer removed %s\n", c)
		}
		return false
	}

	// Skip not Data events
	if e.Event != teonet.EventData {
		return false
	}

	// In server mode get messages and set it to the messages queue
	if c.ServerMode() {

		// Print received message
		fmt.Printf("got from %s, \"%s\", len: %d, id: %d, tt: %6.3fms\n",
			c, p.Data(), len(p.Data()), p.ID(),
			float64(c.Triptime().Microseconds())/1000.0,
		)

		// Check consumerHello message from new consumer
		if len(p.Data()) == len(consumerHello) && string(p.Data()) == string(consumerHello) {

			// Add to consumers list
			fmt.Printf("consumer added %s\n", c)
			br.consumers.add(c)

			// Send answer
			c.Send(consumerAnswer)

			// Wake up messages processing
			br.wakeup()

			return true
		}

		if br.consumers.existsUnsafe(c) != nil {
			fmt.Printf("got answer '%s' from consumer %s\n", p.Data(), c)
			if producer := br.answers.get(answersData{c, 0}); producer != nil {
				producer.ch.Send(p.Data())
				return true
			}
			fmt.Printf("not found in answer\n")
			return true
		}

		// Add messages to queue
		br.set(&message{c, p.ID(), p.Data()})
		fmt.Printf("message added to queue, queue length: %d\n", br.queue.Len())
		br.wakeup()

		// Send answer
		// answer := []byte("Teonet answer to " + string(p.Data()))
		// c.Send(answer)
	}

	return true
}

// wakeup wakes up message processing when messages or(and) consumers added
func (br *Broker) wakeup() {
	br.Signal()
}

// process processing mesagages
func (br *Broker) process() {
	br.L.Lock()
	defer br.L.Unlock()

	for {
		// Check message queue and customers length and sleep if empty until
		// unlock (until wakeup func called)
		if !(br.queue.len() > 0 && br.consumers.len() > 0) {
			br.Wait()
			continue
		}

		fmt.Printf("process message\n")

		// Get producers message and consumers channel
		msg := br.queue.get()
		ch := br.consumers.get()

		// Send message to consumer and save it to answers map
		ch.Send(msg.data)
		br.answers.add(answersData{msg.ch, 0}, answersData{ch, 0})

		fmt.Printf("send '%s' to %s\n", msg.data, ch)
	}
}
