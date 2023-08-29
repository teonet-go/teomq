// Copyright 2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. This is a teonet golang packge wich creates and
// manages messages queue between message producers and consumers.
package teomq

import (
	"fmt"
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
	*queue
}

// Consumer is Teonet messages queue consumer type.
type Consumer struct {
	*teonet.Teonet
}

// NewBroker creates a new Teonet MQueue Broker object.
func NewBroker(appShort string, attr ...interface{}) (mq *Broker, err error) {
	mq = new(Broker)
	mq.queue = newQueue()
	mq.consumers = newConsumers()
	mq.Teonet, err = newTeonet(appShort, append(attr, mq.reader)...)
	return
}

// NewConsumer creates a new Teonet MQueue Consumer object.
func NewConsumer(appShort, broker string, attr ...interface{}) (mq *Consumer, err error) {
	mq = new(Consumer)
	mq.Teonet, err = newTeonet(appShort, append(attr, mq.reader)...)
	if err != nil {
		return
	}
	mq.connectToBroker(broker)
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

// reader is main teonet application reader for Broker object, it receive
// and process incoming teonet messages
func (mq *Broker) reader(c *teonet.Channel, p *teonet.Packet, e *teonet.Event) bool {

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

		// Check consumerHello mwssage from new consumer
		if len(p.Data()) == len(consumerHello) && string(p.Data()) == string(consumerHello) {

			// Add to consumers list
			fmt.Printf("new consumer added")
			mq.add(c)

			// Send answer
			c.Send(consumerAnswer)

			return true
		}

		// Add messages to queue
		mq.set(&message{c, p.ID(), p.Data()})
		fmt.Printf("added to queue, queue length: %d\n", mq.queue.Len())

		// Send answer
		answer := []byte("Teonet answer to " + string(p.Data()))
		c.Send(answer)
	}

	return true
}

// connectToBroker connects consumer to brokers peer
func (mq *Consumer) connectToBroker(addr string) (err error) {
	if err = mq.ConnectTo(addr); err != nil {
		return
	}
	_, err = mq.SendTo(addr, consumerHello)
	return
}

// reader is main teonet application reader for Consumer object, it receive
// and process incoming teonet messages
func (mq *Consumer) reader(c *teonet.Channel, p *teonet.Packet, e *teonet.Event) bool {

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

		// Add messages to queue
		// mq.set(&message{c, p.ID(), p.Data()})
		// fmt.Printf("added to queue, queue length: %d\n", mq.queue.Len())

		// Send answer
		// answer := []byte("Teonet answer to " + string(p.Data()))
		// c.Send(answer)
	}

	return true
}
