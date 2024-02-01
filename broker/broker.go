// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue Broker package provides broker types and methods.
package broker

import (
	"log"
	"sync"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teonet"
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

// init initialize wait structure
func (w *wait) init() {
	w.Mutex = new(sync.Mutex)
	w.Cond = sync.NewCond(w.Mutex)
}

// New creates a new Teonet MQueue Broker object.
func New(appShort string, attr ...interface{}) (br *Broker, err error) {
	br = new(Broker)
	br.wait.init()
	br.queue = newQueue()
	br.answers = newAnswers()
	br.consumers = newConsumers()
	br.Teonet, err = teomq.NewTeonet(appShort, append(attr, br.reader)...)
	go br.process()
	return
}

// reader is main teonet application reader for Broker object, it receive
// and process incoming teonet messages
func (br *Broker) reader(c *teonet.Channel, p *teonet.Packet,
	e *teonet.Event) bool {

	// Check channel disconnected
	if e.Event == teonet.EventDisconnected {
		if err := br.consumers.del(c); err == nil {
			log.Printf("consumer removed %s\n", c)
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
		// log.Printf("got from %s, \"%s\", len: %d, id: %d, tt: %6.3fms\n",
		// 	c, p.Data(), len(p.Data()), p.ID(),
		// 	float64(c.Triptime().Microseconds())/1000.0,
		// )

		// Check consumerHello message from new consumer
		if len(p.Data()) == len(teomq.ConsumerHello) &&
			string(p.Data()) == string(teomq.ConsumerHello) {

			// Add to consumers list
			log.Printf("consumer added %s\n", c)
			br.consumers.add(c)

			// Send answer
			c.Send(teomq.ConsumerAnswer)

			// Wake up messages processing
			br.wakeup()

			return true
		}

		// Got answer from consumer
		if br.consumers.exists(c) {

			// Unmarshal packet data to answer
			ans := &teomq.Packet{}
			if err := ans.UnmarshalBinary(p.Data()); err != nil {
				log.Printf("%s\n", err)
				return true
			}

			// Check answer from consumer in wait answer list
			log.Printf("got '%s' from consumer %s\n", ans.Data(), c)
			p, err := br.answers.get(answersData{c.Address(), ans.ID()})
			if err != nil {
				log.Printf("not found in answer\n")
				return true
			}

			// Create and marshal produser answer packet
			ans = teomq.NewPacket(uint32(p.id), ans.Data())
			data, err := ans.MarshalBinary()
			if err != nil {
				log.Printf("%s\n", err)
				return true
			}

			// Send answer to producer
			if _, err := br.SendTo(p.addr, data); err != nil {
				log.Printf("send answer err: %s\n", err)
				return true
			}
			log.Printf("send '%s' to producer %s\n", ans.Data(), p.addr)

			return true
		}

		// Add messages from produsers to queue
		br.set(&message{c.Address(), p.ID(), p.Data()})
		log.Printf("message from producer %s added to queue, queue length: %d\n",
			c, br.queue.Len())

		br.wakeup()
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

		// Get consumers channel
		ch, err := br.consumers.get()
		if err != nil {
			continue
		}

		// Get producers message
		msg, err := br.queue.get()
		if err != nil {
			continue
		}

		log.Printf("process queue message from %s\n", ch)

		// Send message to consumer and save it to answers map
		id, err := ch.Send(msg.data)
		if err != nil {
			log.Printf("can't send message to consumer, error: %s\n", err)
			continue
		}
		br.answers.add(answersData{msg.from, msg.id}, answersData{ch.Address(), id})
		log.Printf("send '%s' to consumer %s\n", msg.data, ch)
	}
}
