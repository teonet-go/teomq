// Copyright 2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. This is a teonet golang packge wich creates and
// manages messages queue between message producers and consumers.
package teomq

import (
	"bytes"
	"encoding/binary"
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

// init initialize wait structure
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

// sendAnswer send answer to message received from broker
func (co *Consumer) sendAnswer(pac *teonet.Packet, data []byte) (err error) {
	data, err = ConsumerPacket{uint32(pac.ID()), data}.MarshalBinary()
	if err != nil {
		return
	}
	_, err = co.SendTo(pac.From(), data)
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
		co.sendAnswer(p, answer)

	}

	return true
}

// ConsumerPacket defines consumer answer message
type ConsumerPacket struct {
	id   uint32
	data []byte
}

// MarshalBinary marshals consumer binary packet
func (p ConsumerPacket) MarshalBinary() (data []byte, err error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, p.id)
	binary.Write(buf, binary.LittleEndian, p.data)

	data = buf.Bytes()
	return
}

// UnmarshalBinary unmarshals consumer binary packet
func (p *ConsumerPacket) UnmarshalBinary(data []byte) (err error) {
	var buf = bytes.NewBuffer(data)

	if err = binary.Read(buf, binary.LittleEndian, &p.id); err != nil {
		return
	}
	d := make([]byte, buf.Len())
	if err = binary.Read(buf, binary.LittleEndian, d); err != nil {
		return
	}
	p.data = d

	return
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
		// fmt.Printf("got from %s, \"%s\", len: %d, id: %d, tt: %6.3fms\n",
		// 	c, p.Data(), len(p.Data()), p.ID(),
		// 	float64(c.Triptime().Microseconds())/1000.0,
		// )

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

		// Got answer from consumer
		if br.consumers.existsUnsafe(c) != nil {

			ans := ConsumerPacket{}
			if err := ans.UnmarshalBinary(p.Data()); err != nil {
				fmt.Printf("%s\n", err)
				return true
			}

			fmt.Printf("got '%s' from consumer %s\n", ans.data, c)
			if producer := br.answers.get(answersData{c.Address(), int(ans.id)}); producer != nil {
				if _, err := br.SendTo(producer.addr, ans.data); err != nil {
					fmt.Printf("send answer err: %s\n", err)
					return true
				}
				fmt.Printf("send '%s' to producer %s\n", ans.data, producer.addr)
				return true
			}
			fmt.Printf("not found in answer\n")
			return true
		}

		// Add messages to queue
		br.set(&message{c.Address(), p.ID(), p.Data()})
		fmt.Printf("message from producer %s added to queue, queue length: %d\n",
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

		fmt.Printf("process message\n")

		// Get producers message and consumers channel
		msg := br.queue.get()
		ch := br.consumers.get()

		// Send message to consumer and save it to answers map
		id, err := ch.Send(msg.data)
		if err != nil {
			fmt.Printf("can't send message to consumer, error: %s\n", err)
			continue
		}
		br.answers.add(answersData{msg.from, msg.id}, answersData{ch.Address(), id})
		fmt.Printf("send '%s' to consumer %s\n", msg.data, ch)
	}
}
