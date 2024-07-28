// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue Broker package provides broker types and methods.
package broker

import (
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teomq/commands"
	"github.com/teonet-go/teomq/subscribers"
	"github.com/teonet-go/teonet"
)

// Broker is Teonet messages queue broker type.
type Broker struct {
	*teonet.Teonet
	*consumers
	*answers
	*queue
	wait
	*commands.Commands
	*subscribers.Subscribers
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
	attr = br.addCommands(attr...)
	br.Teonet, err = teomq.NewTeonet(appShort, append(attr, br.reader)...)
	go br.process()
	return
}

// addCommands adds command schema to broker.
func (br *Broker) addCommands(attr ...interface{}) (outattr []interface{}) {

	outattr = attr
	for i, v := range attr {
		switch v := v.(type) {
		case func(*commands.Commands):
			fmt.Println("Command schema is on")
			outattr = append(attr[:i], attr[i+1:]...)

			br.Commands = new(commands.Commands)
			br.Commands.Init()

			br.Subscribers = new(subscribers.Subscribers)
			br.Subscribers.Init()

			v(br.Commands)
		}
	}

	return
}

// commandMode returns true if broker is in command mode.
func (br *Broker) commandMode() bool {
	return br.Commands != nil
}

// PacketInterface is interface for teonet Packet.
type PacketInterface interface {
	ID() int
	Data() []byte
}

// reader is main teonet application reader for Broker object, it receive
// and process incoming teonet messages.
func (br *Broker) reader(c *teonet.Channel, p *teonet.Packet,
	e *teonet.Event) bool {
	return br.readerI(c, p, e)
}
func (br *Broker) readerI(c *teonet.Channel, p PacketInterface,
	e *teonet.Event) bool {

	// Check channel disconnected
	if e.Event == teonet.EventDisconnected {
		if err := br.consumers.del(c); err == nil {
			log.Printf("consumer removed %s\n", c)
		}
		if br.commandMode() {
			br.Subscribers.Del(c)
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
				if err == io.ErrUnexpectedEOF && len(p.Data()) == 1 && p.Data()[0] == 255 {
					log.Println("start teonet api protocol for peer", c)
				} else {
					log.Printf("UnmarshalBinary error: %s %v\n", err, p.Data())
				}
				return false
			}

			// Check answer from consumer in wait answer list
			log.Printf("got  id %d, len %d, from consumer %s\n", ans.ID(), len(ans.Data()), c)
			ansd, err := br.answers.get(answersData{c.Address(), ans.ID()})
			if err != nil {

				// Check subscribe / unsubscribe commands from consumers
				if br.commandMode() {
					_, parts, _, err := br.Commands.Unmarshal(p.Data())
					if err != nil {
						switch parts[0] {
						case subscribers.CmdSubscribe:
							br.Subscribers.Add(c, parts[1])
							log.Printf("subscribe command '%s' from consumer %s\n", parts[1], c)
						case subscribers.CmdUnsubscribe:
							br.Subscribers.DelCmd(c, parts[1])
							log.Printf("unsubscribe command '%s' from consumer %s\n", parts[1], c)
						default:
							return false
						}
					}
					return true
				}

				log.Printf("not found in answer\n")
				return true
			}

			// Create and marshal producer answer packet
			ans = teomq.NewPacket(uint32(ansd.id), ans.Data())
			data, err := ans.MarshalBinary()
			if err != nil {
				log.Printf("MarshalBinary error: %s\n", err)
				return true
			}

			// Send answer to producer
			if _, err := br.SendTo(ansd.addr, data); err != nil {
				log.Printf("send answer err: %s\n", err)
				return true
			}
			log.Printf("send id %d, len %d, to producer %s\n", ans.ID(), len(ans.Data()), ansd.addr)

			return true
		}

		// Check command mode
		if br.commandMode() {
			_, _, _, err := br.Commands.Unmarshal(p.Data())
			if err != nil {
				log.Printf("check data in command mode error: %s\n", err)
				return false
			}
		}

		// Add messages from producers to queue
		br.set(&message{c.Address(), p.ID(), p.Data()})
		log.Printf("add queue message id %d, len %d, from producer %s, queue length: %d\n",
			p.ID(), len(p.Data()), c, br.queue.Len())

		br.wakeup()
		return true
	}

	return false
}

// apiPacket implements teonet.Packet and holds message data.
type apiPacket struct {
	*teonet.Packet
	data []byte
}

// Data returns message data for teonet api protocol.
func (p apiPacket) Data() []byte {
	return p.data
}

// SendToReader updates data in packet and sends it to reader.
func (br *Broker) SendToReader(c *teonet.Channel, p *teonet.Packet,
	data []byte) error {

	e := &teonet.Event{Event: teonet.EventData}
	pac := &apiPacket{p, data}
	br.readerI(c, pac, e)

	return nil
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

		switch br.commandMode() {

		// Send message to all consumers subscribed to this commandin command mode
		case true:
			// Get producers message (no delete)
			msg, e, err := br.queue.get(false)
			if err != nil {
				continue
			}

			// Unmarshal command
			cmd, _, _, err := br.Commands.Unmarshal(msg.data)
			if err != nil {
				log.Printf("command unmarshal error: %s\n", err)
				continue
			}
			log.Printf("process queue message command %s, id %d, len %d, from %s\n",
				cmd.Cmd, msg.id, len(msg.data), msg.from)

			// Send message to all consumers which was subscribed to this command
			var sent bool
			for _, ch := range br.consumers.list(cmd.Cmd) {

				if !br.Subscribers.CheckCommand(ch, cmd.Cmd) {
					continue
				}

				// Send message to consumer and save it to answers map
				id, err := ch.Send(msg.data)
				if err != nil {
					log.Printf("can't send message to consumer, error: %s\n", err)
					continue
				}
				br.answers.add(answersData{msg.from, msg.id}, answersData{ch.Address(), id})
				log.Printf("send id %d, len %d to consumer %s\n",
					msg.id, len(msg.data), ch)

				sent = true
			}
			if sent {
				br.queue.del(e)
			}

		// Send message to one consumer in basic mode
		case false:
			// Get consumers channel
			ch, err := br.consumers.get()
			if err != nil {
				continue
			}

			// Get producers message
			msg, _, err := br.queue.get()
			if err != nil {
				continue
			}

			log.Printf("process queue message id %d, len %d, from %s\n",
				msg.id, len(msg.data), ch)

			// Send message to consumer and save it to answers map
			id, err := ch.Send(msg.data)
			if err != nil {
				log.Printf("can't send message to consumer, error: %s\n", err)
				continue
			}
			br.answers.add(answersData{msg.from, msg.id}, answersData{ch.Address(), id})
			log.Printf("send id %d, len %d to consumer %s\n",
				msg.id, len(msg.data), ch)
		}
	}
}
