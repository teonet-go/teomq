// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue Producer package provides producer types and methods.
package producer

import (
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teonet"
)

// Producer is Teonet messages queue producers type.
type Producer struct {
	broker string
	*teonet.Teonet
	*Messages
	commandMode CommandMode
}

// CommandMode is true if producer is in command mode. It used in New method to
// start producer in command mode. In command mode the producer sends commands
// and wait multiple answers during timeout. In normal mode it sends messages
// and waits only one answer.
type CommandMode bool

// New creates a new Teonet Message Queue Producer object.
func New(appShort, broker string, attr ...any) (p *Producer, err error) {
	p = new(Producer)
	p.broker = broker
	attr = p.setCommands(attr...)
	p.Teonet, err = teomq.NewTeonet(appShort, attr...)
	if err != nil {
		return
	}
	p.Messages = NewMessages()
	p.process()
	teomq.ConnectToBroker(p.Teonet, broker)
	return
}

// setCommands sets command schema.
func (p *Producer) setCommands(attr ...any) (outattr []any) {

	outattr = attr
	for i, v := range attr {
		switch v := v.(type) {
		case CommandMode:
			fmt.Println("Command schema is on")
			outattr = slices.Delete(outattr, i, i+1)
			p.commandMode = v
		}
	}

	return
}

// Send sends message to broker.
//
// The message is sent to the broker specified in the Producer object.
// The message data is passed in the data parameter.
// The function returns the message ID and any error that occurred during
// sending.
//
// Optional parameters can be passed in the attr parameter.
// The function looks for the following types:
//   - func(id int, data []byte, err error) bool: callback function to be called
//     when the message is received.
//   - RecvCallback: callback function to be called when the message is received.
//   - time.Duration: timeout value for the message. The default value is 5
//     seconds.
func (p *Producer) Send(data []byte, attr ...any) (id int, err error) {

	// Parse attributes
	// callback function to be called when the message is received
	var f RecvCallback
	// timeout value for the message
	var timeout time.Duration = 5 * time.Second

	// Look for optional parameters
	for _, i := range attr {
		switch v := i.(type) {
		// Callback function to be called when the message is received
		case func(id int, data []byte, err error) bool:
			f = v
		case RecvCallback:
			f = v
		// Timeout value for the message
		case time.Duration:
			timeout = v
		}
	}

	// Send message
	id, err = p.SendTo(p.broker, data)
	if err != nil {
		return
	}

	// Add message to messages queue
	if f != nil {
		p.Messages.add(id, data, f, timeout)
	}

	return
}

// Answer unmarshals answer packet from broker.
func Answer(data []byte) (ans *teomq.Packet, err error) {
	ans = new(teomq.Packet)
	err = ans.UnmarshalBinary(data)
	return
}

// Process answers from broker.
func (p *Producer) process() {

	// Add teonet reader
	p.AddReader(func(c *teonet.Channel, pac *teonet.Packet, e *teonet.Event) bool {

		// Skip not Data events
		if e.Event != teonet.EventData {
			return false
		}

		// Skip not from broker
		if c.Address() != p.broker {
			return false
		}

		// Unmarshal answer
		ans, err := Answer(pac.Data())
		if err != nil {
			log.Printf("answer unmarshal error: %s\n", err)
			return false
		}

		// Find message in messages queue
		_, f, err := p.Messages.get(ans.ID())
		if err != nil {
			log.Printf("answer id %d error: %s\n", ans.ID(), err)
			return false
		}

		// Execute callback
		if f != nil {
			f(ans.ID(), ans.Data(), nil)
		}

		// Delete message
		if !p.commandMode {
			p.Messages.del(ans.ID())
		}

		return true
	})

	// Check timeouts in messages queue, execute callback with error and delete
	// message
	go func() {
		for {
			msg, ok := p.Messages.check()
			if !ok {
				time.Sleep(1 * time.Second)
				continue
			}
			if msg.f != nil {
				msg.f(msg.p.ID(), nil, teonet.ErrTimeout)
			}
			p.Messages.del(msg.p.ID())
		}
	}()
}
