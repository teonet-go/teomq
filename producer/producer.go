// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue Producer package provides producer types and methods.
package producer

import (
	"log"
	"time"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teonet"
)

// Producer is Teonet messages queue producers type.
type Producer struct {
	broker string
	*teonet.Teonet
	*Messages
}

// New creates a new Teonet Message Queue Producer object.
func New(appShort, broker string, attr ...interface{}) (p *Producer, err error) {
	p = new(Producer)
	p.broker = broker
	p.Teonet, err = teomq.NewTeonet(appShort, attr...)
	if err != nil {
		return
	}
	p.Messages = NewMessages()
	p.process()
	teomq.ConnectToBroker(p.Teonet, broker)
	return
}

// Send sends message to broker.
func (p *Producer) Send(data []byte, attr ...any) (id int, err error) {

	// Parse attributes
	var f RecvCallback
	var timeout time.Duration = 5 * time.Second
	for _, i := range attr {
		switch v := i.(type) {
		case func(id int, data []byte, err error) bool:
			f = v
		case RecvCallback:
			f = v
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
			log.Printf("!!! answer id %d not found: %s\n", ans.ID(), err)
			return false
		}

		// Execute callback
		if f != nil {
			f(ans.ID(), ans.Data(), nil)
		}

		// Delete message
		p.Messages.del(ans.ID())

		return true
	})

	// Check timeouts in messages queue
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
