// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue Consumer package provides consumer types and methods.
package consumer

import (
	"log"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teonet"
)

// Consumer is Teonet messages queue consumer type.
type Consumer struct {
	*teonet.Teonet
	ProcessMessage
}

type ProcessMessage func(p *teonet.Packet) (answer []byte, err error)

// New creates a new Teonet MQueue Consumer object.
//
// Args:
//
//	appShort: teonet application short name
//	broker: broker address
//	reader: consumer message processor callback function:
//	        func(p *teonet.Packet) ([]byte, error)
//	attr: teonet application attributes
//
// Returns:
//
//	*Consumer: new Teonet MQueue Consumer object
//	error: error if occurred
func New(appShort, broker string, reader ProcessMessage, attr ...interface{}) (co *Consumer, err error) {
	// Create new consumer object and connect to teonet
	co = new(Consumer)

	// Append custom Reader to teonet application attributes
	attr = append(attr, co.reader)

	// Connect to teonet
	co.Teonet, err = teomq.NewTeonet(appShort, attr...)
	if err != nil {
		return
	}

	// Add custom Reader if it exists in attributes
	co.ProcessMessage = reader

	// Connect to broker
	err = teomq.ConnectToBroker(co.Teonet, broker)
	return
}

// sendAnswer send answer to message received from broker
func (co *Consumer) sendAnswer(pac *teonet.Packet, data []byte) (err error) {
	data, err = teomq.NewPacket(uint32(pac.ID()), data).MarshalBinary()
	if err != nil {
		return
	}
	_, err = co.SendTo(pac.From(), data)
	return
}

// reader is Consumer teonet main reader connected to brokers peer
// and process incoming teonet messages
func (co *Consumer) reader(c *teonet.Channel, p *teonet.Packet,
	e *teonet.Event) bool {

	// On connected
	if e.Event == teonet.EventConnected {
		log.Printf("connected to %s\n", c)
		c.Send(teomq.ConsumerHello)
		return false
	}

	// On disconnected
	if e.Event == teonet.EventDisconnected {
		log.Printf("disconnected from %s\n", c)
		return false
	}

	// Skip not Data events
	if e.Event != teonet.EventData {
		return false
	}

	// In client mode get messages and ...
	if c.ClientMode() {

		// Print received message
		log.Printf("got  id %d, len: %d, from %s, tt: %6.3fms\n",
			p.ID(), len(p.Data()), c,
			float64(c.Triptime().Microseconds())/1000.0,
		)

		// Check consumerHello message from new consumer
		if len(p.Data()) == len(teomq.ConsumerAnswer) &&
			string(p.Data()) == string(teomq.ConsumerAnswer) {
			log.Printf("connected to broker\n")
			return true
		}

		// Process message and Send answer
		go func() {
			var err error
			var answer []byte
			if co.ProcessMessage != nil {
				answer, err = co.ProcessMessage(p)
			} else {
				answer = []byte("Answer to " + string(p.Data()))
			}
			if err != nil {
				log.Printf("process message id %d, from %s, error: %s\n", p.ID(), c, err)
				return
			}

			// Send answer
			err = co.sendAnswer(p, answer)
			if err != nil {
				log.Printf("send id %d, len: %d, to %s, error: %s\n", p.ID(), len(answer), c, err)
			} else {
				log.Printf("send id %d, len: %d, to %s\n", p.ID(), len(answer), c)
			}
		}()

		return true
	}

	return false
}
