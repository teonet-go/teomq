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
}

// New creates a new Teonet MQueue Consumer object.
func New(appShort, broker string, attr ...interface{}) (co *Consumer, err error) {
	co = new(Consumer)
	co.Teonet, err = teomq.NewTeonet(appShort, append(attr, co.reader)...)
	if err != nil {
		return
	}
	teomq.ConnectToBroker(co.Teonet, broker)
	return
}

// sendAnswer send answer to message received from broker
func (co *Consumer) sendAnswer(pac *teonet.Packet, data []byte) (err error) {
	data, err = Packet{uint32(pac.ID()), data}.MarshalBinary()
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
		log.Printf("got from %s, \"%s\", len: %d, id: %d, tt: %6.3fms\n",
			c, p.Data(), len(p.Data()), p.ID(),
			float64(c.Triptime().Microseconds())/1000.0,
		)

		// Check consumerHello message from new consumer
		if len(p.Data()) == len(teomq.ConsumerAnswer) &&
			string(p.Data()) == string(teomq.ConsumerAnswer) {
			log.Printf("connected to broker\n")
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
