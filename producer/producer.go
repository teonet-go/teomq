// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue Producer package provides producer types and methods.
package producer

import (
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
	teomq.ConnectToBroker(p.Teonet, broker)
	return
}

// Send sends message to broker.
func (p *Producer) Send(data []byte) (id int, err error) {
	id, err = p.SendTo(p.broker, data)
	if err != nil {
		return
	}

	p.Messages.add(id, data)
	return
}

// Answer unmarshals answer packet from broker.
func Answer(data []byte) (ans *teomq.Packet, err error) {
	ans = new(teomq.Packet)
	err = ans.UnmarshalBinary(data)
	return
}
