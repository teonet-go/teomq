// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Packet module.

package teomq

import (
	"bytes"
	"encoding/binary"
)

// Packet defines answer message
type Packet struct {
	id   uint32
	data []byte
}

// NewPacket creates new packet.
func NewPacket(id uint32, data []byte) *Packet {
	return &Packet{id, data}
}

// ID returns message ID.
func (p Packet) ID() int {
	return int(p.id)
}

// Data returns message data.
func (p Packet) Data() []byte {
	return p.data
}

// MarshalBinary marshals binary packet
func (p Packet) MarshalBinary() (data []byte, err error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, p.id)
	binary.Write(buf, binary.LittleEndian, p.data)

	data = buf.Bytes()
	return
}

// UnmarshalBinary unmarshals binary packet
func (p *Packet) UnmarshalBinary(data []byte) (err error) {
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
