// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Consumer packet module.

package consumer

import (
	"bytes"
	"encoding/binary"
)

// Packet defines consumer answer message
type Packet struct {
	id   uint32
	data []byte
}

// ID returns consumer message ID.
func (p Packet) ID() int {
	return int(p.id)
}

// Data returns consumer message data.
func (p Packet) Data() []byte {
	return p.data
}

// MarshalBinary marshals consumer binary packet
func (p Packet) MarshalBinary() (data []byte, err error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, p.id)
	binary.Write(buf, binary.LittleEndian, p.data)

	data = buf.Bytes()
	return
}

// UnmarshalBinary unmarshals consumer binary packet
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
