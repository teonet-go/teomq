// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Producer send messages.

package producer

import (
	"errors"
	"sync"
	"time"

	"github.com/teonet-go/teomq"
)

var ErrMessageNotFound = errors.New("message not found")

// Messages is messages queue type.
type Messages struct {
	m map[int]MessagesData
	*sync.RWMutex
}
type MessagesData struct {
	f RecvCallback
	p *teomq.Packet
	t time.Time
}
type RecvCallback func(id int, data []byte, err error) bool

// NewMessages creates new messages queue.
func NewMessages() *Messages {
	return &Messages{
		m:       make(map[int]MessagesData),
		RWMutex: &sync.RWMutex{},
	}
}

// add adds new message to messages queue.
func (m *Messages) add(id int, data []byte, f RecvCallback,
	timeout time.Duration) {

	m.Lock()
	defer m.Unlock()
	ttl := time.Now().Add(timeout)
	m.m[id] = MessagesData{f, teomq.NewPacket(uint32(id), data), ttl}
}

// get returns message from messages queue.
func (m *Messages) get(id int) (p *teomq.Packet, f RecvCallback, err error) {
	m.RLock()
	defer m.RUnlock()

	msg, ok := m.m[id]
	if !ok {
		err = ErrMessageNotFound
	}
	p = msg.p
	return
}

// del deletes message from messages queue.
func (m *Messages) del(id int) {
	m.Lock()
	defer m.Unlock()
	delete(m.m, id)
}

// check returns true if message exists in messages queue and timeout expired.
func (m *Messages) check() (msg MessagesData, ok bool) {
	m.RLock()
	defer m.RUnlock()

	for _, msg = range m.m {
		if time.Now().After(msg.t) {
			ok = true
			return
		}
	}

	return
}
