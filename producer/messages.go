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
	p *teomq.Packet
	t time.Time
}

// NewMessages creates new messages queue.
func NewMessages() *Messages {
	return &Messages{
		m:       make(map[int]MessagesData),
		RWMutex: &sync.RWMutex{},
	}
}

// add adds new message to messages queue.
func (m *Messages) add(id int, data []byte) {
	m.Lock()
	defer m.Unlock()
	ttl := time.Now().Add(time.Second * 5)
	m.m[id] = MessagesData{teomq.NewPacket(uint32(id), data), ttl}
}

// Get returns message from messages queue.
func (m *Messages) Get(id int) (p *teomq.Packet, err error) {
	m.RLock()
	defer m.RUnlock()

	msg, ok := m.m[id]
	if !ok {
		err = ErrMessageNotFound
	}
	p = msg.p
	return
}

// Del deletes message from messages queue.
func (m *Messages) Del(id int) {
	m.Lock()
	defer m.Unlock()
	delete(m.m, id)
}
