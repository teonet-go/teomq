// Copyright 2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Messages queue module provides messages queue types
// and methods.

package teomq

import (
	"container/list"
	"sync"

	"github.com/teonet-go/teonet"
)

// queue contain messages queue data and methods to process it.
type queue struct {
	list.List     // list of messages
	*sync.RWMutex // mutext
}

// message is the messageQueue data type.
type message struct {
	from *teonet.Channel
	id   int
	data []byte
}

// newQueue creates a new queue object.
func newQueue() (q *queue) {
	q = new(queue)
	q.RWMutex = new(sync.RWMutex)
	return
}

// set adds new message to the back of queue.
func (q *queue) set(messages *message) {
	q.Lock()
	defer q.Unlock()
	q.PushBack(messages)
}

// get returns first element from queue and remove it, or returns nil if the
// queue is empty.
func (q *queue) get() (messages *message) {
	q.Lock()
	defer q.Unlock()

	// Get first element of messages queue
	e := q.Front()
	if e == nil {
		return nil
	}

	// Get message from element
	messages, ok := e.Value.(*message)
	if !ok {
		return nil
	}

	// Remove element from messages queue
	q.Remove(e)

	return
}
