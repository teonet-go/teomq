// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Messages queue module provides messages queue types
// and methods.

package broker

import (
	"container/list"
	"errors"
	"sync"
)

var ErrMessageNotFound = errors.New("message not found")

// queue contain messages queue data and methods to process it.
type queue struct {
	list.List     // list of messages
	*sync.RWMutex // mutext
}

// message is the messageQueue data type.
type message struct {
	from string // Got message from
	id   int    // Message ID
	data []byte // Message data
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

// get returns first element from queue and remove it, or returns nil and error
// if the queue is empty.
func (q *queue) get(removes ...bool) (*message, *list.Element, error) {
	q.Lock()
	defer q.Unlock()

	// Get first element of messages queue
	e := q.Front()
	if e == nil {
		return nil, nil, ErrMessageNotFound
	}

	// Get message from element
	m, ok := e.Value.(*message)
	if !ok {
		return nil, nil, ErrMessageNotFound
	}

	// Remove element from messages queue
	if len(removes) == 0 || removes[0] {
		q.Remove(e)
	}

	return m, e, nil
}

// del removes element from queue.
func (q *queue) del(e *list.Element) {
	q.Lock()
	defer q.Unlock()
	q.Remove(e)
}

// len returns number of elements in queue
func (q *queue) len() int {
	q.RLock()
	defer q.RUnlock()
	return q.Len()
}
