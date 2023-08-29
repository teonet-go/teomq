// Copyright 2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Consumers list module provides consumers list types
// and methods.

package teomq

import (
	"container/list"
	"errors"
	"sync"

	"github.com/teonet-go/teonet"
)

var (
	ErrConsumerNotFound      = errors.New("consumer not found")
	ErrConsumerAlreadyExists = errors.New("consumer already exists")
)

// queue contain messages queue data and methods to process it.
type consumers struct {
	list.List         // list of consumers
	*sync.RWMutex     // mutext
	idx           int // current index in list used in get function
}

// newConsumers creates a new consumers object.
func newConsumers() (c *consumers) {
	c = new(consumers)
	c.RWMutex = new(sync.RWMutex)
	return
}

// add adds new consumer to the back of consumers list.
func (c *consumers) add(ch *teonet.Channel) error {
	c.Lock()
	defer c.Unlock()

	// Check consumer already exists
	if c.existsUnsafe(ch) {
		return ErrConsumerAlreadyExists
	}

	// Insert new consumer to consumers list
	c.PushBack(ch)

	return nil
}

// del delete consumer from the consumers list.
func (c *consumers) del(ch *teonet.Channel) error {
	c.Lock()
	defer c.Unlock()

	// Range by consumers list to find input teonet channel
	for e := c.Front(); e != nil; e = e.Next() {
		if v, ok := e.Value.(*teonet.Channel); ok && v == ch {
			c.Remove(e)
			return nil
		}
	}

	return ErrConsumerNotFound
}

// get gets next consumer from consumers list
func (c *consumers) get() (ch *teonet.Channel) {
	c.Lock()
	defer c.Unlock()

	// Check length of consumer
	var l = c.Len()
	if l == 0 {
		return
	}

	// Check index more than length
	if c.idx >= l {
		c.idx = 0
	}

	// Range by consumers list to find idx teonet channel
	var i int
	for e := c.Front(); e != nil; e = e.Next() {
		if i == c.idx {
			c.idx++
			if ch, ok := e.Value.(*teonet.Channel); ok {
				return ch
			}
		}
		i++
	}

	return
}

// existsUnsafe returns true if consumer exists in consumers list
func (c *consumers) existsUnsafe(ch *teonet.Channel) bool {
	for e := c.Front(); e != nil; e = e.Next() {
		if v, ok := e.Value.(*teonet.Channel); ok && v == ch {
			return true
		}
	}
	return false
}

// len returns consumers list length
func (c *consumers) len() int {
	c.RLock()
	defer c.RUnlock()
	return c.Len()
}
