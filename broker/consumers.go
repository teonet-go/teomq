// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Consumers list module provides consumers list types
// and methods.

package broker

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
	list.List                   // list of consumers
	indexMap                    // map of list elements by consumer channel
	*sync.RWMutex               // mutext
	element       *list.Element // current list element used in get function
}
type indexMap map[*teonet.Channel]*list.Element

// newConsumers creates a new consumers object.
func newConsumers() (c *consumers) {
	c = new(consumers)
	c.indexMap = make(indexMap)
	c.RWMutex = new(sync.RWMutex)
	return
}

// add adds new consumer to the back of consumers list.
func (c *consumers) add(ch *teonet.Channel) error {
	c.Lock()
	defer c.Unlock()

	// Check consumer already exists
	if c.existsUnsafe(ch) != nil {
		return ErrConsumerAlreadyExists
	}

	// Insert new consumer to consumers list and index
	e := c.PushBack(ch)
	c.indexMap[ch] = e

	return nil
}

// del delete consumer from the consumers list.
func (c *consumers) del(ch *teonet.Channel) error {
	c.Lock()
	defer c.Unlock()

	if e := c.existsUnsafe(ch); e != nil {
		if c.element == e {
			c.element = e.Next()
		}
		c.Remove(e)
		delete(c.indexMap, ch)
		return nil
	}

	return ErrConsumerNotFound
}

// get gets next consumer from consumers list.
func (c *consumers) get() (*teonet.Channel, error) {
	c.Lock()
	defer c.Unlock()

	// Check length of consumer
	if c.Len() == 0 {
		return nil, ErrConsumerNotFound
	}

	// Get current element from list
	if c.element == nil {
		c.element = c.Front()
	} else if c.element = c.element.Next(); c.element == nil {
		c.element = c.Front()
	}
	// TODO: perhaps this condition is not needed here, because we first check
	// the length of the list and the element of the list must be found.
	if c.element == nil {
		return nil, ErrConsumerNotFound
	}

	// Get list value
	if ch, ok := c.element.Value.(*teonet.Channel); ok {
		return ch, nil
	}

	return nil, ErrConsumerNotFound
}

// list returns consumers list.
// TODO: Get list of channels which was subscribed to this command
func (c *consumers) list(cmd string) (l []*teonet.Channel) {
	c.RLock()
	defer c.RUnlock()

	for e := c.Front(); e != nil; e = e.Next() {
		ch, ok := e.Value.(*teonet.Channel)
		if !ok {
			continue
		}
		l = append(l, ch)
	}

	return
}

// exists returns true if consumer exists in list or false if not.
func (c *consumers) exists(ch *teonet.Channel) bool {
	c.RLock()
	defer c.RUnlock()
	return c.existsUnsafe(ch) != nil
}

// existsUnsafe returns list.Element if consumer exists in list or nil if not.
func (c *consumers) existsUnsafe(ch *teonet.Channel) (e *list.Element) {
	e, exists := c.indexMap[ch]
	if !exists {
		return nil
	}
	return
}

// len returns consumers list length.
func (c *consumers) len() int {
	c.RLock()
	defer c.RUnlock()
	return c.Len()
}
