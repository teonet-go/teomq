// Copyright 2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Messages answer map module provides messages answer
// map types and methods.

package teomq

import (
	"sync"

	"github.com/teonet-go/teonet"
)

// answers contain messages answers data and methods to process it.
type answers struct {
	answersMap    // map of messages IDs
	*sync.RWMutex // mutext
}
type answersMap map[answersData]answersData
type answersData struct {
	ch *teonet.Channel // message channel
	id int             // message id
}

// newAnswers creates a new answers object.
func newAnswers() (a *answers) {
	a = new(answers)
	a.RWMutex = new(sync.RWMutex)
	a.answersMap = make(answersMap)
	return
}

// add adds message to the messages answers
func (a *answers) add(producer, consumer answersData) {
	a.Lock()
	defer a.Unlock()
	a.answersMap[consumer] = producer
}

// get returns producer by consumer and delete it if found or returns nil if
// not found.
func (a *answers) get(consumer answersData) (producer *answersData) {
	a.Lock()
	defer a.Unlock()

	p, ok := a.answersMap[consumer]
	if !ok {
		return nil
	}
	producer = &p

	delete(a.answersMap, consumer)
	return
}

// len return length of the answers map
func (a *answers) len() int {
	a.RLock()
	defer a.RUnlock()

	return len(a.answersMap)
}
