// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Messages answer map module provides messages answer
// map types and methods.

package broker

import (
	"errors"
	"sync"
)

var ErrAnswerNotFound = errors.New("answer not found")

// answers contain messages answers data and methods to process it.
type answers struct {
	answersMap    // map of messages IDs
	*sync.RWMutex // mutext
}
type answersMap map[answersData]answersData
type answersData struct {
	addr string // message channel
	id   int    // message id
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

// get returns producers answerData by consumers answerData and delete it if
// found or returns error ErrAnswerNotFound if not found.
func (a *answers) get(consumer answersData) (*answersData, error) {
	a.Lock()
	defer a.Unlock()

	p, ok := a.answersMap[consumer]
	if !ok {
		return nil, ErrAnswerNotFound
	}

	delete(a.answersMap, consumer)
	return &p, nil
}

// len return length of the answers map
func (a *answers) len() int {
	a.RLock()
	defer a.RUnlock()

	return len(a.answersMap)
}
