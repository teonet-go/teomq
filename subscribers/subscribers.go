// Copyright 2024 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. Subscribers map package provides subscribers types
// and methods.
package subscribers

import (
	"fmt"
	"sync"

	"slices"

	"github.com/teonet-go/teonet"
)

const (
	CmdSubscribe   = "subscribe"
	CmdUnsubscribe = "unsubscribe"
)

// Subscribers map and mutex to store commands by tru channel
type Subscribers struct {
	m   SubscribersMap
	mut *sync.RWMutex
}
type SubscribersMap map[*teonet.Channel][]string

// init PlayersOnlineSubscribersMap
func (s *Subscribers) Init() {
	s.m = make(SubscribersMap)
	s.mut = new(sync.RWMutex)
}

// CheckCommand returns true if command exists in subscribers map.
func (s *Subscribers) CheckCommand(ch *teonet.Channel, command string) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()

	// Check channel exists in subscribers
	if _, ok := s.m[ch]; !ok {
		fmt.Print("!!! channel does not exists in subscribers map")
		return false
	}

	// Check command exists in commands slice
	// if !slices.Contains(s.m[ch], command) {
	// 	fmt.Printf("!!! command %s does not exists in subscribers map/n", command)
	// 	return true
	// }

	return true
}

// Add teonet channel to subscribers map
func (s *Subscribers) Add(ch *teonet.Channel, command string) {
	s.mut.Lock()
	defer s.mut.Unlock()

	// Create new slice if channel does not exists
	if _, ok := s.m[ch]; !ok {
		s.m[ch] = []string{command}
		return
	}

	// Check command already exists in commands slice
	if slices.Contains(s.m[ch], command) {
		return
	}

	// Insert new command to commands slice
	s.m[ch] = append(s.m[ch], command)
}

// DelCmd deletes command from subscribers map
func (s *Subscribers) DelCmd(ch *teonet.Channel, command string) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if _, ok := s.m[ch]; ok {
		for i, v := range s.m[ch] {
			if v == command {
				s.m[ch] = slices.Delete(s.m[ch], i, i+1)
				break
			}
		}
	}
}

// Del deletes tru ch from subscribers map
func (s *Subscribers) Del(ch *teonet.Channel) {
	s.mut.Lock()
	defer s.mut.Unlock()

	delete(s.m, ch)
}
