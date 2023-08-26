// Copyright 2023 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. This is a teonet golang packge wich creates and
// manages messages queue between message producers and consumers.
package teomqueue

import (
	"fmt"

	"github.com/teonet-go/teonet"
)

// MQueue is Teonet messages broker type.
type MQueue struct {
	teo *teonet.Teonet
	*queue
}

// New creates a new MQueue Broker object.
func New(appShort string) (mq *MQueue, err error) {
	mq = new(MQueue)
	mq.queue = newQueue()
	mq.teo, err = mq.newTeonet(appShort)
	return
}

// newTeonet creates new teonet connection.
func (mq *MQueue) newTeonet(appShort string) (*teonet.Teonet, error) {
	return teonet.New(appShort, mq.reader)
}

// reader is main teonet application reader - receive and process incoming 
// teonet messages
func (mq *MQueue) reader(c *teonet.Channel, p *teonet.Packet, e *teonet.Event) bool {

	// Skip not Data events
	if e.Event != teonet.EventData {
		return false
	}

	// In server mode
	if c.ServerMode() {

		// Print received message
		fmt.Printf("got from %s, \"%s\", len: %d, id: %d, tt: %6.3fms\n",
			c, p.Data(), len(p.Data()), p.ID(),
			float64(c.Triptime().Microseconds())/1000.0,
		)

		// Send answer
		answer := []byte("Teonet answer to " + string(p.Data()))
		c.Send(answer)
	}

	return true
}
