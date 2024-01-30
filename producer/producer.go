// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue Producer package provides producer types and methods.
package producer

import (
	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teonet"
)

// Producer is Teonet messages queue producers type.
type Producer struct {
	*teonet.Teonet
}

// New creates a new Teonet MQueue Producer object.
func New(appShort, broker string, attr ...interface{}) (pr *Producer, err error) {
	pr = new(Producer)
	pr.Teonet, err = teomq.NewTeonet(appShort, attr...)
	if err != nil {
		return
	}
	teomq.ConnectToBroker(pr.Teonet, broker)
	return
}
