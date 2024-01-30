// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue. This is a teonet golang packge wich creates and
// manages messages queue between message producers and consumers.
package teomq

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/teonet-go/teonet"
)

var (
	ConsumerHello  = []byte("Consumer")
	ConsumerAnswer = []byte("Connected to broker")
)

// NewTeonet creates new teonet connection and connect to teonet.
func NewTeonet(appShort string, attr ...interface{}) (teo *teonet.Teonet, err error) {

	// Create teonet connection
	teo, err = teonet.New(appShort, attr...)
	if err != nil {
		return
	}

	// Connect to teonet
	for teo.Connect() != nil {
		time.Sleep(1 * time.Second)
	}

	// Process Ctrl+C signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	go func() {
		<-quit
		fmt.Println("\nCtrl+C signal received. Exiting...")
		teo.Close()
		os.Exit(0)
	}()

	return
}

// connectToBroker connects producer or consumer to brokers peer
func ConnectToBroker(teo *teonet.Teonet, addr string) (err error) {
	if err = teo.ConnectTo(addr); err != nil {
		return
	}
	return
}
