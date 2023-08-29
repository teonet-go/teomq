package main

import (
	"fmt"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teonet"
)

const (
	appName    = "Teonet messages broker sample application"
	appShort   = "teomqbroker"
	appVersion = "0.0.1"
)

func main() {

	// Teonet application logo
	teonet.Logo(appName, appVersion)

	// Create and start new Teonet messages broker
	teo, err := teomq.NewBroker(appShort, teonet.Stat(true))
	if err != nil {
		panic("can't connect to Teonet, error: " + err.Error())
	}

	// Print application address
	addr := teo.Address()
	fmt.Println("Connected to Teonet, this app address:", addr)

	select {}
}
