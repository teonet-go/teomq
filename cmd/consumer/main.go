package main

import (
	"flag"
	"fmt"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teonet"
)

const (
	appName    = "Teonet messages consumer sample application"
	appShort   = "teomqconsumer"
	appVersion = "0.0.1"

	broker = "og71X6Y8Z44xuTU1Y2W4G9GkUsKmxnvvd9r"
)

var name = flag.String("name", "", "application short name")

func main() {

	// Teonet application logo
	teonet.Logo(appName, appVersion)

	flag.Parse()
	short := appShort
	if len(*name) > 0 {
		short = *name
	}

	// Create and start new Teonet messages consumer
	teo, err := teomq.NewConsumer(short, broker, teonet.Stat(true))
	if err != nil {
		panic("can't connect to Teonet, error: " + err.Error())
	}

	// Print application address
	addr := teo.Address()
	fmt.Println("Connected to Teonet, this app address:", addr)

	select {}
}
