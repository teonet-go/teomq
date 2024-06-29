package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/teonet-go/teomq/broker"
	"github.com/teonet-go/teomq/commands"
	"github.com/teonet-go/teonet"
)

const (
	appName    = "Teonet messages broker (command scheme) sample application"
	appShort   = "teomqbroker-c"
	appVersion = "0.0.2"
)

func main() {

	// Teonet application logo
	teonet.Logo(appName, appVersion)

	// Log in microseconds
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	// Parse application flags
	var nomsg = flag.Bool("nomsg", false, "don't show log messages")
	var stat = flag.Bool("stat", false, "show statistics")
	flag.Parse()

	// Don't show log messages
	if *nomsg {
		log.SetOutput(io.Discard)
	}

	// Set teonet application attributes
	attr := []any{}
	if *stat {
		attr = append(attr, teonet.Stat(true))
	}

	// Add broker commands
	attr = append(attr, Commands)

	// Create and start new Teonet messages broker
	teo, err := broker.New(appShort, attr...)
	if err != nil {
		panic("can't connect to Teonet, error: " + err.Error())
	}

	// Print application address
	addr := teo.Address()
	fmt.Println("Connected to Teonet, this app address:", addr)

	select {}
}

// Commands adds available broker commands.
func Commands(cmd *commands.Commands) {
	fmt.Println("Commands loaded:")

	cmd.Add("version", "Get consumer version.", commands.Teonet, "")

	cmd.Print()
}
