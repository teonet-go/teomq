package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/teonet-go/teomq/commands"
	"github.com/teonet-go/teomq/consumer"
	"github.com/teonet-go/teonet"
)

const (
	appName    = "Teonet messages consumer sample application"
	appShort   = "teomqconsumer"
	appVersion = "0.0.2"
)

func main() {

	// Teonet application logo
	teonet.Logo(appName, appVersion)

	// Log in microseconds
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	// Parse application flags
	var name = flag.String("name", "", "application short name")
	var nomsg = flag.Bool("nomsg", false, "don't show log messages")
	var broker = flag.String("broker", "", "broker address")
	var stat = flag.Bool("stat", false, "show statistics")
	flag.Parse()

	// Check requered parameter -broker
	if len(*broker) == 0 {
		fmt.Println("The broker address should be set. Use -broker flag to set it.")
		os.Exit(0)
	}

	// Set app short name
	short := appShort
	if len(*name) > 0 {
		short = *name
	}

	// Don't show log messages
	if *nomsg {
		log.SetOutput(io.Discard)
	}

	// Set teonet application attributes
	attr := []any{}
	if *stat {
		attr = append(attr, teonet.Stat(true))
	}

	// Create messages consumer reader callback function
	reader := func(p *teonet.Packet) (answer []byte, err error) {
		return []byte("Answer to " + string(p.Data())), nil
	}

	// Add consumer commands
	attr = append(attr, Commands)

	// Create and start new Teonet messages consumer
	teo, err := consumer.New(short, *broker, reader, attr...)
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

	cmd.Add("version", "Get consumer version.", commands.Teonet, "",
		func(cmd *commands.CommandData, processIn commands.ProcessIn, data any) (
			[]byte, error) {

			return []byte(appVersion), nil
		})

	cmd.Print()
}
