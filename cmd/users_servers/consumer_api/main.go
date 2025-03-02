// Consumer gets messages from broker and prints it to log.
// This consumer uses Teonet API to send messages to broker.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/kirill-scherba/command/v2"
	"github.com/teonet-go/teomq/consumer"
	"github.com/teonet-go/teonet"
)

const (
	appName    = "Teonet messages consumer sample application"
	appShort   = "usrser_mqconsumer"
	appVersion = "0.0.1"
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
	var api = flag.Bool("api", false, "use teonet api")
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

	// Add consumer commands
	attr = append(attr, Commands)

	// Add consumer teonet api interface
	if *api {
		attr = append(attr, consumer.API(true))
	}

	// Create and start new Teonet messages consumer
	teo, err := consumer.New(short, *broker, nil, attr...)
	if err != nil {
		panic("can't connect to Teonet, error: " + err.Error())
	}

	// Print application address
	addr := teo.Teonet.Address()
	fmt.Println("Connected to Teonet, this app address:", addr)

	select {}
}

// Commands adds available broker commands.
func Commands(c *command.Commands) {
	fmt.Println("Commands loaded:")

	c.Add("version", "Get consumer version.", command.Teonet, "{data}/{description}",
		"", "", "",
		func(cmd *command.CommandData, processIn command.ProcessIn, data any) (
			[]byte, error) {

			// Get vars
			vars, err := c.Vars(data)
			if err != nil {
				return nil, err
			}

			return fmt.Appendf(nil, "version: %s, data: %s", appVersion, vars["data"]), nil
		})

	c.Add("num_players", "Number of players.", command.Teonet, "{num_players}",
		"", "", "",
		func(cmd *command.CommandData, processIn command.ProcessIn, data any) (
			[]byte, error) {

			// Get vars
			vars, err := c.Vars(data)
			if err != nil {
				return nil, err
			}

			log.Printf("got command %s: %v", cmd.Cmd, vars[cmd.Cmd])

			return nil, nil
		})

	c.Add("num_servers", "Number of servers.", command.Teonet, "{num_servers}",
		"", "", "",
		func(cmd *command.CommandData, processIn command.ProcessIn, data any) (
			[]byte, error) {

			// Get vars
			vars, err := c.Vars(data)
			if err != nil {
				return nil, err
			}

			log.Printf("got command %s: %v", cmd.Cmd, vars[cmd.Cmd])

			return nil, nil
		})

	c.Print()
}
