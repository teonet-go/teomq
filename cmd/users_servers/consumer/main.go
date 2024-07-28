// Consumer gets messages from broker and prints it to log.
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
func Commands(c *commands.Commands) {
	fmt.Println("Commands loaded:")

	c.Add("version", "Get consumer version.", commands.Teonet, "{data}/{description}",
		func(cmd *commands.CommandData, processIn commands.ProcessIn, data any) (
			[]byte, error) {

			// Parse teonet parameters
			_, _, vars, err := c.Unmarshal(data.([]byte))
			if err != nil {
				return nil, err
			}

			return []byte(fmt.Sprintf("version: %s, data: %s", appVersion, vars["data"])), nil
		})

	c.Add("num_players", "Number of players.", commands.Teonet, "{num_players}",
		func(cmd *commands.CommandData, processIn commands.ProcessIn, data any) (
			[]byte, error) {

			// Parse teonet parameters
			_, _, vars, err := c.Unmarshal(data.([]byte))
			if err != nil {
				return nil, err
			}

			log.Printf("got command %s: %v", cmd.Cmd, vars[cmd.Cmd])

			return nil, nil
		})

	c.Add("num_servers", "Number of servers.", commands.Teonet, "{num_servers}",
		func(cmd *commands.CommandData, processIn commands.ProcessIn, data any) (
			[]byte, error) {

			// Parse teonet parameters
			_, _, vars, err := c.Unmarshal(data.([]byte))
			if err != nil {
				return nil, err
			}

			log.Printf("got command %s: %v", cmd.Cmd, vars[cmd.Cmd])

			return nil, nil
		})

	c.Print()
}