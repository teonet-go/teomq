// Broker gets messages form producers and resend it to subscriber (consumer).
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
	appShort   = "usrser_mqbroker"
	appVersion = "0.0.1"
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

	// Add API commands
	ApiCommands(teo)

	// Print application address
	addr := teo.Address()
	fmt.Println("Connected to Teonet, this app address:", addr)
	fmt.Println()

	select {}
}

// Commands adds available broker commands.
func Commands(cmd *commands.Commands) {
	fmt.Println("Commands loaded:")

	cmd.Add("num_players", "Number of players.", commands.Teonet, "")
	cmd.Add("num_servers", "Number of players.", commands.Teonet, "")

	cmd.Print()
}

// ApiCommands processing API commands to use teoproxy in web application.
func ApiCommands(teo *broker.Broker) {

	// Create Teonet API
	api := teo.NewAPI(appName, appShort, appName, appVersion)

	api.Add(
		// Create API command "hello"
		func(cmdApi teonet.APInterface) teonet.APInterface {
			const name = "hello"
			cmdApi = teonet.MakeAPI2().
				SetCmd(api.Cmd(129)).                 // Command number cmd = 129
				SetName(name).                        // Command name
				SetShort("get 'hello name' message"). // Short description
				SetUsage("<name string>").            // Usage (input parameter)
				SetReturn("<answer string>").         // Return (output parameters)
				// Command reader (execute when command received)
				SetReader(func(c *teonet.Channel, p *teonet.Packet, data []byte) bool {
					fmt.Printf("got from %s, \"%s\", len: %d, id: %d, tt: %6.3fms\n",
						c, data, len(data), p.ID(),
						float64(c.Triptime().Microseconds())/1000.0,
					)
					data = append([]byte("Hello "), data...)
					api.SendAnswer(cmdApi, c, data, p)
					return true
				}).SetAnswerMode(teonet.DataAnswer)
			return cmdApi
		}(teonet.APIData{}),

		// Create API command "msg"
		func(cmdApi teonet.APInterface) teonet.APInterface {
			const name = "msg"
			cmdApi = teonet.MakeAPI2().
				SetCmd(api.CmdNext()).              // Command number cmd = 130
				SetName(name).                      // Command name
				SetShort("get 'msg data' message"). // Short description
				SetUsage("<name string>").          // Usage (input parameter)
				SetReturn("<answer string>").       // Return (output parameters)
				// Command reader (execute when command received)
				SetReader(func(c *teonet.Channel, p *teonet.Packet, data []byte) bool {
					fmt.Printf("got `%s` from %s, \"%s\", len: %d, id: %d, tt: %6.3fms\n",
						name, c, data, len(data), p.ID(),
						float64(c.Triptime().Microseconds())/1000.0,
					)

					// Send command data to brocker reader
					teo.SendToReader(c, p, data)
					api.SendAnswer(cmdApi, c, []byte("OK"), p)
					return true
				}).SetAnswerMode(teonet.DataAnswer)
			return cmdApi
		}(teonet.APIData{}),
	)

	// Add API reader
	teo.AddReader(api.Reader())

	// Print API
	fmt.Printf("API description:\n\n%s\n\n", api.Help())
}
