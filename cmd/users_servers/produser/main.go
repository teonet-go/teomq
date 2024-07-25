// Producer generates number of users or numbers of servers depending on command
// application parameter and send it to the broker.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"math/rand/v2"

	"github.com/teonet-go/teomq/producer"
	"github.com/teonet-go/teonet"
)

const (
	appName    = "Teonet messages producer sample application"
	appShort   = "usrser_mqproducer"
	appVersion = "0.0.1"
)

func main() {

	// Teonet application logo
	teonet.Logo(appName, appVersion)

	// Log in microseconds
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	// Parse application flags
	var name = flag.String("name", "", "application short name")
	var command = flag.String("command", "none", "command name")
	var delay = flag.Int("delay", 1000000, "send delay in microsecond")
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

	// Set teonet application attributes
	attr := []any{}
	if *stat {
		attr = append(attr, teonet.Stat(true))
	}

	// Set producer command schema
	attr = append(attr, producer.CommandMode(true))

	// Create and start new Teonet messages producer
	prod, err := producer.New(short, *broker, attr...)
	if err != nil {
		panic("can't connect to Teonet, error: " + err.Error())
	}

	// Print application address
	addr := prod.Address()
	fmt.Println("Connected to Teonet, this app address:", addr)

	s2 := rand.NewPCG(42, 1024)
	r2 := rand.New(s2)

	// Send messages to broker
	for {

		// Generate random value 1..100
		value := r2.IntN(100) + 1

		// Make message to send
		data := []byte(fmt.Sprintf("%s/%d", *command, value))

		// Send message to broker
		id, err := prod.Send(data)
		log.Printf("sent %s = %d to broker, id: %d, err: %v", *command, value, id, err)

		// Sleep
		time.Sleep(time.Microsecond * time.Duration(*delay))
	}
}
