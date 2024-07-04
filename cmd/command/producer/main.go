package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/teonet-go/teomq/producer"
	"github.com/teonet-go/teonet"
)

const (
	appName    = "Teonet messages producer sample application"
	appShort   = "teomqproducer"
	appVersion = "0.0.2"
)

func main() {

	// Teonet application logo
	teonet.Logo(appName, appVersion)

	// Log in microseconds
	log.SetFlags(log.Flags() | log.Lmicroseconds)

	// Parse application flags
	var name = flag.String("name", "", "application short name")
	var delay = flag.Int("delay", 1000000, "send delay in microsecond")
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

	// Add custom Reader to process additional info or process messages without
	// answer callback function
	attr = append(attr, reader)

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

	// Message sender
	for i := 1; ; i++ {

		// Make message to send
		data := []byte("version/some_data")

		// Answer callback function
		answer := func(id int, data []byte, err error) bool {
			if err != nil {
				log.Printf("message id %d removed from answers queue\n", id)
				return true
			}
			log.Printf("recv answer  id %d: %s\n", id, data)
			return true
		}

		// Send message to broker
		id, err := prod.Send(data, answer)
		if err != nil {
			fmt.Printf("send to error: %s\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Printf("send message id %d: %s\n", id, string(data))

		time.Sleep(time.Microsecond * time.Duration(*delay))
	}
}

// reader is Producer teonet main reader connected to brokers peer
// and process incoming teonet messages
func reader(c *teonet.Channel, p *teonet.Packet, e *teonet.Event) bool {

	// On connected
	if e.Event == teonet.EventConnected {
		fmt.Printf("connected to %s\n", c)
		return false
	}

	if e.Event == teonet.EventDisconnected {
		fmt.Printf("disconnected from %s\n", c)
		return false
	}

	// Skip not Data events
	if e.Event != teonet.EventData {
		return false
	}

	// In client mode get and process messages
	if c.ClientMode() {
		// Some additional incoming messages processing may be done here.
		// For example:

		// Unmarshal answer
		// ans, err := producer.Answer(p.Data())
		// if err != nil {
		// 	log.Printf("answer unmarshal error: %s\n", err)
		// 	return false
		// }

		// Print received message
		// log.Printf("recv answer  id %d: %s\n", ans.ID(), ans.Data())
	}

	return false
}
