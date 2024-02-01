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
	appVersion = "0.0.1"
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
	attr = append(attr, reader)

	// Create and start new Teonet messages producer
	teo, err := producer.New(short, *broker, attr...)
	if err != nil {
		panic("can't connect to Teonet, error: " + err.Error())
	}

	// Print application address
	addr := teo.Address()
	fmt.Println("Connected to Teonet, this app address:", addr)

	// Message sender
	for i := 1; ; i++ {
		data := []byte(fmt.Sprintf("Hello wold #%d!", i))

		_, err := teo.SendTo(*broker, data)
		if err != nil {
			fmt.Printf("send to error: %s\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Printf("send message: %s\n", string(data))

		// This application use common reader function to receive all messages
		// (all answers from broker). The code below shows how to get answer
		// after send. In this code we check that all answers was received
		// during timeout. The timeout is 5 seconds.
		//
		// go func(id int, msg []byte) {
		// 	data, err = teo.WaitFrom(*broker /* , uint32(id) */ /* , 5*time.Second */)

		// 	if err != nil {
		// 		fmt.Printf(
		// 			"no reply to sent message %s with id %d, error: %s\n",
		// 			msg, id, err,
		// 		)
		// 		return
		// 	}
		// 	fmt.Printf("!!! got answer: %s\n", string(data))
		// }(id, data)

		time.Sleep(time.Microsecond * time.Duration(*delay))
	}

	// select {}
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

	// In client mode get messages and ...
	if c.ClientMode() {

		// Print received message
		log.Printf("got answer id %d, message: %s\n", p.ID(), p.Data())
	}

	return false
}