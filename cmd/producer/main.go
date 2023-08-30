package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teonet"
)

const (
	appName    = "Teonet messages producer sample application"
	appShort   = "teomqproducer"
	appVersion = "0.0.1"

	broker = "og71X6Y8Z44xuTU1Y2W4G9GkUsKmxnvvd9r"
)

var name = flag.String("name", "", "application short name")
var delay = flag.Int("delay", 1000000, "send delay in microsecond")
var nomsg = flag.Bool("nomsg", false, "don't show log messages")

func main() {

	// Teonet application logo
	teonet.Logo(appName, appVersion)

	// Parse application flags
	flag.Parse()
	short := appShort
	if len(*name) > 0 {
		short = *name
	}

	// Don't show log messages
	if *nomsg {
		log.SetOutput(io.Discard)
	}

	// Create and start new Teonet messages producer
	teo, err := teomq.NewProducer(short, broker, reader, teonet.Stat(true))
	if err != nil {
		panic("can't connect to Teonet, error: " + err.Error())
	}

	// Print application address
	addr := teo.Address()
	fmt.Println("Connected to Teonet, this app address:", addr)

	// Message sender
	for i := 1; ; i++ {
		data := []byte(fmt.Sprintf("Hello wold #%d!", i))

		_, err := teo.SendTo(broker, data)
		if err != nil {
			fmt.Printf("send to error: %s\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Printf("send message: %s\n", string(data))

		// This application use reader function to receive message. The code
		// below shows how to get messages after send.
		//
		// data, err = teo.WaitFrom(broker)
		// if err != nil {
		// 	fmt.Printf("wait from error: %s\n", err)
		// 	continue
		// }
		// fmt.Printf("got answer: %s\n", string(data))
		//

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
		log.Printf("got answer: %s\n", string(p.Data()))
	}

	return false
}
