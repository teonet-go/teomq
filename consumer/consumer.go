// Copyright 2023-24 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Teonet messages queue Consumer package provides consumer types and methods.
package consumer

import (
	"fmt"
	"log"

	"github.com/teonet-go/teomq"
	"github.com/teonet-go/teomq/commands"
	"github.com/teonet-go/teonet"
)

// Consumer is Teonet messages queue consumer type.
type Consumer struct {
	*teonet.Teonet
	*teonet.APIClient
	ProcessMessage
	*commands.Commands
}

type ProcessMessage func(p *teonet.Packet) (answer []byte, err error)

type API bool

// New creates a new Teonet MQueue Consumer object.
//
// Args:
//
//	appShort: teonet application short name
//	broker: broker address
//	reader: consumer message processor callback function:
//	        func(p *teonet.Packet) ([]byte, error)
//	attr: teonet application attributes
//
// Returns:
//
//	*Consumer: new Teonet MQueue Consumer object
//	error: error if occurred
func New(appShort, broker string, reader ProcessMessage, attr ...interface{}) (
	co *Consumer, err error) {

	// Create new consumer object and connect to teonet
	co = new(Consumer)

	// Append custom Reader to teonet application attributes
	attr = append(attr, co.reader)

	// Add consumer commands in command schema
	attr = co.addCommands(attr...)

	// Get connectAPI attribute
	attr, connectAPI := co.addAPI(attr...)

	// Connect to teonet
	co.Teonet, err = teomq.NewTeonet(appShort, attr...)
	if err != nil {
		return
	}

	// Add custom Reader if it exists in attributes
	co.ProcessMessage = reader

	// Subscribe to broker commands when connected to broker
	co.Teonet.WhenConnectedTo(broker, func() {
		go func() {
			// Add teonet api interface
			if connectAPI {
				co.API(broker)
			}

			// Subscribe to broker commands
			err = co.subscribeCommands(broker)
		}()
	})

	// Connect to broker
	err = teomq.ConnectToBroker(co.Teonet, broker)
	if err != nil {
		return
	}

	return
}

// API connects to brokers api
func (co *Consumer) API(broker string) (err error) {

	co.APIClient, err = co.Teonet.NewAPIClient(broker)
	if err != nil {
		log.Println("can't connect to broker api, error:", err)
		return
	}
	log.Println("connected to broker api:", co.APIClient.String())

	return
}

// subscribe subscribe to brokers command.
func (co *Consumer) subscribe(broker, command string) (err error) {
	data := []byte(fmt.Sprintf("subscribe/%s", command))
	if co.APIClient == nil {
		co.Teonet.SendTo(broker, data)
		return
	}
	co.APIClient.SendTo("msg", data)
	return
}

// addCommands adds command schema to consumer.
func (co *Consumer) addCommands(attr ...interface{}) (outattr []interface{}) {

	outattr = attr
	for i, v := range attr {
		switch v := v.(type) {
		case func(*commands.Commands):
			fmt.Println("Command schema is on")
			outattr = append(attr[:i], attr[i+1:]...)

			co.Commands = new(commands.Commands)
			co.Commands.Init()

			v(co.Commands)
			return
		}
	}

	return
}

func (co *Consumer) addAPI(attr ...interface{}) (outattr []interface{}, ok bool) {
	outattr = attr
	for i, v := range attr {
		switch v.(type) {
		case API:
			fmt.Println("API is on")
			outattr = append(attr[:i], attr[i+1:]...)
			ok = true
			return
		}
	}
	return
}

// subscribeCommands subscribe to brokers commands.
func (co *Consumer) subscribeCommands(broker string) (err error) {
	co.Commands.ForEach(func(command string, cmd *commands.CommandData) {
		err = co.subscribe(broker, command)
	})
	return
}

// sendAnswer send answer to message received from broker
func (co *Consumer) sendAnswer(pac *teonet.Packet, data []byte) (err error) {
	data, err = teomq.NewPacket(uint32(pac.ID()), data).MarshalBinary()
	if err != nil {
		return
	}
	if co.APIClient == nil {
		_, err = co.Teonet.SendTo(pac.From(), data)
		return
	}
	_, err = co.APIClient.SendTo("msg", data)
	return
}

// reader is Consumer teonet main reader connected to brokers peer
// and process incoming teonet messages
func (co *Consumer) reader(c *teonet.Channel, p *teonet.Packet,
	e *teonet.Event) bool {

	// On connected
	if e.Event == teonet.EventConnected {
		log.Printf("connected to %s\n", c)
		c.Send(teomq.ConsumerHello)
		return false
	}

	// On disconnected
	if e.Event == teonet.EventDisconnected {
		log.Printf("disconnected from %s\n", c)
		return false
	}

	// Skip not Data events
	if e.Event != teonet.EventData {
		return false
	}

	// In client mode get messages and ...
	if c.ClientMode() {

		// Print received message
		// log.Printf("got  id %d, len: %d, from %s, tt: %6.3fms\n",
		// 	p.ID(), len(p.Data()), c,
		// 	float64(c.Triptime().Microseconds())/1000.0,
		// )

		// Check consumerHello message from new consumer
		if len(p.Data()) == len(teomq.ConsumerAnswer) &&
			string(p.Data()) == string(teomq.ConsumerAnswer) {
			log.Printf("connected to broker\n")
			return true
		}

		// Process message and Send answer
		go func() {
			var err error
			var answer []byte

			// Process message
			switch {

			// Execute command
			case co.Commands != nil:
				var parts []string
				_, parts, _, err = co.Commands.Unmarshal(p.Data())
				if err == nil {
					answer, err = co.Commands.Exec(parts[0], commands.Teonet, p.Data())
				}

			// Execute custom reader
			case co.ProcessMessage != nil:
				answer, err = co.ProcessMessage(p)

			// Default answer if commands and reader does not added
			default:
				answer = []byte("Answer to " + string(p.Data()))
			}

			// Check error
			if err != nil {
				log.Printf("process message id %d, from %s, error: %s\n", p.ID(), c, err)
				return
			}

			// Don't send empty answer
			if len(answer) == 0 {
				return
			}

			// Send answer
			err = co.sendAnswer(p, answer)
			if err != nil {
				log.Printf("send id %d, len: %d, to %s, error: %s\n", p.ID(), len(answer), c, err)
			} else {
				// log.Printf("send id %d, len: %d, to %s\n", p.ID(), len(answer), c)
			}
		}()

		return true
	}

	return false
}
