// Copyright 2024 Kirill Scherba <kirill@scherba.ru>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Commands module of Teonet games control server system.

package commands

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
)

var ErrWrongInputData = fmt.Errorf("wrong input data")
var ErrWrongProcessIn = fmt.Errorf("wrong process in")

// Commands contains map of commands and mutex for access synchronization.
type Commands struct {
	m   map[string]*CommandData
	mut *sync.RWMutex
}
type CommandData struct {
	Cmd       string
	ProcessIn ProcessIn
	Params    string
	Descr     string
	Handler   CommandHandler
}
type CommandHandler func(cmd *CommandData, processIn ProcessIn, data any) ([]byte, error)

const (
	HTTP   ProcessIn = 1 << iota // HTTP request
	TRU                          // TRU request
	WebRTC                       // WebRTC request
	Teonet                       // Teonet request
)

// ProcessIn defines where command can be used.
type ProcessIn byte

// String returns string representation of ProcessIn.
func (p ProcessIn) String() string {

	var s string
	if p&HTTP != 0 {
		s += "HTTP, "
	}
	if p&TRU != 0 {
		s += "TRU, "
	}
	if p&WebRTC != 0 {
		s += "WebRTC, "
	}
	if p&Teonet != 0 {
		s += "Teonet, "
	}
	s = strings.Trim(s, ", ")
	s = strings.ToLower(s)
	return s
}

// Init initialize Commands object and add default commands.
func (c *Commands) Init() {
	c.m = make(map[string]*CommandData)
	c.mut = new(sync.RWMutex)
}

// Add adds command to commands map.
func (c *Commands) Add(command, descr string, processIn ProcessIn, params string,
	handlers ...CommandHandler) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if len(handlers) == 0 {
		handlers = []CommandHandler{c.ProcessBrokerCommand}
	}

	c.m[command] = &CommandData{command, processIn, params, descr, handlers[0]}
}

// get returns command from commands map.
func (c *Commands) get(name string) (cmd *CommandData, ok bool) {
	c.mut.RLock()
	cmd, ok = c.m[name]
	c.mut.RUnlock()
	return
}

// Del removes command from commands map.
func (c *Commands) Del(name string) {
	c.mut.Lock()
	delete(c.m, name)
	c.mut.Unlock()
}

// Exec executes command from commands map.
func (c *Commands) Exec(command string, processIn ProcessIn, data any) ([]byte, error) {
	cmd, ok := c.get(command)
	if ok && cmd.Handler != nil {
		return cmd.Handler(cmd, processIn, data)
	}
	return nil, fmt.Errorf("command '%s' not found", command)
}

// ForEach call f fuction for each added command.
func (c *Commands) ForEach(f func(command string, cmd *CommandData)) {
	c.mut.RLock()
	for command, cmd := range c.m {
		f(command, cmd)
	}
	c.mut.RUnlock()
}

// HabdleCommands adds handlers of added commands.
func (c *Commands) HabdleCommands(processIn ProcessIn,
	h func(command, params string, f CommandHandler)) {

	c.ForEach(func(command string, cmd *CommandData) {
		if cmd.ProcessIn&processIn != 0 && cmd.Handler != nil {
			h(command, cmd.Params, cmd.Handler)
		}
	})
}

// Print prints commands.
func (c *Commands) Print(msgs ...string) {
	c.ForEach(func(command string, cmd *CommandData) {
		fmt.Printf("---\n"+
			"%s\n"+
			"%s\n"+
			"parameters: %s\n"+
			"processing in: %s\n"+
			"\n",
			command, cmd.Descr, cmd.Params, cmd.ProcessIn)
	})
}

// HttpRequest contains gorilla mux variables and HTTP request.
type HttpRequest struct {
	Vars map[string]string
	*http.Request
}

// ParseHttpParams parses HTTP input command parameters.
func (*Commands) ParseHttpParams(command *CommandData, indata any) (*HttpRequest,
	error) {

	request, ok := indata.(*HttpRequest)
	if !ok {
		err := ErrWrongInputData
		log.Printf("%s parse error: %s", command.Cmd, err)
		return nil, err
	}

	return request, nil
}

func (*Commands) ProcessBrokerCommand(cmd *CommandData, processIn ProcessIn, data any) ([]byte, error) {

	// Parse commands parameters and send to consumer (or save to queue)

	return nil, nil
}

// Unmarshal unmarshals a string into a CommandData object and returns it along with the command's
// parts and variables.
//
// The input string is expected to be in the format "commandName/param1/param2/...". The function
// splits the string by "/" and retrieves the command name. It then looks up the corresponding
// CommandData object from the Commands map. If the command is found, the function creates a map
// of variables by iterating over the command's parameters and assigning the corresponding values
// from the input string. If the number of parameters is exceeded, the remaining values are ignored.
//
// The function returns the CommandData object, the command's parts, and the map of variables.
// If the command is not found in the Commands map, an error is returned.
func (c *Commands) Unmarshal(data []byte) (cmd *CommandData, parts []string,
	vars map[string]string, err error) {

	// Split data string by /
	parts = strings.Split(string(data), "/")

	// Get command name
	command := parts[0]

	// Look up the command in the Commands map
	cmd, ok := c.get(command)
	if !ok {
		// Return an error if the command is not found
		return nil, parts, nil, fmt.Errorf("command '%s' not found", command)
	}

	// Create a map of variables
	vars = make(map[string]string)
	for i, param := range strings.Split(cmd.Params, "/") {
		if len(param) == 0 {
			// If a parameter is empty, stop iterating
			break
		}

		// Trim the parameter name of any leading or trailing curly braces
		param = strings.Trim(param, "{}")

		var v string
		if len(parts) > i+1 {
			// If there is a value for the parameter, assign it
			v = parts[i+1]
		}

		// Assign the parameter name and value to the variables map
		vars[param] = v
	}

	// Return the CommandData object, the command's parts, and the map of variables
	return cmd, parts, vars, nil
}
