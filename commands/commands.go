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

// ForEach call f for each added command.
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

// ParseParams parses HTTP input command parameters.
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

// Unmarshal unmarshals string with commands name and parameters.
func (c *Commands) Unmarshal(data []byte) (cmd *CommandData, parts []string, vars map[string]string, err error) {

	// Split data string by /
	parts = strings.Split(string(data), "/")

	// Get command name
	command := parts[0]
	cmd, ok := c.get(command)
	if !ok {
		return nil, parts, nil, fmt.Errorf("command '%s' not found", command)
	}

	// Make map of variables
	vars = make(map[string]string)
	for i, param := range strings.Split(cmd.Params, "/") {
		if len(param) == 0 {
			break
		}
		param = strings.Trim(param, "{}")
		var v string
		if len(parts) > i+1 {
			v = parts[i+1]
		}
		vars[param] = v
	}

	return cmd, parts, vars, nil
}
