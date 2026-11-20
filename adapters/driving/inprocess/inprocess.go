// Package inprocess is the library-mode transport: no wire. A Go host that embeds the engine holds the
// engine's ports directly and calls them in its own process, so a producer's code is the same line
// whether it embeds the engine or calls the service:
//
//	var n ports.Notifier = engine.Notifier           // library
//	var n ports.Notifier = grpcclient.New(conn)      // service
//
// The package holds the shape and nothing else. A transport that did anything here, a copy, a retry,
// a translation of an error, would make the library and the service differ, and NR-025 says they do
// not: NotifierContract runs against the engine's own Notifier and, with the one case that cannot
// cross a wire swapped (notifytest.Remote), against the gRPC client.
package inprocess

import "github.com/truongpx396/intel-notification/ports"

// Engine is the engine's driving surface as an embedded host holds it. A host fills the ports it
// runs: a producer-only process needs the Notifier, a worker needs the Dispatcher and Jobs, and a
// process that serves recipients needs the Inbox and Admin. A port it does not run stays nil.
type Engine struct {
	Notifier   ports.Notifier
	Inbox      ports.Inbox
	Admin      ports.Admin
	Dispatcher ports.Dispatcher
	Jobs       ports.Jobs
}
