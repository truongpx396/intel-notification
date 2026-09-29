// Package migrations embeds the schema so it travels with the module: a host,
// the service binary and the integration tests all apply the same files.
package migrations

import "embed"

// FS holds every migration, applied in filename order.
//
//go:embed *.sql
var FS embed.FS
