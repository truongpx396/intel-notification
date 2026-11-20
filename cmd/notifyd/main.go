// Command notifyd is the service: the REST and SSE surface for recipients, the gRPC surface for producers,
// and the dispatcher and the maintenance jobs, in one process or split by role (D31). Everything it needs
// is in its environment (docs/configuration.md), and what it is built from is the packages of this module:
// this file and its neighbours only assemble them, and hold no rule of their own.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	_ "time/tzdata" // zone names are validated against the database, and a minimal image has none
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := LoadConfig(os.Getenv)
	if err != nil {
		log.Error("the configuration is not usable", "error", err.Error())
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	svc, err := Build(ctx, cfg, log)
	if err != nil {
		log.Error("the service could not start", "error", err.Error())
		os.Exit(1)
	}
	log.Info("notifyd is serving", "role", cfg.Role, "http", svc.HTTPAddr(), "grpc", svc.GRPCAddr(), "realms", len(cfg.Bindings.Realms()))
	if err := svc.Run(ctx); err != nil {
		log.Error("the service stopped", "error", err.Error())
		svc.Close()
		os.Exit(1)
	}
	svc.Close()
	log.Info("notifyd stopped")
}
