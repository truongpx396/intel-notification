package app

import (
	"context"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
)

// Planned is what the planner decided for one notification: the notification with
// its effective priority filled in, which is what the store requires, and the plan.
type Planned struct {
	Notification domain.Notification
	Plan         domain.DeliveryPlan
}

// Planner turns a notification into the plan PersistAndEnqueue writes.
type Planner struct{}

// NewPlanner returns a Planner over d's topics, preferences, channels and quotas.
func NewPlanner(cfg notify.Config, d Deps) (*Planner, error) {
	return &Planner{}, nil
}

// Plan decides, for the notification n of id, which channels it is sent on, when,
// and whether the inbox shows it.
func (p *Planner) Plan(ctx context.Context, id domain.Identity, n domain.Notification) (Planned, error) {
	return Planned{}, errNotImplemented
}
