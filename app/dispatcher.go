package app

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

const (
	// pollFloor is the shortest wait between passes. After work, a wake or a signal
	// the dispatcher looks again this soon, and an idle one doubles it up to
	// Config.PollInterval (D22).
	pollFloor = 10 * time.Millisecond

	// recordTimeout bounds the writes that record an outcome. They are made on a
	// context that outlives the worker's shutdown, so a delivery that was sent is
	// recorded, and this keeps a database that has hung from holding the worker.
	recordTimeout = 30 * time.Second
)

// Dispatcher is the worker: ports.Dispatcher over the queue, the store and the
// channels. It claims deliveries and runs each through the nine steps in
// notification-ports.md (§ The dispatcher, step by step), and it decides nothing the
// domain decides: which ending is a dead letter and whether a fallback follows is
// domain.DispositionOf, how long a retry waits is domain.Backoff, and the store
// executes whatever Finish it is handed (D37).
//
// The ceiling on attempts is enforced where a crash can be seen (D42): a delivery
// claimed more than Config.MaxAttempts times is finished as max_attempts without being
// sent, because a worker that dies on every attempt never reaches the place where a
// failed one is counted.
//
// A claimed batch is handled concurrently, one goroutine per claim, because the lease
// of the whole batch runs from the claim: handled one by one, a batch of 100 slow
// sends outlives ClaimLease although no single send does, and the later deliveries are
// re-driven while the first worker is still going to send them. ClaimLease need only
// exceed one send.
type Dispatcher struct {
	cfg          notify.Config
	channels     ports.ChannelRegistry
	topics       ports.TopicRegistry
	renderer     ports.TemplateRenderer
	addresses    ports.AddressBook
	store        ports.Store
	queue        ports.Queue
	suppressions ports.SuppressionStore
	quota        quotaGate
	metrics      ports.Metrics    // nil: nothing is counted
	steps        ports.StepHook   // nil: the steps are not observed
	listener     ports.WakeSource // nil: polling alone
	clock        ports.Clock

	rnd     func() float64
	onError func(error)
	wake    chan struct{} // capacity one: a wake is a level, not a count
}

var (
	_ ports.Dispatcher = (*Dispatcher)(nil)
	_ ports.Waker      = (*Dispatcher)(nil)
)

// DispatcherOption changes how a Dispatcher is built.
type DispatcherOption func(*Dispatcher)

// WithRand sets the source of randomness, a value in [0, 1): the backoff jitter, and
// the shard a pass starts at. The default is math/rand/v2's.
func WithRand(rnd func() float64) DispatcherOption {
	return func(d *Dispatcher) { d.rnd = rnd }
}

// WithErrorHandler sets where a fault the dispatcher survives is reported: a pass
// that could not claim, a quota counter that could not answer, a listener that
// ended, a panic in a channel. The default discards them. The handler may be called
// from several goroutines.
func WithErrorHandler(h func(error)) DispatcherOption {
	return func(d *Dispatcher) { d.onError = h }
}

// NewDispatcher returns a Dispatcher for cfg over d. It refuses a configuration that
// does not validate, names every port it needs and d leaves nil, and refuses
// Config.ListenNotify with no Deps.Listener: a deployment that turned LISTEN on and
// got polling alone would not know (D22).
func NewDispatcher(cfg notify.Config, d Deps, opts ...DispatcherOption) (*Dispatcher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var g gaps
	g.need("Addresses", d.Addresses == nil)
	g.need("Channels", d.Channels == nil)
	g.need("Queue", d.Queue == nil)
	g.need("Quotas", d.Quotas == nil)
	g.need("Renderer", d.Renderer == nil)
	g.need("Store", d.Store == nil)
	g.need("Suppressions", d.Suppressions == nil)
	g.need("Topics", d.Topics == nil)
	if err := g.err("Dispatcher"); err != nil {
		return nil, err
	}
	cfg = cfg.WithDefaults()
	if cfg.ListenNotify && d.Listener == nil {
		return nil, errors.New("app: Config.ListenNotify is on, and Dispatcher needs Deps.Listener to listen on")
	}
	disp := &Dispatcher{
		cfg: cfg, channels: d.Channels, topics: d.Topics, renderer: d.Renderer, addresses: d.Addresses,
		store: d.Store, queue: d.Queue, suppressions: d.Suppressions, quota: newQuotaGate(d),
		metrics: d.Metrics, steps: d.Steps, listener: d.Listener, clock: d.clockOf(),
		rnd: rand.Float64, onError: func(error) {}, wake: make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(disp)
	}
	return disp, nil
}

// Wake implements ports.Waker. It never blocks: an idle dispatcher claims at once,
// and a busy one looks again as soon as its pass ends. A wake sent while a pass is
// running is kept, because the commit that sent it can land just after a claim came
// back empty.
func (d *Dispatcher) Wake() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// Run implements ports.Dispatcher. It makes a pass over every shard, then waits, and
// returns nil when ctx ends.
//
// The wait is adaptive (D22): none after a pass that filled a batch, since more is
// waiting; the floor after a pass that found some; and, after one that found nothing,
// a wait that doubles from the floor up to Config.PollInterval. A wake, or a signal
// from the listener, ends the wait at once and starts the doubling again. A pass is
// one claim per shard, so a shard with a deep backlog does not hold the others back.
//
// Run is one loop. More throughput is more loops, in this process or another:
// claims are SKIP LOCKED leases, so any number may run side by side (D19).
func (d *Dispatcher) Run(ctx context.Context) error {
	var signals <-chan struct{}
	if d.cfg.ListenNotify {
		ch, err := d.listener.Listen(ctx)
		if err != nil {
			d.report(fmt.Errorf("listen for commits: %w; polling alone", err))
		} else {
			signals = ch
		}
	}

	ceiling := d.cfg.PollInterval
	floor := min(pollFloor, ceiling)
	interval := floor
	for ctx.Err() == nil {
		worked, full := d.pass(ctx)
		var wait time.Duration
		switch {
		case full:
			wait, interval = 0, floor
		case worked:
			wait, interval = floor, min(2*floor, ceiling)
		default:
			wait, interval = interval, min(2*interval, ceiling)
		}
		if wait <= 0 {
			continue
		}

		timer := time.NewTimer(wait)
	waiting:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
				break waiting
			case <-d.wake:
				interval = floor
				timer.Stop()
				break waiting
			case _, open := <-signals:
				if !open {
					signals = nil // a nil channel is never ready: no spinning on a closed one
					d.report(errors.New("the listener for commits has ended; polling alone"))
					continue waiting
				}
				interval = floor
				timer.Stop()
				break waiting
			}
		}
	}
	return nil
}

// pass drains one batch from every shard, starting at a random one so that workers
// started together do not all contend for the same shard first. It reports whether
// any shard had work and whether any filled its batch.
func (d *Dispatcher) pass(ctx context.Context) (worked, full bool) {
	shards := d.cfg.Shards
	start := int(d.rnd()*float64(shards)) % shards
	for i := range shards {
		if ctx.Err() != nil {
			return worked, full
		}
		n, err := d.Drain(ctx, domain.Shard((start+i)%shards), d.cfg.ClaimBatch)
		if err != nil && ctx.Err() == nil {
			d.report(err)
		}
		worked = worked || n > 0
		full = full || n >= d.cfg.ClaimBatch
	}
	return worked, full
}

// Drain implements ports.Dispatcher. It claims up to max deliveries from the shard
// and handles each, concurrently, returning how many it handled to an end. A claim
// whose outcome could not be written is not counted and its error is returned with
// the others: its lease lapses and another worker drives it.
//
// A panic in a handler that is not the code of a channel, a renderer or an address
// book (those are retries) is raised again here once the others have finished, so a
// worker that dies at an injected step dies in the caller's goroutine and not in a
// spawned one.
func (d *Dispatcher) Drain(ctx context.Context, shard domain.Shard, max int) (int, error) {
	if shard < 0 || max < 1 {
		return 0, fmt.Errorf("drain: shard %d and batch %d: a shard is not negative and a batch has at least one delivery", shard, max)
	}
	claims, err := d.queue.Claim(ctx, shard, max, d.cfg.ClaimLease)
	if err != nil {
		return 0, fmt.Errorf("drain shard %d: %w", shard, err)
	}
	if len(claims) == 0 {
		return 0, nil
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		handled  int
		errs     []error
		panicked bool
		panicVal any
		topics   = &topicCache{registry: d.topics}
	)
	for _, c := range claims {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					d.report(fmt.Errorf("a handler panicked: %v\n%s", r, debug.Stack()))
					mu.Lock()
					defer mu.Unlock()
					if !panicked {
						panicked, panicVal = true, r
					}
				}
			}()
			err := d.handle(ctx, topics, c)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			handled++
		})
	}
	wg.Wait()
	if panicked {
		panic(panicVal)
	}
	return handled, errors.Join(errs...)
}

// handle runs one claim through the nine steps. It returns an error only for a write
// that failed, which leaves the claim to lapse; a lost lease, a retry and every
// ending are outcomes, not errors.
func (d *Dispatcher) handle(ctx context.Context, topics *topicCache, c domain.Claim) error {
	e := c.Entry
	d.reach(ctx, domain.StepClaim)

	// 2. Expired? Never sent late (D29). The instant itself is already not before it.
	d.reach(ctx, domain.StepExpiry)
	if e.DeliverBefore != nil && !d.clock.Now().Before(*e.DeliverBefore) {
		return d.finish(ctx, c, domain.OutcomeExpired, "", "the delivery deadline passed before it could be sent")
	}
	// A claim beyond the ceiling is not an attempt: finish it, unsent (D42).
	if e.Attempts > d.cfg.MaxAttempts {
		return d.finish(ctx, c, domain.OutcomeMaxAttempts, "", fmt.Sprintf(
			"claimed %d times without an outcome being recorded and the ceiling is %d: the worker died or lost its lease on every attempt",
			e.Attempts, d.cfg.MaxAttempts))
	}
	ch, registered := d.channels.Get(e.Channel)
	if !registered {
		// A worker without the channel is a deploy in progress, or a registration
		// missing. Waiting keeps the delivery, and the ceiling still parks it with this
		// error if nobody fixes it.
		return d.fail(ctx, c, 0, fmt.Sprintf("channel %q is not registered on this worker", e.Channel))
	}
	caps := ch.Capabilities()

	// 3. Load the notification, or the digest's members, under the claim's scope.
	d.reach(ctx, domain.StepLoad)
	n, members, canceled, err := d.store.Load(ctx, c)
	if err != nil {
		return d.fail(ctx, c, 0, "load: "+err.Error())
	}
	if canceled {
		return d.finish(ctx, c, domain.OutcomeCanceled, "", "")
	}

	// 4. Address.
	d.reach(ctx, domain.StepAddress)
	addr, addressKey, done, err := d.address(ctx, c, ch, caps)
	if done || err != nil {
		return err
	}

	// 5. Quota.
	d.reach(ctx, domain.StepQuota)
	key := domain.QuotaKey{Realm: e.Identity.Realm, Tenant: e.Identity.Tenant, Channel: e.Channel}
	allowed, resetAt, qerr := d.quota.take(ctx, key)
	switch {
	case qerr != nil:
		// Quota fails open: a counter that cannot answer must not hold deliveries back.
		d.report(fmt.Errorf("quota for %s on %s: %w; the delivery goes ahead", e.Identity.Tenant.ID, e.Channel, qerr))
	case !allowed:
		return d.exhausted(ctx, topics, c, key, resetAt)
	}

	// 6. Render, in the address's locale.
	d.reach(ctx, domain.StepRender)
	def, known, err := topics.lookup(ctx, e.Identity.Realm, e.Topic)
	if err != nil {
		return d.fail(ctx, c, 0, "topic: "+err.Error())
	}
	req := domain.RenderRequest{Realm: e.Identity.Realm, Tenant: e.Identity.Tenant, Topic: e.Topic,
		Channel: e.Channel, Locale: addr.Locale, Digest: members}
	if known {
		req.TemplateRef = def.TemplateRef
	}
	if req.Locale == "" {
		req.Locale = d.cfg.DefaultLocale
	}
	if n != nil {
		req.Data = n.Data
	}
	content, err := guarded(d, "the renderer", func() (domain.RenderedContent, error) { return d.renderer.Render(ctx, req) })
	switch {
	case err == nil:
	case errors.Is(err, domain.ErrNoTemplate):
		// The notification's own copy, or nothing to say (D27).
		if n == nil || (n.Title == "" && n.Body == "") {
			return d.finish(ctx, c, domain.OutcomeRejected, "", "no template, and the notification carries no fallback copy: "+err.Error())
		}
		content = domain.RenderedContent{Subject: n.Title, Body: n.Body}
	default:
		return d.fail(ctx, c, 0, "render: "+err.Error())
	}

	// 7. Deliver, with the key that is stable across re-drives and distinct per address (D25).
	delivery := domain.Delivery{
		ID: e.ID, IdemKey: domain.DeliveryIdemKey(e.Identity, idemKeyOf(n), e.DigestID, e.Channel, addressKey),
		Identity: e.Identity, NotificationID: e.NotificationID, DigestID: e.DigestID, Topic: e.Topic,
		Digest: members, Channel: e.Channel, Address: addr, Content: content, Attempt: e.Attempts,
	}
	if n != nil {
		delivery.Notification = *n
	}
	d.reach(ctx, domain.StepDeliver)
	res, derr := guarded(d, "channel "+string(e.Channel), func() (domain.DeliveryResult, error) { return ch.Deliver(ctx, delivery) })

	// 8. Record.
	d.reach(ctx, domain.StepRecord)
	return d.record(ctx, c, caps, ch, addr, res, derr)
}

// address is step 4: where the delivery goes. It reports done when the claim has
// reached an end (finished, fanned out, or fenced) and there is nothing more for this
// worker to do with it.
func (d *Dispatcher) address(ctx context.Context, c domain.Claim, ch ports.Channel, caps domain.ChannelCapabilities) (addr domain.Address, key string, done bool, err error) {
	e := c.Entry
	if !caps.NeedsAddress {
		// Addressed by the recipient's identity alone, like the in-app nudge. There is
		// nothing to resolve, and nothing to suppress: a suppression is an address's.
		return domain.Address{Channel: e.Channel}, e.AddressKey, false, nil
	}
	normalize := normalizerOf(ch)

	if e.Address != nil {
		// Bound at an earlier claim, or one of a fan-out. The suppression is checked
		// again, because a bounce can land while a delivery waits for its retry.
		dead, serr := d.suppressed(ctx, e.Identity.Realm, e.Channel, normalize(e.Address.Value))
		if serr != nil {
			return addr, "", true, d.fail(ctx, c, 0, "suppression check: "+serr.Error())
		}
		if dead {
			return addr, "", true, d.finish(ctx, c, domain.OutcomeSuppressed, "", "the address is suppressed")
		}
		return *e.Address, e.AddressKey, false, nil
	}

	resolved, rerr := guarded(d, "the address book", func() ([]domain.Address, error) {
		return d.addresses.Resolve(ctx, e.Identity, e.Channel)
	})
	if rerr != nil {
		return addr, "", true, d.fail(ctx, c, 0, "resolve addresses: "+rerr.Error())
	}
	var (
		live []domain.Address
		keys []string
		seen = map[string]bool{}
	)
	for _, a := range resolved {
		a.Channel = e.Channel
		k := domain.AddressKey(e.Channel, normalize(a.Value))
		if seen[k] {
			continue // two spellings of one address are one address
		}
		seen[k] = true
		dead, serr := d.suppressed(ctx, e.Identity.Realm, e.Channel, normalize(a.Value))
		if serr != nil {
			return addr, "", true, d.fail(ctx, c, 0, "suppression check: "+serr.Error())
		}
		if !dead {
			live, keys = append(live, a), append(keys, k)
		}
	}
	switch {
	case len(seen) == 0:
		return addr, "", true, d.finish(ctx, c, domain.OutcomeNoAddress, "", "the recipient has no address on this channel")
	case len(live) == 0:
		return addr, "", true, d.finish(ctx, c, domain.OutcomeSuppressed, "", "every address of the recipient on this channel is suppressed")
	}

	rctx, cancel := d.recordCtx(ctx)
	defer cancel()
	binding, ok, berr := d.queue.BindAddresses(rctx, c, live, keys)
	if berr != nil {
		return addr, "", true, fmt.Errorf("bind %s: %w", e.ID, berr)
	}
	if !d.fenced(ctx, c, ok) {
		return addr, "", true, nil
	}
	if !binding.StillHeld {
		// Fanned out: the claim is spent, and each address is claimed on its own (D25).
		return addr, "", true, nil
	}
	return live[0], keys[0], false, nil
}

// exhausted is step 5 when the budget is spent. A policy that drops, and a topic that
// may be dropped, end the delivery as dropped_quota; otherwise the row is deferred to
// the budget's reset and the tenant's whole due backlog on this channel goes with it,
// in one statement, so the tenant stops occupying the head of every shard (D24).
func (d *Dispatcher) exhausted(ctx context.Context, topics *topicCache, c domain.Claim, key domain.QuotaKey, resetAt time.Time) error {
	e := c.Entry
	// A topic that cannot be read is not known to be droppable, and dropping discards.
	if def, known, err := topics.lookup(ctx, e.Identity.Realm, e.Topic); err == nil && known && d.quota.drops(ctx, key, def, e.Priority) {
		return d.finish(ctx, c, domain.OutcomeDroppedQuota, "", "the channel's budget is spent and its policy drops non-essential notifications")
	}

	// A reset that is already past would defer to a moment that has gone and be claimed
	// again at once, in a loop: wait at least one poll.
	until := resetAt
	if soonest := d.clock.Now().Add(d.cfg.PollInterval); until.Before(soonest) {
		until = soonest
	}
	rctx, cancel := d.recordCtx(ctx)
	defer cancel()
	ok, err := d.queue.Defer(rctx, c, until, "quota")
	if err != nil {
		return fmt.Errorf("defer %s: %w", e.ID, err)
	}
	if !d.fenced(ctx, c, ok) {
		return nil
	}
	if _, err := d.queue.DeferTenantChannel(rctx, e.Identity.Realm, e.Identity.Tenant, e.Channel, until); err != nil {
		return fmt.Errorf("defer the backlog of %s on %s: %w", e.Identity.Tenant.ID, e.Channel, err)
	}
	return nil
}

// record is step 8: what a channel's answer means for the delivery.
func (d *Dispatcher) record(ctx context.Context, c domain.Claim, caps domain.ChannelCapabilities, ch ports.Channel,
	addr domain.Address, res domain.DeliveryResult, derr error) error {

	e := c.Entry
	if derr != nil {
		return d.fail(ctx, c, 0, derr.Error())
	}
	switch res.Outcome {
	case domain.Delivered:
		return d.finish(ctx, c, domain.OutcomeDelivered, res.ProviderMessageID, res.Detail)
	case domain.Retry:
		detail := res.Detail
		if detail == "" {
			detail = "the channel asked for a retry"
		}
		return d.fail(ctx, c, res.RetryAfter, detail)
	case domain.Suppressed:
		// The suppression is written before the delivery is finished: a crash between
		// the two leaves a delivery that the next claim finds suppressed, where the other
		// order loses the suppression and mails a dead address again.
		if caps.NeedsAddress {
			if err := d.suppress(ctx, e, ch, addr, res); err != nil {
				return d.fail(ctx, c, 0, "record the suppression: "+err.Error())
			}
		}
		return d.finish(ctx, c, domain.OutcomeSuppressed, "", res.Detail)
	case domain.Rejected:
		detail := res.Detail
		if detail == "" {
			detail = "the channel refused the message"
		}
		return d.finish(ctx, c, domain.OutcomeRejected, "", detail)
	default:
		return d.fail(ctx, c, 0, fmt.Sprintf("the channel returned the unknown outcome %d", int(res.Outcome)))
	}
}

// suppress records a channel's dead address, expiring after SuppressFor and never when
// that is zero (D14). A reason that is not one of the six becomes invalid_address,
// which is the least a channel reporting a dead address can have meant.
func (d *Dispatcher) suppress(ctx context.Context, e domain.OutboxEntry, ch ports.Channel, addr domain.Address, res domain.DeliveryResult) error {
	reason := res.SuppressReason
	if !reason.Valid() {
		reason = domain.SuppressInvalidAddress
	}
	var until time.Time
	if res.SuppressFor > 0 {
		until = d.clock.Now().Add(res.SuppressFor)
	}
	rctx, cancel := d.recordCtx(ctx)
	defer cancel()
	return d.suppressions.Suppress(rctx, e.Identity.Realm, e.Channel,
		domain.SuppressionHash(e.Channel, normalizerOf(ch)(addr.Value)), reason, until)
}

func (d *Dispatcher) suppressed(ctx context.Context, realm domain.Realm, ch domain.ChannelKind, normalized string) (bool, error) {
	return d.suppressions.Suppressed(ctx, realm, ch, domain.SuppressionHash(ch, normalized))
}

// finish ends a delivery with outcome. The finish carries the domain's disposition,
// which says whether it is a dead letter and whether the fallback chain goes on. A lost
// lease records nothing and is counted; a dead letter that was written raises the alarm.
func (d *Dispatcher) finish(ctx context.Context, c domain.Claim, outcome domain.TerminalOutcome, providerMessageID, detail string) error {
	f, err := domain.NewFinish(outcome, providerMessageID, detail)
	if err != nil {
		return err
	}
	rctx, cancel := d.recordCtx(ctx)
	defer cancel()
	ok, err := d.queue.Finish(rctx, c, f)
	if err != nil {
		return fmt.Errorf("finish %s as %s: %w", c.Entry.ID, outcome, err)
	}
	if !d.fenced(ctx, c, ok) {
		return nil
	}
	if f.Disposition.DeadLetter {
		d.count(domain.MetricDeadLetter, map[string]string{
			domain.LabelChannel: string(c.Entry.Channel), domain.LabelReason: string(outcome)})
	}
	return nil
}

// fail records a transient failure (D10). At the ceiling the delivery is parked as
// max_attempts with this error; below it, it waits out a full-jitter backoff, no
// shorter than the provider's own hint.
func (d *Dispatcher) fail(ctx context.Context, c domain.Claim, retryAfter time.Duration, detail string) error {
	if c.Entry.Attempts >= d.cfg.MaxAttempts {
		return d.finish(ctx, c, domain.OutcomeMaxAttempts, "", detail)
	}
	delay := domain.Backoff(c.Entry.Attempts, d.cfg.BackoffBase, d.cfg.BackoffCeiling, retryAfter, d.rnd)
	rctx, cancel := d.recordCtx(ctx)
	defer cancel()
	ok, err := d.queue.Retry(rctx, c, d.clock.Now().Add(delay), detail)
	if err != nil {
		return fmt.Errorf("retry %s: %w", c.Entry.ID, err)
	}
	d.fenced(ctx, c, ok)
	return nil
}

// fenced is step 9. A write that reports ok=false was made by a worker that no longer
// holds the lease: nothing was recorded, the row's current holder owns it, and the loss
// is counted. It reports whether the write took.
func (d *Dispatcher) fenced(ctx context.Context, c domain.Claim, ok bool) bool {
	d.reach(ctx, domain.StepFence)
	if !ok {
		d.count(domain.MetricLeaseLost, map[string]string{domain.LabelChannel: string(c.Entry.Channel)})
	}
	return ok
}

// recordCtx is the context an outcome is written on. It is not cancelled with the
// worker: a delivery that was sent must be recorded even if shutdown began while it
// was in flight, or the next worker sends it again on a channel that cannot tell.
func (d *Dispatcher) recordCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
}

func (d *Dispatcher) reach(ctx context.Context, step domain.DispatchStep) {
	if d.steps != nil {
		d.steps.Reach(ctx, step)
	}
}

func (d *Dispatcher) count(name string, labels map[string]string) {
	if d.metrics != nil {
		d.metrics.Inc(name, labels)
	}
}

func (d *Dispatcher) report(err error) { d.onError(err) }

// guarded calls code the engine does not own (a channel, a renderer, an address
// book) and turns its panic into an error, so that one delivery that breaks it is a
// retry that reaches the ceiling and not a worker that dies with a batch in hand. The
// stack goes to the error handler.
func guarded[T any](d *Dispatcher, what string, call func() (T, error)) (v T, err error) {
	defer func() {
		if r := recover(); r != nil {
			d.report(fmt.Errorf("%s panicked: %v\n%s", what, r, debug.Stack()))
			err = fmt.Errorf("%s panicked: %v", what, r)
		}
	}()
	return call()
}

// normalizerOf is how a channel defines address equality: its own, or the trimmed
// value (ports.AddressNormalizer).
func normalizerOf(ch ports.Channel) func(string) string {
	if n, ok := ch.(ports.AddressNormalizer); ok {
		return n.NormalizeAddress
	}
	return strings.TrimSpace
}

func idemKeyOf(n *domain.Notification) string {
	if n == nil {
		return ""
	}
	return n.IdemKey
}

// topicCache reads each topic once per batch: a batch is one realm's recent work and
// usually a few topics, and in service mode a lookup is a query.
type topicCache struct {
	registry ports.TopicRegistry

	mu sync.Mutex
	m  map[topicKey]topicEntry
}

type (
	topicKey struct {
		realm domain.Realm
		topic domain.Topic
	}
	topicEntry struct {
		def   domain.TopicDef
		known bool
		err   error
	}
)

func (t *topicCache) lookup(ctx context.Context, realm domain.Realm, topic domain.Topic) (domain.TopicDef, bool, error) {
	k := topicKey{realm, topic}
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.m[k]; ok {
		return e.def, e.known, e.err
	}
	def, known, err := t.registry.Lookup(ctx, realm, topic)
	if t.m == nil {
		t.m = map[topicKey]topicEntry{}
	}
	t.m[k] = topicEntry{def, known, err}
	return def, known, err
}
