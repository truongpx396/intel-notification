package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	notify "github.com/truongpx396/intel-notification"
	"github.com/truongpx396/intel-notification/adapters/driven/channel/email"
	"github.com/truongpx396/intel-notification/adapters/driven/channel/inapp"
	"github.com/truongpx396/intel-notification/adapters/driven/channel/providers"
	"github.com/truongpx396/intel-notification/adapters/driven/channel/webhook"
	"github.com/truongpx396/intel-notification/adapters/driven/postgres"
	"github.com/truongpx396/intel-notification/adapters/driven/redis"
	"github.com/truongpx396/intel-notification/adapters/driven/secrets"
	"github.com/truongpx396/intel-notification/adapters/driving/auth"
	"github.com/truongpx396/intel-notification/adapters/driving/grpcclient"
	"github.com/truongpx396/intel-notification/adapters/driving/grpcserver"
	"github.com/truongpx396/intel-notification/adapters/driving/httpapi"
	"github.com/truongpx396/intel-notification/adapters/driving/natsingest"
	"github.com/truongpx396/intel-notification/app"
	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// Service is the assembled process: its listeners, and the loops it runs.
type Service struct {
	cfg Config
	log *slog.Logger

	pool *pgxpool.Pool
	rdb  *goredis.Client

	httpSrv  *http.Server
	httpLn   net.Listener
	grpcSrv  *grpc.Server
	grpcLn   net.Listener
	loops    []namedLoop
	closers  []func()
	closeOne sync.Once
}

type namedLoop struct {
	name string
	run  func(context.Context) error
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// HTTPAddr is where the REST surface listens, once Build has returned; empty for a role with none.
func (s *Service) HTTPAddr() string {
	if s.httpLn == nil {
		return ""
	}
	return s.httpLn.Addr().String()
}

// GRPCAddr is where the producer surface listens; empty for a role with none.
func (s *Service) GRPCAddr() string {
	if s.grpcLn == nil {
		return ""
	}
	return s.grpcLn.Addr().String()
}

// Build assembles the service from cfg: it connects, checks what must hold before anything is served, and
// returns a Service that has bound its listeners and started nothing. It refuses to run as a database role
// that bypasses row-level security, because tenant isolation is held by the schema and not by a call site,
// and a role that skips the schema's policies holds none of it (constitution VII, D17).
func Build(ctx context.Context, cfg Config, log *slog.Logger) (*Service, error) {
	s := &Service{cfg: cfg, log: log}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()

	// -- stores
	pool, err := pgxpool.New(ctx, cfg.Engine.StoreDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	s.pool = pool
	s.closers = append(s.closers, pool.Close)
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("no answer from PostgreSQL: %w", err)
	}
	if err := s.checkRole(ctx); err != nil {
		return nil, err
	}
	redisOpt, err := goredis.ParseURL(cfg.Engine.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("NOTIFY_REDIS_URL is not a Redis URL: %w", err)
	}
	s.rdb = goredis.NewClient(redisOpt)
	s.closers = append(s.closers, func() { _ = s.rdb.Close() })
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("no answer from Redis: %w", err)
	}

	engine := cfg.Engine.WithDefaults()
	pgOpts := []postgres.Option{postgres.WithDefaultLocale(engine.DefaultLocale)}
	if engine.ListenNotify {
		pgOpts = append(pgOpts, postgres.WithNotify())
	}
	pg := postgres.NewAll(pool, pgOpts...)
	var clock ports.Clock = systemClock{}
	fault := func(what string) func(error) {
		return func(err error) { log.Error(what, "error", err.Error()) }
	}
	stream := redis.NewStream(s.rdb)
	precheck := redis.NewPreCheck(s.rdb, redis.WithMaxTTL(engine.PreCheckTTL),
		redis.WithErrorHandler(func(op string, err error) { log.Warn("pre-check", "op", op, "error", err.Error()) }))
	quota := redis.NewQuota(s.rdb, pg.Quotas,
		redis.WithQuotaErrorHandler(func(op string, err error) { log.Warn("quota", "op", op, "error", err.Error()) }))

	// -- credentials and the channels built from them
	resolver := secrets.New()
	links, signer, err := s.unsubscribe(ctx, resolver, clock)
	if err != nil {
		return nil, err
	}
	mgr := providers.NewManager(pg.Providers, resolver, providerFactories(pg.Topics, links))
	mgr.Register(inapp.New(stream))
	if err := mgr.Reload(ctx); err != nil {
		return nil, err
	}
	for _, st := range mgr.Report() {
		if st.Available {
			log.Info("provider ready", "realm", st.Realm, "channel", st.Channel, "provider", st.Provider)
		} else {
			// The reference and the reason; never the credential.
			log.Warn("provider unavailable", "realm", st.Realm, "channel", st.Channel, "provider", st.Provider, "secret_ref", st.SecretRef, "reason", st.Reason)
		}
	}
	channels := mgr.Registry()
	s.loops = append(s.loops, namedLoop{"provider refresh", func(ctx context.Context) error {
		ticker := time.NewTicker(cfg.ProviderRefresh)
		defer ticker.Stop()
		mgr.Run(ctx, ticker.C, fault("reload the providers"))
		return nil
	}})

	// -- the host's directories, one per realm that has one
	var audience ports.AudienceResolver
	if len(cfg.DirectoryAddrs) > 0 {
		conns := map[domain.Realm]grpc.ClientConnInterface{}
		for realm, addr := range cfg.DirectoryAddrs {
			creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
			if !cfg.DirectoryTLS {
				creds = insecure.NewCredentials()
			}
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
			if err != nil {
				return nil, fmt.Errorf("the Directory of realm %q at %s: %w", realm, addr, err)
			}
			s.closers = append(s.closers, func() { _ = conn.Close() })
			conns[realm] = conn
		}
		audience = grpcclient.NewDirectory(conns)
	}

	// -- the workers
	var waker ports.Waker
	if cfg.Role != "api" {
		dispatcher, err := app.NewDispatcher(engine, app.Deps{
			Channels: channels, Topics: pg.Topics, Renderer: pg.Templates, Addresses: pg.Addresses,
			Store: pg.Store, Queue: pg.Queue, Suppressions: pg.Suppressions, Quotas: quota, QuotaPolicies: pg.Quotas,
			Clock: clock, Listener: s.listener(engine, pool),
		}, app.WithErrorHandler(fault("dispatcher")))
		if err != nil {
			return nil, fmt.Errorf("build the dispatcher: %w", err)
		}
		waker = dispatcher
		maintenance, err := app.NewMaintenance(engine, app.Deps{Maintenance: pg.Maintenance, Clock: clock},
			app.WithMaintenanceErrorHandler(fault("maintenance")),
			app.WithPreflightWarningHandler(func(w domain.PreflightWarning) { log.Warn("database preflight", "code", w.Code, "message", w.Message) }))
		if err != nil {
			return nil, fmt.Errorf("build the maintenance jobs: %w", err)
		}
		flusher, err := app.NewDigestFlusher(engine, app.Deps{Digests: pg.Digests, Maintenance: pg.Maintenance, Clock: clock}, app.WithFlushErrorHandler(fault("digest flush")))
		if err != nil {
			return nil, fmt.Errorf("build the digest flusher: %w", err)
		}
		expander, err := app.NewBroadcastExpander(engine, app.Deps{
			Channels: channels, Topics: pg.Topics, Prefs: pg.Prefs, Quotas: quota, QuotaPolicies: pg.Quotas,
			Audience: audience, Store: pg.Store, Waker: waker, Clock: clock,
		}, app.WithBroadcastErrorHandler(fault("broadcast expansion")))
		if err != nil {
			return nil, fmt.Errorf("build the broadcast expander: %w", err)
		}
		s.loops = append(s.loops,
			namedLoop{"dispatcher", dispatcher.Run}, namedLoop{"maintenance", maintenance.Run},
			namedLoop{"digest flush", flusher.Run}, namedLoop{"broadcast expansion", expander.Run})
	}

	// -- the surfaces
	if cfg.Role != "worker" {
		if err := s.surfaces(ctx, pg, channels, stream, precheck, quota, audience, signer, clock, waker); err != nil {
			return nil, err
		}
	}
	ok = true
	return s, nil
}

// checkRole refuses a database role that skips row-level security.
func (s *Service) checkRole(ctx context.Context) error {
	var name string
	var privileged bool
	err := s.pool.QueryRow(ctx, `SELECT current_user::text, rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user`).Scan(&name, &privileged)
	if err != nil {
		return fmt.Errorf("read the database role: %w", err)
	}
	switch {
	case !privileged:
		return nil
	case s.cfg.AllowPrivilegedRole:
		s.log.Warn("the database role bypasses row-level security: tenant isolation is NOT held", "role", name)
		return nil
	}
	return fmt.Errorf("the database role %q is a superuser or bypasses row-level security, so the schema's tenant isolation would not hold: connect as the table owner (a role with neither), or set NOTIFY_ALLOW_PRIVILEGED_DB_ROLE for a throwaway development database", name)
}

// listener is the LISTEN end of the dispatcher's wakeups, when the configuration asks for one.
func (s *Service) listener(engine notify.Config, pool *pgxpool.Pool) ports.WakeSource {
	if !engine.ListenNotify {
		return nil
	}
	return postgres.NewListener(pool)
}

// unsubscribe builds what signs and carries the one-click unsubscribe links (D34): the signer, from the keys
// the configuration references, and the links the email channel puts in a message. Without keys there is no
// signer, no link, and no email provider can be built: sending without the unsubscribe headers is the
// compliance failure, so the provider is reported unavailable and says why.
func (s *Service) unsubscribe(ctx context.Context, resolver ports.SecretResolver, clock ports.Clock) (email.UnsubscribeLinks, *domain.UnsubscribeSigner, error) {
	if len(s.cfg.UnsubscribeKeys) == 0 {
		s.log.Warn("NOTIFY_UNSUBSCRIBE_KEYS is not set: no unsubscribe link can be made, so email providers are unavailable")
		return nil, nil, nil
	}
	keys := map[string][]byte{}
	for _, k := range s.cfg.UnsubscribeKeys {
		secret, err := resolver.Resolve(ctx, k.Ref)
		if err != nil {
			return nil, nil, fmt.Errorf("the unsubscribe key %q: %w", k.ID, err)
		}
		keys[k.ID] = []byte(secret)
	}
	signer, err := domain.NewUnsubscribeSigner(s.cfg.UnsubscribeKeys[0].ID, keys, domain.MinUnsubscribeTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("the unsubscribe signer: %w", err)
	}
	if s.cfg.PublicURL == "" {
		return nil, signer, errors.New("NOTIFY_PUBLIC_URL is required with NOTIFY_UNSUBSCRIBE_KEYS: an unsubscribe link must point at the REST surface as it is reached from outside")
	}
	var opts []email.LinksOption
	if s.cfg.AllowInsecureHTTP {
		opts = append(opts, email.AllowInsecureHTTP())
	}
	links, err := email.NewSignedLinks(signer, s.cfg.PublicURL, clock, opts...)
	if err != nil {
		return nil, nil, err
	}
	return links, signer, nil
}

// providerFactories is what each provider name in channel_providers builds. A new provider is a factory
// here and a row there, and nothing in fan-out, persistence or preferences knows its name (NR-016).
func providerFactories(topics ports.TopicRegistry, links email.UnsubscribeLinks) map[string]providers.Factory {
	needLinks := func() error {
		if links == nil {
			return errors.New("no unsubscribe link can be made: set NOTIFY_UNSUBSCRIBE_KEYS and NOTIFY_PUBLIC_URL")
		}
		return nil
	}
	str := func(m map[string]any, key string) string { v, _ := m[key].(string); return v }
	flag := func(m map[string]any, key string) bool { v, _ := m[key].(bool); return v }
	return map[string]providers.Factory{
		"smtp": func(sp providers.Spec) (ports.Channel, error) {
			if err := needLinks(); err != nil {
				return nil, err
			}
			opts := []email.SMTPOption{}
			if flag(sp.Config, "require_tls") {
				opts = append(opts, email.WithSMTPRequireTLS())
			}
			mailer, err := email.NewSMTP(str(sp.Config, "addr"), str(sp.Config, "from"), opts...)
			if err != nil {
				return nil, err
			}
			return email.New(mailer, topics, links), nil
		},
		"resend": func(sp providers.Spec) (ports.Channel, error) {
			if err := needLinks(); err != nil {
				return nil, err
			}
			var opts []email.ResendOption
			if base := str(sp.Config, "base_url"); base != "" {
				opts = append(opts, email.WithResendBaseURL(base))
			}
			mailer, err := email.NewResend(sp.Secret, str(sp.Config, "from"), opts...)
			if err != nil {
				return nil, err
			}
			return email.New(mailer, topics, links), nil
		},
		"webhook": func(sp providers.Spec) (ports.Channel, error) {
			return webhook.New(sp.Secret)
		},
	}
}

// surfaces builds the REST and gRPC servers and binds their listeners.
func (s *Service) surfaces(ctx context.Context, pg *postgres.All, channels ports.ChannelRegistry, stream *redis.Stream,
	precheck ports.PreCheck, quota ports.QuotaCounter, audience ports.AudienceResolver, signer *domain.UnsubscribeSigner, clock ports.Clock, waker ports.Waker) error {
	cfg, log := s.cfg, s.log
	engine := cfg.Engine.WithDefaults()
	fault := func(what string) func(error) {
		return func(err error) { log.Error(what, "error", err.Error()) }
	}

	// One Notifier per realm a producer is bound to: its realm is its configuration (NR-009), and the call
	// picks the engine by the realm its caller is bound to.
	notifiers := map[domain.Realm]ports.Notifier{}
	for _, realm := range cfg.Bindings.Realms() {
		rcfg := engine
		rcfg.Realm = realm
		n, err := app.NewNotifier(rcfg, app.Deps{
			Channels: channels, Topics: pg.Topics, Prefs: pg.Prefs, Quotas: quota, QuotaPolicies: pg.Quotas,
			Audience: audience, Store: pg.Store, Queue: pg.Queue, PreCheck: precheck, Waker: waker, Clock: clock,
		})
		if err != nil {
			return fmt.Errorf("build the Notifier of realm %q: %w", realm, err)
		}
		notifiers[realm] = n
	}
	route := auth.Route(notifiers)
	if cfg.NATSURL != "" {
		if err := s.ingest(ctx, notifiers, pg.Poison); err != nil {
			return err
		}
	}

	inbox, err := app.NewInbox(engine, app.Deps{
		Channels: channels, Inbox: pg.Inbox, Prefs: pg.Prefs, Renderer: pg.Templates, Topics: pg.Topics,
		UnsubscribeSigner: signer, Clock: clock,
	})
	if err != nil {
		return fmt.Errorf("build the Inbox: %w", err)
	}
	admin, err := app.NewAdmin(engine, app.Deps{Channels: channels, Maintenance: pg.Maintenance, Prefs: pg.Prefs, Topics: pg.Topics, Waker: waker, PreCheck: precheck})
	if err != nil {
		return fmt.Errorf("build Admin: %w", err)
	}
	catalog, err := app.NewCatalog(app.Deps{Channels: channels, TopicWriter: pg.Topics, TemplateWriter: pg.Templates, AddressWriter: pg.Addresses})
	if err != nil {
		return fmt.Errorf("build the Catalog: %w", err)
	}
	// The transports are handed ports and not the use-cases behind them: the transport never sees an app type.
	var (
		catalogPort ports.Catalog = catalog
		adminPort   ports.Admin   = admin
	)

	// -- who may call
	keys, err := auth.NewKeys(cfg.RecipientJWKS, s.keyOptions()...)
	if err != nil {
		return fmt.Errorf("NOTIFY_RECIPIENT_JWKS: %w", err)
	}
	var recipientOpts []auth.RecipientOption
	if cfg.RecipientAud != "" {
		recipientOpts = append(recipientOpts, auth.WithAudience(cfg.RecipientAud))
	}
	recipientOpts = append(recipientOpts, auth.WithRefusalHandler(func(err error) { log.Info("recipient token refused", "reason", err.Error()) }))
	recipients := auth.NewRecipients(keys, recipientOpts...)

	resolver := secrets.New()
	tokens := map[string]domain.Realm{}
	for _, ot := range cfg.OperatorTokens {
		token, err := resolver.Resolve(ctx, ot.Ref)
		if err != nil {
			return fmt.Errorf("the operator token of realm %q: %w", ot.Realm, err)
		}
		tokens[token] = ot.Realm
	}
	operators, err := auth.NewOperators(tokens)
	if err != nil {
		return fmt.Errorf("NOTIFY_OPERATOR_TOKENS: %w", err)
	}

	// -- REST
	httpDeps := httpapi.Deps{
		Inbox: inbox, Notifier: &auth.RoutedNotifier{Route: route}, Admin: admin, Catalog: catalogPort, Stream: stream,
		Identify: recipients.Identify,
		Ready: []httpapi.Check{
			{Name: "postgres", Run: s.pool.Ping},
			{Name: "redis", Run: func(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }},
			httpapi.PartitionsCheck(pg.Maintenance, clock),
		},
	}
	if len(tokens) > 0 {
		httpDeps.Operator = operators.Authenticate
	}
	handler, err := httpapi.New(httpDeps, httpapi.WithErrorHandler(fault("http")))
	if err != nil {
		return fmt.Errorf("build the REST surface: %w", err)
	}
	s.httpLn, err = net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	s.closers = append(s.closers, func() { _ = s.httpLn.Close() })
	s.httpSrv = &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}

	// -- gRPC
	principal := auth.FirstPrincipal(auth.MTLSPrincipal)
	if len(cfg.RecipientJWKS) > 0 {
		var popts []auth.ProducerOption
		if cfg.ProducerAud != "" {
			popts = append(popts, auth.WithProducerAudience(cfg.ProducerAud))
		}
		principal = auth.FirstPrincipal(auth.MTLSPrincipal, auth.ProducerTokens(cfg.Bindings, keys, popts...))
	}
	unary, streamIc := auth.RealmInterceptors(cfg.Bindings, principal,
		auth.WithDenied(func(p string) { log.Warn("a producer with no realm binding was refused", "principal", p) }),
		auth.WithFault(func(err error) { log.Info("a producer's credentials were refused", "reason", err.Error()) }))
	serverOpts := []grpc.ServerOption{grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(streamIc)}
	if creds, err := s.grpcCredentials(); err != nil {
		return err
	} else if creds != nil {
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	s.grpcSrv = grpc.NewServer(serverOpts...)
	grpcserver.NewRouted(route, grpcserver.WithErrorHandler(fault("grpc producer"))).Register(s.grpcSrv)
	grpcserver.NewCatalog(catalogPort, adminPort, auth.RealmOf, grpcserver.WithCatalogErrorHandler(fault("grpc catalog"))).Register(s.grpcSrv)
	s.grpcLn, err = net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}
	s.closers = append(s.closers, func() { _ = s.grpcLn.Close() })
	return nil
}

func (s *Service) keyOptions() []auth.KeysOption {
	opts := []auth.KeysOption{auth.WithTTL(s.cfg.JWKSTTL), auth.WithMinRefetch(s.cfg.JWKSMinRefetch)}
	if s.cfg.AllowInsecureHTTP {
		opts = append(opts, auth.AllowInsecureHTTP())
	}
	return opts
}

// grpcCredentials is the server's TLS, or nil for a plaintext listener on an internal network. With a
// client CA, a client certificate is required and verified, which is what makes its SAN a principal.
func (s *Service) grpcCredentials() (credentials.TransportCredentials, error) {
	f := s.cfg.GRPCTLS
	if f.Cert == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		return nil, fmt.Errorf("the gRPC certificate: %w", err)
	}
	conf := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if f.ClientCA != "" {
		pem, err := os.ReadFile(f.ClientCA)
		if err != nil {
			return nil, fmt.Errorf("the gRPC client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("the gRPC client CA holds no certificate")
		}
		conf.ClientCAs, conf.ClientAuth = pool, tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(conf), nil
}

// Run serves and works until ctx ends or a loop fails, then stops gracefully: the surfaces finish what they
// are answering, and the loops finish the pass they are in. The first failure of any part ends all of them.
func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	fail := func(name string, err error) {
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", name, err)
		}
		mu.Unlock()
		cancel()
	}
	for _, l := range s.loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fail(l.name, l.run(ctx))
		}()
	}
	if s.httpSrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.httpSrv.Serve(s.httpLn); !errors.Is(err, http.ErrServerClosed) {
				fail("http", err)
			}
		}()
	}
	if s.grpcSrv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fail("grpc", s.grpcSrv.Serve(s.grpcLn))
		}()
	}

	<-ctx.Done()
	stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer stop()
	if s.httpSrv != nil {
		_ = s.httpSrv.Shutdown(stopCtx)
	}
	if s.grpcSrv != nil {
		done := make(chan struct{})
		go func() { s.grpcSrv.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-stopCtx.Done():
			s.grpcSrv.Stop()
		}
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return firstErr
}

// Close releases what Build opened. It is safe to call twice.
func (s *Service) Close() {
	s.closeOne.Do(func() {
		for i := len(s.closers) - 1; i >= 0; i-- {
			s.closers[i]()
		}
	})
}

// ingest connects the optional JetStream ingest (T052, D23): the stream the producers publish to, a durable
// consumer per realm bound to that realm's subjects, and a loop per realm that turns each message into a
// Notify call of that realm's engine. The realm is the subject's, so a consumer is created with the realm's
// filter and the ingest is handed that realm's engine only (NR-009).
func (s *Service) ingest(ctx context.Context, notifiers map[domain.Realm]ports.Notifier, sink ports.PoisonSink) error {
	nc, err := nats.Connect(s.cfg.NATSURL, nats.Name("notifyd"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("NOTIFY_INGEST_NATS_URL: %w", err)
	}
	s.closers = append(s.closers, nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("JetStream: %w", err)
	}
	const stream = "NOTIFY_INGEST"
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: stream, Subjects: []string{natsingest.SubjectPrefix + ".>"}, Replicas: s.cfg.NATSReplicas,
		Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy, MaxAge: 7 * 24 * time.Hour,
	}); err != nil {
		return fmt.Errorf("the ingest stream: %w", err)
	}
	const maxDeliver = 10
	for realm, notifier := range notifiers {
		consumer, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
			Durable: "notifyd-" + string(realm), FilterSubject: natsingest.SubjectPrefix + "." + string(realm) + ".*",
			AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: maxDeliver, AckWait: 30 * time.Second,
		})
		if err != nil {
			return fmt.Errorf("the ingest consumer of realm %q: %w", realm, err)
		}
		in := natsingest.New(map[domain.Realm]ports.Notifier{realm: notifier}, sink, js,
			natsingest.WithMaxDeliver(maxDeliver),
			natsingest.WithErrorHandler(func(err error) { s.log.Warn("ingest", "realm", string(realm), "event", err.Error()) }))
		s.loops = append(s.loops, namedLoop{"nats ingest " + string(realm), func(ctx context.Context) error { return in.Run(ctx, consumer) }})
	}
	return nil
}
