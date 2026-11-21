// Package secrets resolves the secret references of channel_providers rows to the credentials they name
// (D31): env:NAME from the process environment and file:/path from a mounted file, which is how a
// container orchestrator hands a service its secrets. A reference to a secret manager is a resolver
// registered under its scheme.
//
// A credential never appears in an error. What an error says is the reference, which is not secret, and
// what went wrong in general terms.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

// maxFileBytes bounds a secret read from a file, so a reference that points at the wrong file does not
// read a large one into memory.
const maxFileBytes = 64 << 10

// ErrUnsupportedScheme is wrapped by the refusal of a reference whose scheme has no resolver.
var ErrUnsupportedScheme = errors.New("secrets: no resolver for the scheme")

// Resolver resolves env: and file: references, and any scheme registered with WithScheme.
type Resolver struct {
	lookupEnv func(string) (string, bool)
	readFile  func(string) ([]byte, error)
	schemes   map[string]ports.SecretResolver
}

var _ ports.SecretResolver = (*Resolver)(nil)

// Option configures a Resolver.
type Option func(*Resolver)

// WithEnv replaces the environment lookup: a test's, which cannot set the process's in parallel.
func WithEnv(lookup func(string) (string, bool)) Option {
	return func(r *Resolver) { r.lookupEnv = lookup }
}

// WithReadFile replaces how a file is read.
func WithReadFile(read func(string) ([]byte, error)) Option {
	return func(r *Resolver) { r.readFile = read }
}

// WithScheme registers the resolver of a scheme this package does not know, such as a secret manager's.
func WithScheme(scheme string, resolver ports.SecretResolver) Option {
	return func(r *Resolver) { r.schemes[scheme] = resolver }
}

// New returns a Resolver over the process environment and the file system.
func New(opts ...Option) *Resolver {
	r := &Resolver{lookupEnv: os.LookupEnv, readFile: readLimited, schemes: map[string]ports.SecretResolver{}}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// readLimited reads a file of at most maxFileBytes.
func readLimited(path string) ([]byte, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, maxFileBytes+1)
	n, err := f.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

// Resolve implements ports.SecretResolver.
func (r *Resolver) Resolve(ctx context.Context, ref string) (string, error) {
	scheme, rest, err := domain.ParseSecretRef(ref)
	if err != nil {
		return "", err
	}
	switch scheme {
	case "env":
		value, ok := r.lookupEnv(rest)
		if !ok || value == "" {
			return "", fmt.Errorf("secret %s: the environment variable %q is not set", ref, rest)
		}
		return value, nil
	case "file":
		if !filepath.IsAbs(rest) {
			return "", fmt.Errorf("secret %s: the path must be absolute", ref)
		}
		raw, err := r.readFile(rest)
		if err != nil {
			return "", fmt.Errorf("secret %s: the file cannot be read: %w", ref, pathError(err))
		}
		if len(raw) > maxFileBytes {
			return "", fmt.Errorf("secret %s: the file is over %d bytes, which no credential is", ref, maxFileBytes)
		}
		value := strings.TrimSuffix(strings.TrimSuffix(string(raw), "\n"), "\r")
		if value == "" {
			return "", fmt.Errorf("secret %s: the file is empty", ref)
		}
		return value, nil
	}
	if other, ok := r.schemes[scheme]; ok {
		return other.Resolve(ctx, ref)
	}
	return "", fmt.Errorf("%w %q (env and file are built in): %s", ErrUnsupportedScheme, scheme, ref)
}

// pathError keeps what went wrong with a file and drops nothing a credential could be in: an *os.PathError
// names a path and an operating-system condition and holds no content.
func pathError(err error) error {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}
