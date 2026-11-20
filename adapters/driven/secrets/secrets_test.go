package secrets_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/truongpx396/intel-notification/adapters/driven/secrets"
)

// T049, D31. A credential is read from where its reference says, and appears nowhere else.

const credential = "re_live_SuperSecretValue123"

func TestResolveReadsTheEnvironmentAndFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return "file:" + path
	}
	r := secrets.New(secrets.WithEnv(func(name string) (string, bool) {
		return map[string]string{"RESEND_KEY": credential, "EMPTY": ""}[name], name == "RESEND_KEY" || name == "EMPTY"
	}))
	cases := []struct {
		name, ref, want string
	}{
		{"an environment variable", "env:RESEND_KEY", credential},
		{"a file", write("plain", credential), credential},
		{"a file ends with the newline an editor adds", write("newline", credential+"\n"), credential},
		{"a file with a Windows line ending", write("crlf", credential+"\r\n"), credential},
		{"only one trailing newline is a newline", write("two", credential+"\n\n"), credential + "\n"},
	}
	for _, tc := range cases {
		got, err := r.Resolve(t.Context(), tc.ref)
		if err != nil || got != tc.want {
			t.Errorf("%s: Resolve(%q) = %q, %v, want %q", tc.name, tc.ref, got, err, tc.want)
		}
	}
	dirRef := "file:" + dir
	for name, ref := range map[string]string{
		"an unset variable":          "env:NOPE",
		"an empty variable":          "env:EMPTY",
		"a file that is not there":   "file:" + filepath.Join(dir, "missing"),
		"a relative path":            "file:relative/key",
		"an empty file":              write("empty", ""),
		"a directory":                dirRef,
		"a reference with no scheme": credential,
		"a scheme nobody resolves":   "vault://kv/notify#key",
	} {
		got, err := r.Resolve(t.Context(), ref)
		if err == nil || got != "" {
			t.Errorf("%s: Resolve(%q) = %q, %v, want a refusal and no value", name, ref, got, err)
			continue
		}
		if strings.Contains(err.Error(), credential) {
			t.Errorf("%s: the error %q holds the credential", name, err)
		}
	}
}

func TestResolveRefusesAFileNoCredentialIs(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "big")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 70<<10)), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := secrets.New().Resolve(t.Context(), "file:"+path); err == nil || got != "" {
		t.Fatalf("Resolve of a 70 KiB file = %d bytes, %v, want a refusal", len(got), err)
	}
}

func TestAnErrorFromAFileNeverCarriesWhatTheFileHeld(t *testing.T) {
	t.Parallel()
	r := secrets.New(secrets.WithReadFile(func(path string) ([]byte, error) {
		return nil, errors.New("read " + path + ": the disk said " + credential)
	}))
	_, err := r.Resolve(t.Context(), "file:/run/secrets/key")
	if err == nil {
		t.Fatal("Resolve = nil, want an error")
	}
	// An *os.PathError is stripped to its condition; an error of any other shape is the reader's own and
	// is passed as it is, so what a custom reader returns is the custom reader's to keep clean.
	if !strings.Contains(err.Error(), "file:/run/secrets/key") {
		t.Fatalf("error %q does not name the reference", err)
	}
}

type fakeScheme struct{}

func (fakeScheme) Resolve(_ context.Context, ref string) (string, error) { return "from-" + ref, nil }

func TestASchemeCanBeRegistered(t *testing.T) {
	t.Parallel()
	r := secrets.New(secrets.WithScheme("vault", fakeScheme{}))
	if got, err := r.Resolve(t.Context(), "vault://kv/x"); err != nil || got != "from-vault://kv/x" {
		t.Fatalf("Resolve = %q, %v, want the registered resolver's answer", got, err)
	}
	if _, err := r.Resolve(t.Context(), "gcp://other"); !errors.Is(err, secrets.ErrUnsupportedScheme) {
		t.Fatalf("Resolve of an unregistered scheme = %v, want ErrUnsupportedScheme", err)
	}
}

// The rules are the resolver's own and do not lean on the file system refusing: a reader that would
// succeed for any path is shown never to be asked for a relative one, and one that returns nothing is an
// empty secret.
func TestTheResolversOwnRulesHoldWhateverTheReaderWouldDo(t *testing.T) {
	t.Parallel()
	asked := 0
	r := secrets.New(secrets.WithReadFile(func(string) ([]byte, error) {
		asked++
		return []byte(credential), nil
	}))
	if got, err := r.Resolve(t.Context(), "file:relative/key"); err == nil || got != "" || asked != 0 {
		t.Fatalf("Resolve of a relative path = %q, %v after %d reads, want a refusal before any read: a relative path means whatever the working directory is", got, err, asked)
	}
	empty := secrets.New(secrets.WithReadFile(func(string) ([]byte, error) { return nil, nil }))
	if got, err := empty.Resolve(t.Context(), "file:/run/secrets/key"); err == nil || got != "" {
		t.Fatalf("Resolve of an empty read = %q, %v, want a refusal: an empty credential is a missing one", got, err)
	}
	huge := secrets.New(secrets.WithReadFile(func(string) ([]byte, error) { return []byte(strings.Repeat("x", 70<<10)), nil }))
	if got, err := huge.Resolve(t.Context(), "file:/run/secrets/key"); err == nil || got != "" {
		t.Fatalf("Resolve of a 70 KiB read = %d bytes, %v, want a refusal", len(got), err)
	}
}
