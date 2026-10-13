package email_test

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"net/mail"
	"net/textproto"
	"strings"
	"sync"
	"testing"

	"github.com/truongpx396/intel-notification/internal/notifytest"
)

// received is one message the fake relay accepted.
type received struct {
	From string
	To   []string
	Raw  string // LF line endings: the dot-decoding of DATA
	TLS  bool
}

// smtpServer is an SMTP relay for tests. It answers like one, from a script of
// replies per stage, and, when given a probe, like a provider under the channel
// contract: it learns the delivery key from the Message-ID, consumes the armed
// failure and answers it as a real relay would, and counts a send per message.
// It never dedupes, which is what a plain relay is.
type smtpServer struct {
	ln    net.Listener
	probe *notifytest.Probe
	tls   *tls.Config // when set, STARTTLS is offered

	mu       sync.Mutex
	messages []received
	script   map[string]string // stage -> reply line, for every conversation
	stages   []string          // the stages reached, in order, across conversations
	wg       sync.WaitGroup
}

func newSMTPServer(t *testing.T, probe *notifytest.Probe, tlsConfig *tls.Config) *smtpServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &smtpServer{ln: ln, probe: probe, tls: tlsConfig, script: map[string]string{}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(c)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		s.wg.Wait()
	})
	return s
}

func (s *smtpServer) addr() string { return s.ln.Addr().String() }

// reply scripts the line the server answers a stage with: AUTH, MAIL, RCPT or DATA.
func (s *smtpServer) reply(stage, line string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.script[stage] = line
}

func (s *smtpServer) accepted() []received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]received(nil), s.messages...)
}

func (s *smtpServer) scripted(stage string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stages = append(s.stages, stage)
	line, ok := s.script[stage]
	return line, ok
}

func (s *smtpServer) serve(raw net.Conn) {
	defer raw.Close()
	conn := raw
	br := bufio.NewReader(conn)
	say := func(lines ...string) {
		for _, l := range lines {
			_, _ = io.WriteString(conn, l+"\r\n")
		}
	}
	say("220 fake.example.test ESMTP")
	var from string
	var to []string
	secured := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		verb := strings.ToUpper(strings.Fields(line + " ")[0])
		switch verb {
		case "EHLO", "HELO":
			caps := []string{"250-fake.example.test", "250-8BITMIME"}
			if s.tls != nil && !secured {
				caps = append(caps, "250-STARTTLS")
			}
			if _, ok := s.script["AUTH"]; ok {
				caps = append(caps, "250-AUTH PLAIN")
			}
			caps = append(caps, "250 SIZE 10485760")
			say(caps...)
		case "STARTTLS":
			say("220 2.0.0 go ahead")
			tc := tls.Server(conn, s.tls)
			if err := tc.Handshake(); err != nil {
				return
			}
			conn, br, secured = tc, bufio.NewReader(tc), true
		case "AUTH":
			if r, ok := s.scripted("AUTH"); ok {
				say(r)
			} else {
				say("235 2.7.0 ok")
			}
		case "MAIL":
			if r, ok := s.scripted("MAIL"); ok && !strings.HasPrefix(r, "250") {
				say(r)
				continue
			}
			from = angle(line)
			to = nil
			say("250 2.1.0 ok")
		case "RCPT":
			if r, ok := s.scripted("RCPT"); ok && !strings.HasPrefix(r, "250") {
				say(r)
				continue
			}
			to = append(to, angle(line))
			say("250 2.1.5 ok")
		case "DATA":
			say("354 end data with <CR><LF>.<CR><LF>")
			body, err := textproto.NewReader(br).ReadDotBytes()
			if err != nil {
				return
			}
			if !s.finish(conn, say, received{From: from, To: to, Raw: string(body), TLS: secured}) {
				return
			}
		case "RSET", "NOOP":
			say("250 2.0.0 ok")
		case "QUIT":
			say("221 2.0.0 bye")
			return
		default:
			say("502 5.5.2 command not recognized")
		}
	}
}

// finish answers the end of DATA. It reports false when the connection is to be
// dropped.
func (s *smtpServer) finish(_ net.Conn, say func(...string), msg received) bool {
	if r, ok := s.scripted("DATA"); ok && !strings.HasPrefix(r, "250") {
		say(r)
		return true
	}
	if s.probe != nil {
		key := messageKey(msg.Raw)
		if f, failed := s.probe.Attempt(key); failed {
			switch f {
			case notifytest.Transient:
				say("451 4.3.0 try again later")
			case notifytest.DeadAddress:
				say("550 5.1.1 no such user here")
			case notifytest.Permanent:
				say("554 5.6.0 message refused")
			default:
				return false // an unclassified fault: the connection drops
			}
			return true
		}
		s.probe.Record(key, false)
	}
	s.mu.Lock()
	s.messages = append(s.messages, msg)
	s.mu.Unlock()
	say("250 2.0.0 queued")
	return true
}

// messageKey is the local part of the Message-ID: the delivery key the mailer put
// there.
func messageKey(raw string) string {
	m, err := mail.ReadMessage(bytes.NewReader([]byte(raw)))
	if err != nil {
		return ""
	}
	id := strings.Trim(m.Header.Get("Message-ID"), "<>")
	local, _, _ := strings.Cut(id, "@")
	return local
}

// angle is the path in a MAIL FROM or RCPT TO line: what is between the brackets,
// whatever parameters follow.
func angle(line string) string {
	open, end := strings.IndexByte(line, '<'), strings.IndexByte(line, '>')
	if open < 0 || end < open {
		return ""
	}
	return line[open+1 : end]
}
