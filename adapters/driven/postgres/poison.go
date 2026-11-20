package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/truongpx396/intel-notification/domain"
	"github.com/truongpx396/intel-notification/ports"
)

var _ ports.PoisonSink = (*Store)(nil)

// maxPoisonBody bounds the body a poison dead letter keeps, so a producer that publishes garbage in bulk
// cannot turn the dead-letter table into the thing that fills the disk (D8).
const maxPoisonBody = 64 << 10

// recordPoisonSQL parks the message unless it is already parked: a message the bus redelivers while it was
// being given up on is one dead letter. The id is derived from the message's key, so the check needs no
// column of its own.
const recordPoisonSQL = `
INSERT INTO dead_letters (id, realm, source, reason, payload, attempts, last_error, created_at)
SELECT $1::uuid, $2, 'bus', 'poison', $3::jsonb, $4, $5, $6
 WHERE NOT EXISTS (SELECT 1 FROM dead_letters WHERE id = $1::uuid)`

// RecordPoison implements ports.PoisonSink. The row has no tenant, recipient or queue row: what a poison
// message is, is that none of those could be read from it. The body is kept as text when it is text, and
// as base64 when it is not, and is cut at maxPoisonBody.
func (s *Store) RecordPoison(ctx context.Context, m domain.PoisonMessage) error {
	if err := m.Realm.Validate(); err != nil {
		return fmt.Errorf("record poison: %w", err)
	}
	if m.Key == "" {
		return fmt.Errorf("record poison: a message with no key could not be told from a redelivery of itself")
	}
	body, encoding, truncated := m.Body, "utf8", false
	if len(body) > maxPoisonBody {
		body, truncated = body[:maxPoisonBody], true
	}
	text := string(body)
	if !utf8.Valid(body) || containsNUL(body) {
		text, encoding = base64.StdEncoding.EncodeToString(body), "base64"
	}
	payload, err := json.Marshal(map[string]any{"subject": m.Subject, "key": m.Key, "body": text, "body_encoding": encoding, "truncated": truncated})
	if err != nil {
		return fmt.Errorf("record poison: %w", err)
	}
	sum := sha256.Sum256([]byte(string(m.Realm) + "\x00" + m.Key))
	sum[6], sum[8] = sum[6]&0x0f|0x80, sum[8]&0x3f|0x80 // a version 8 UUID: derived, not random
	id := fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
	if _, err := s.pool.Exec(ctx, recordPoisonSQL, id, string(m.Realm), payload, m.Attempts, m.Reason, s.clock.Now()); err != nil {
		return fmt.Errorf("record poison: %w", err)
	}
	return nil
}

func containsNUL(b []byte) bool {
	for _, c := range b {
		if c == 0 {
			return true
		}
	}
	return false
}
