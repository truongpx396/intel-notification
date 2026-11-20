// Package wire maps the engine's domain types to the generated notify.v1 messages and back, and a
// domain error to a gRPC status and back. The gRPC server and the gRPC client both stand on it, so a
// field cannot be mapped one way going out and another way coming in.
package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
)

func tenantToProto(t domain.Tenant) *notifyv1.Tenant { return &notifyv1.Tenant{Kind: t.Kind, Id: t.ID} }

func tenantFromProto(p *notifyv1.Tenant) domain.Tenant {
	return domain.Tenant{Kind: p.GetKind(), ID: p.GetId()}
}

func recipientToProto(r domain.Recipient) *notifyv1.Recipient {
	return &notifyv1.Recipient{Kind: r.Kind, Id: r.ID}
}

func recipientFromProto(p *notifyv1.Recipient) domain.Recipient {
	return domain.Recipient{Kind: p.GetKind(), ID: p.GetId()}
}

// timeToProto leaves an unset time unset: a zero DeliverAfter means "now" and a zero DeliverBefore
// "never expires" (D29), and 1970 would mean neither.
func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func timeFromProto(p *timestamppb.Timestamp) time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.AsTime()
}

// maxExactInt is the largest integer a double holds exactly. A protobuf Struct holds numbers as
// doubles, so an integer beyond it would arrive as a different one.
const maxExactInt = 1 << 53

// dataToStruct puts a template-variable document into a Struct. The document is JSON on both sides
// (the store keeps it as JSONB), so what must survive is the document and not the Go types: it goes
// through JSON, which is what turns an int, a []string and a time.Time into what a Struct holds. An
// integer a double cannot hold exactly is an error and not a rounding: a 19-digit id would arrive as
// another id, and the template would render someone else's invoice number.
func dataToStruct(data map[string]any) (*structpb.Struct, error) {
	if data == nil {
		return nil, nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("data is not a JSON document: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("data is not a JSON object: %w", err)
	}
	if err := exact(doc); err != nil {
		return nil, err
	}
	s, err := structpb.NewStruct(doc)
	if err != nil {
		return nil, fmt.Errorf("data does not fit a protobuf Struct: %w", err)
	}
	return s, nil
}

// exact replaces each json.Number in v with the float64 a Struct will hold, refusing an integer that
// float64 cannot hold exactly.
func exact(v any) error {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			if n, ok := e.(json.Number); ok {
				f, err := number(n)
				if err != nil {
					return fmt.Errorf("data[%q]: %w", k, err)
				}
				x[k] = f
				continue
			}
			if err := exact(e); err != nil {
				return err
			}
		}
	case []any:
		for i, e := range x {
			if n, ok := e.(json.Number); ok {
				f, err := number(n)
				if err != nil {
					return fmt.Errorf("data[%d]: %w", i, err)
				}
				x[i] = f
				continue
			}
			if err := exact(e); err != nil {
				return err
			}
		}
	}
	return nil
}

func number(n json.Number) (float64, error) {
	s := n.String()
	if !strings.ContainsAny(s, ".eE") { // an integer: exactness is checkable
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			if i > maxExactInt || i < -maxExactInt {
				return 0, fmt.Errorf("the integer %s is beyond 2^53, which a protobuf Struct cannot hold exactly: send it as a string", s)
			}
			return float64(i), nil
		}
		if u, err := strconv.ParseUint(s, 10, 64); err == nil && u <= maxExactInt {
			return float64(u), nil
		}
		return 0, fmt.Errorf("the integer %s is beyond 2^53, which a protobuf Struct cannot hold exactly: send it as a string", s)
	}
	f, err := n.Float64()
	if err != nil || math.IsInf(f, 0) {
		return 0, fmt.Errorf("the number %s does not fit a double", s)
	}
	return f, nil
}

func dataFromStruct(s *structpb.Struct) map[string]any {
	if s == nil {
		return nil
	}
	return s.AsMap()
}

// NotificationToProto is the message for n. It fails only for Data that cannot cross exactly.
func NotificationToProto(n domain.Notification) (*notifyv1.Notification, error) {
	data, err := dataToStruct(n.Data)
	if err != nil {
		return nil, err
	}
	return &notifyv1.Notification{
		Tenant:        tenantToProto(n.Tenant),
		Recipient:     recipientToProto(n.Recipient),
		Topic:         string(n.Topic),
		Priority:      string(n.Priority),
		Data:          data,
		Title:         n.Title,
		Body:          n.Body,
		Payload:       n.Payload,
		IdemKey:       n.IdemKey,
		OccurredAt:    timeToProto(n.OccurredAt),
		DeliverAfter:  timeToProto(n.DeliverAfter),
		DeliverBefore: timeToProto(n.DeliverBefore),
		Attributes:    n.Attributes,
	}, nil
}

// NotificationFromProto is the notification a message carries. It carries no realm: the message has
// none to carry, and the engine's realm is its own (NR-009).
func NotificationFromProto(p *notifyv1.Notification) domain.Notification {
	return domain.Notification{
		Tenant:        tenantFromProto(p.GetTenant()),
		Recipient:     recipientFromProto(p.GetRecipient()),
		Topic:         domain.Topic(p.GetTopic()),
		Priority:      domain.Priority(p.GetPriority()),
		Data:          dataFromStruct(p.GetData()),
		Title:         p.GetTitle(),
		Body:          p.GetBody(),
		Payload:       p.GetPayload(),
		Attributes:    p.GetAttributes(),
		IdemKey:       p.GetIdemKey(),
		OccurredAt:    timeFromProto(p.GetOccurredAt()),
		DeliverAfter:  timeFromProto(p.GetDeliverAfter()),
		DeliverBefore: timeFromProto(p.GetDeliverBefore()),
	}
}

// ReceiptToProto is the message for r. It names the channels planned, in plan order, and not each
// plan's timing: a producer learns the rest from Status.
func ReceiptToProto(r domain.Receipt) *notifyv1.Receipt {
	var channels []string
	for _, d := range r.Deliveries {
		channels = append(channels, string(d.Channel))
	}
	return &notifyv1.Receipt{NotificationId: r.NotificationID, Applied: r.Applied, Channels: channels, InboxVisible: r.InboxVisible}
}

// ReceiptFromProto is the receipt a message carries. The message does not echo the key, so the caller
// supplies the one it sent.
func ReceiptFromProto(p *notifyv1.Receipt, idemKey string) domain.Receipt {
	r := domain.Receipt{NotificationID: p.GetNotificationId(), IdemKey: idemKey, Applied: p.GetApplied(), InboxVisible: p.GetInboxVisible()}
	for _, ch := range p.GetChannels() {
		r.Deliveries = append(r.Deliveries, domain.PlannedDelivery{Channel: domain.ChannelKind(ch)})
	}
	return r
}

// BroadcastToProto is the message for b. A request that names both sources cannot be said on the
// wire, where the source is one of two, and is an error and not a request that quietly names one: the
// library refuses it, so the service must.
func BroadcastToProto(b domain.BroadcastRequest) (*notifyv1.BroadcastRequest, error) {
	data, err := dataToStruct(b.Data)
	if err != nil {
		return nil, err
	}
	p := &notifyv1.BroadcastRequest{
		Tenant: tenantToProto(b.Tenant), Topic: string(b.Topic), Priority: string(b.Priority), Data: data,
		Title: b.Title, Body: b.Body, Payload: b.Payload, IdemKey: b.IdemKey,
	}
	switch {
	case b.Audience != "" && len(b.Recipients) > 0:
		return nil, &domain.ValidationError{Problems: []domain.Problem{{
			Field: "Audience", Message: "and Recipients are both set: a broadcast has exactly one source of recipients"}}}
	case b.Audience != "":
		p.Source = &notifyv1.BroadcastRequest_Audience{Audience: b.Audience}
	case len(b.Recipients) > 0:
		list := &notifyv1.RecipientList{}
		for _, r := range b.Recipients {
			list.Recipients = append(list.Recipients, recipientToProto(r))
		}
		p.Source = &notifyv1.BroadcastRequest_Recipients{Recipients: list}
	}
	return p, nil
}

// BroadcastFromProto is the broadcast a message carries.
func BroadcastFromProto(p *notifyv1.BroadcastRequest) domain.BroadcastRequest {
	b := domain.BroadcastRequest{
		Tenant: tenantFromProto(p.GetTenant()), Audience: p.GetAudience(), Topic: domain.Topic(p.GetTopic()),
		Priority: domain.Priority(p.GetPriority()), Data: dataFromStruct(p.GetData()), Title: p.GetTitle(), Body: p.GetBody(),
		Payload: p.GetPayload(), IdemKey: p.GetIdemKey(),
	}
	for _, r := range p.GetRecipients().GetRecipients() {
		b.Recipients = append(b.Recipients, recipientFromProto(r))
	}
	return b
}

func BroadcastReceiptToProto(r domain.BroadcastReceipt) *notifyv1.BroadcastReceipt {
	return &notifyv1.BroadcastReceipt{BroadcastId: r.BroadcastID, Applied: r.Applied}
}

func BroadcastReceiptFromProto(p *notifyv1.BroadcastReceipt) domain.BroadcastReceipt {
	return domain.BroadcastReceipt{BroadcastID: p.GetBroadcastId(), Applied: p.GetApplied()}
}

// RefToProto names one notification by its producer key, for Cancel and Status.
func RefToProto(c domain.CancelRequest) *notifyv1.NotificationRef {
	return &notifyv1.NotificationRef{Tenant: tenantToProto(c.Tenant), Recipient: recipientToProto(c.Recipient), IdemKey: c.IdemKey}
}

func RefFromProto(p *notifyv1.NotificationRef) domain.CancelRequest {
	return domain.CancelRequest{Tenant: tenantFromProto(p.GetTenant()), Recipient: recipientFromProto(p.GetRecipient()), IdemKey: p.GetIdemKey()}
}

func CancelReceiptToProto(r domain.CancelReceipt) *notifyv1.CancelReceipt {
	return &notifyv1.CancelReceipt{Matched: r.Matched, Canceled: int32(r.Canceled), InFlight: int32(r.InFlight)} //nolint:gosec // counts of one notification's deliveries
}

func CancelReceiptFromProto(p *notifyv1.CancelReceipt) domain.CancelReceipt {
	return domain.CancelReceipt{Matched: p.GetMatched(), Canceled: int(p.GetCanceled()), InFlight: int(p.GetInFlight())}
}

func StatusToProto(s domain.NotificationStatus) *notifyv1.NotificationStatus {
	p := &notifyv1.NotificationStatus{Found: s.Found, NotificationId: s.NotificationID, Canceled: s.Canceled}
	for _, d := range s.Deliveries {
		p.Deliveries = append(p.Deliveries, &notifyv1.DeliveryStatus{
			Channel: string(d.Channel), AddressKey: d.AddressKey, State: d.State, Attempts: int32(d.Attempts), //nolint:gosec // an attempt count
			ProviderMessageId: d.ProviderMessageID, ProviderStatus: d.ProviderStatus, CompletedAt: timeToProto(d.CompletedAt),
		})
	}
	return p
}

func StatusFromProto(p *notifyv1.NotificationStatus) domain.NotificationStatus {
	s := domain.NotificationStatus{Found: p.GetFound(), NotificationID: p.GetNotificationId(), Canceled: p.GetCanceled()}
	for _, d := range p.GetDeliveries() {
		s.Deliveries = append(s.Deliveries, domain.DeliveryStatus{
			Channel: domain.ChannelKind(d.GetChannel()), AddressKey: d.GetAddressKey(), State: d.GetState(), Attempts: int(d.GetAttempts()),
			ProviderMessageID: d.GetProviderMessageId(), ProviderStatus: d.GetProviderStatus(), CompletedAt: timeFromProto(d.GetCompletedAt()),
		})
	}
	return s
}

// ErrorDomain is the ErrorInfo domain of every status this package makes. A producer in another
// language reads the reason beside it, so the pairs of code and reason below are as much the contract
// as the proto is.
const ErrorDomain = "notify.v1"

// The reasons. Each is stable: a producer branches on it.
const (
	ReasonMissingIdemKey      = "MISSING_IDEM_KEY"
	ReasonEmptyIdentity       = "EMPTY_IDENTITY"
	ReasonUnknownTopic        = "UNKNOWN_TOPIC"
	ReasonTooManyRecipients   = "TOO_MANY_RECIPIENTS"
	ReasonAudienceUnsupported = "AUDIENCE_UNSUPPORTED"
	ReasonInvalidRequest      = "INVALID_REQUEST"
	ReasonBatchTooLarge       = "BATCH_TOO_LARGE"
)

// refusals are the engine's sentinels a producer can act on, in the order they are tried: an error
// that wraps several (a request with no key and no tenant) is reported as the first.
var refusals = []struct {
	err    error
	code   codes.Code
	reason string
}{
	{domain.ErrMissingIdem, codes.InvalidArgument, ReasonMissingIdemKey},
	{domain.ErrEmptyIdentity, codes.InvalidArgument, ReasonEmptyIdentity},
	{domain.ErrUnknownTopic, codes.InvalidArgument, ReasonUnknownTopic},
	{domain.ErrTooManyRecipients, codes.InvalidArgument, ReasonTooManyRecipients},
	{domain.ErrAudienceUnsupported, codes.FailedPrecondition, ReasonAudienceUnsupported},
}

// Reject is a status of code with the reason a producer reads beside it.
func Reject(code codes.Code, reason, message string) error {
	st := status.New(code, message)
	if withInfo, err := st.WithDetails(&errdetails.ErrorInfo{Domain: ErrorDomain, Reason: reason}); err == nil {
		st = withInfo
	}
	return st.Err()
}

// ToStatus is the server's half of an error crossing the wire: the status the producer sees.
//
// A refusal the producer can act on keeps its code, its reason and its own words. A caller that ended
// the call (its context was cancelled, or its deadline passed) is told so, and is no fault. Anything
// else is a fault of the engine's own: a SQL error, a host name, a DSN. The producer is told there was
// one and not what it was, and report, when it is not nil, is given the real error so an operator can
// read it.
func ToStatus(err error, report func(error)) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err // already a status: the server's own refusal, or one the engine passed through
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	for _, r := range refusals {
		if errors.Is(err, r.err) {
			return Reject(r.code, r.reason, err.Error())
		}
	}
	var invalid *domain.ValidationError
	if errors.As(err, &invalid) {
		return Reject(codes.InvalidArgument, ReasonInvalidRequest, err.Error())
	}
	if report != nil {
		report(err)
	}
	return status.Error(codes.Internal, "notify: internal error")
}

// remoteError is an error that came back over the wire. It is still the gRPC status it arrived as, so
// status.Code reads it, and it unwraps to the engine's own error, so errors.Is and errors.As read it as
// they would read the library's.
type remoteError struct {
	st    *status.Status
	cause error
}

func (e *remoteError) Error() string              { return e.st.Message() }
func (e *remoteError) Unwrap() error              { return e.cause }
func (e *remoteError) GRPCStatus() *status.Status { return e.st }

// FromStatus is the client's half. A refusal comes back as the sentinel the library returns for it
// (and a request that broke a rule as a *domain.ValidationError, as the library does), so a producer's
// `errors.Is` reads the same in library mode and in service mode. A context ending comes back as the
// context's own error. Every other failure, a transport that is down above all, stays the gRPC status
// it is, because the producer's retry policy reads its code.
func FromStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() { //nolint:exhaustive // the others are the engine's refusals, read below, or a transport's status, kept
	case codes.Canceled:
		return &remoteError{st: st, cause: context.Canceled}
	case codes.DeadlineExceeded:
		return &remoteError{st: st, cause: context.DeadlineExceeded}
	}
	reason := ""
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == ErrorDomain {
			reason = info.GetReason()
			break
		}
	}
	var sentinel error
	for _, r := range refusals {
		if r.reason == reason && r.code == st.Code() {
			sentinel = r.err
		}
	}
	switch {
	case reason == ReasonInvalidRequest && st.Code() == codes.InvalidArgument,
		sentinel != nil && st.Code() == codes.InvalidArgument:
		return &remoteError{st: st, cause: &domain.ValidationError{Problems: []domain.Problem{{Field: "request", Message: st.Message(), Err: sentinel}}}}
	case sentinel != nil:
		return &remoteError{st: st, cause: sentinel}
	}
	return err
}

// Message is the text of a status: what a batch puts in errors[i].
func Message(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}
