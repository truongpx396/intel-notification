// Package wire maps the engine's domain types to the generated notify.v1 messages and back, and a
// domain error to a gRPC status and back. The gRPC server and the gRPC client both stand on it, so a
// field cannot be mapped one way going out and another way coming in.
package wire

import (
	"errors"

	"github.com/truongpx396/intel-notification/api/notifyv1"
	"github.com/truongpx396/intel-notification/domain"
)

// ErrorDomain is the ErrorInfo domain of every status this package makes. A producer in another
// language reads the reason beside it.
const ErrorDomain = "notify.v1"

var errNotImplemented = errors.New("wire: not implemented")

func NotificationToProto(domain.Notification) (*notifyv1.Notification, error) {
	return nil, errNotImplemented
}
func NotificationFromProto(*notifyv1.Notification) domain.Notification { return domain.Notification{} }
func ReceiptToProto(domain.Receipt) *notifyv1.Receipt                  { return nil }
func ReceiptFromProto(*notifyv1.Receipt, string) domain.Receipt        { return domain.Receipt{} }
func BroadcastToProto(domain.BroadcastRequest) (*notifyv1.BroadcastRequest, error) {
	return nil, errNotImplemented
}
func BroadcastFromProto(*notifyv1.BroadcastRequest) domain.BroadcastRequest {
	return domain.BroadcastRequest{}
}
func BroadcastReceiptToProto(domain.BroadcastReceipt) *notifyv1.BroadcastReceipt { return nil }
func BroadcastReceiptFromProto(*notifyv1.BroadcastReceipt) domain.BroadcastReceipt {
	return domain.BroadcastReceipt{}
}
func RefToProto(domain.CancelRequest) *notifyv1.NotificationRef         { return nil }
func RefFromProto(*notifyv1.NotificationRef) domain.CancelRequest       { return domain.CancelRequest{} }
func CancelReceiptToProto(domain.CancelReceipt) *notifyv1.CancelReceipt { return nil }
func CancelReceiptFromProto(*notifyv1.CancelReceipt) domain.CancelReceipt {
	return domain.CancelReceipt{}
}
func StatusToProto(domain.NotificationStatus) *notifyv1.NotificationStatus { return nil }
func StatusFromProto(*notifyv1.NotificationStatus) domain.NotificationStatus {
	return domain.NotificationStatus{}
}

// ToStatus is the server's half of an error crossing the wire.
func ToStatus(err error, report func(error)) error { return err }

// FromStatus is the client's half.
func FromStatus(err error) error { return err }
