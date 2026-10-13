package webhook

import (
	"net/http"
	"net/netip"
	"testing"
)

// The destinations a tenant's address must never reach. They are checked on the
// address a connection is made to, after the name is resolved, so a name pointing at one
// of them is refused like the number.
func TestBlockedDestinations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ip      string
		blocked bool
		why     string
	}{
		{"127.0.0.1", true, "loopback"},
		{"127.255.255.254", true, "all of 127/8 is loopback"},
		{"::1", true, "IPv6 loopback"},
		{"10.0.0.1", true, "private, RFC 1918"},
		{"172.16.0.1", true, "private, RFC 1918"},
		{"172.31.255.255", true, "the top of 172.16/12"},
		{"192.168.1.1", true, "private, RFC 1918"},
		{"fd00::1", true, "unique local IPv6"},
		{"169.254.169.254", true, "link-local, where a cloud's metadata service lives"},
		{"fe80::1", true, "link-local IPv6"},
		{"100.64.0.1", true, "carrier-grade NAT, shared address space"},
		{"100.127.255.255", true, "the top of 100.64/10"},
		{"0.0.0.0", true, "unspecified"},
		{"::", true, "unspecified IPv6"},
		{"224.0.0.1", true, "multicast"},
		{"ff02::1", true, "multicast IPv6"},
		{"255.255.255.255", true, "broadcast"},
		{"::ffff:127.0.0.1", true, "IPv4 loopback written as IPv6: unmapped before it is judged"},
		{"::ffff:10.0.0.1", true, "a private address in the same disguise"},
		{"::ffff:100.64.0.1", true, "carrier-grade NAT in the same disguise: a prefix does not match a mapped address until it is unmapped"},
		{"::ffff:240.0.0.1", true, "a reserved address in the same disguise"},
		{"::ffff:198.18.0.1", true, "a benchmarking address in the same disguise"},
		{"::ffff:0.0.0.5", true, "this-network in the same disguise"},
		{"8.8.8.8", false, "public"},
		{"1.1.1.1", false, "public"},
		{"172.32.0.1", false, "just past 172.16/12"},
		{"100.128.0.1", false, "just past 100.64/10"},
		{"2606:4700:4700::1111", false, "public IPv6"},
		{"::ffff:8.8.8.8", false, "a public IPv4 as IPv6"},
	}
	for _, tc := range cases {
		t.Run(tc.ip, func(t *testing.T) {
			t.Parallel()
			if got := blockedDestination(netip.MustParseAddr(tc.ip)); got != tc.blocked {
				t.Fatalf("blockedDestination(%s) = %v, want %v: %s", tc.ip, got, tc.blocked, tc.why)
			}
		})
	}
}

// The transport takes no proxy from the environment: a proxy would be what the
// connection is made to, and the destination check would judge the proxy.
func TestTheTransportUsesNoProxy(t *testing.T) {
	t.Parallel()
	ch, err := New("secret")
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := ch.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("the transport is a %T", ch.client.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("the transport has a Proxy: HTTPS_PROXY would redirect every request past the destination check")
	}
	if ch.client.CheckRedirect == nil {
		t.Fatal("the client follows redirects")
	}
}
