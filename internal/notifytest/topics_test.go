package notifytest

import (
	"slices"
	"testing"

	"github.com/truongpx396/intel-notification/domain"
)

func sampleTopic() domain.TopicDef {
	return domain.TopicDef{
		DefaultChannels: []domain.ChannelKind{"in_app", "email"},
		Fallback:        []domain.ChannelKind{"sms"},
		DefaultPriority: domain.PriorityWarning,
		Essential:       true,
		TemplateRef:     "invoice",
	}
}

func sameTopic(a, b domain.TopicDef) bool {
	return slices.Equal(a.DefaultChannels, b.DefaultChannels) && slices.Equal(a.Fallback, b.Fallback) &&
		a.DefaultPriority == b.DefaultPriority && a.Essential == b.Essential && a.TemplateRef == b.TemplateRef
}

func TestTopicsLookup(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	topics := NewTopics()
	topics.Register("aisat", "invoice_overdue", sampleTopic())

	def, ok, err := topics.Lookup(ctx, "aisat", "invoice_overdue")
	if err != nil || !ok || !sameTopic(def, sampleTopic()) {
		t.Fatalf("Lookup = %+v, %v, %v, want the registered topic", def, ok, err)
	}

	def, ok, err = topics.Lookup(ctx, "aisat", "never_registered")
	if err != nil || ok || !sameTopic(def, domain.TopicDef{}) {
		t.Fatalf("Lookup of an unknown topic = %+v, %v, %v, want not found and no error", def, ok, err)
	}
}

// One service serves several realms, so a topic is registered in a realm and is
// not visible from another (D1, D31).
func TestTopicsAreScopedToARealm(t *testing.T) {
	t.Parallel()
	topics := NewTopics()
	topics.Register("product-a", "invite", sampleTopic())
	if _, ok, _ := topics.Lookup(t.Context(), "product-b", "invite"); ok {
		t.Fatal("a topic registered in one realm was found in another")
	}
	if _, ok, _ := topics.Lookup(t.Context(), "product-a", "invite"); !ok {
		t.Fatal("the topic was not found in its own realm")
	}
}

func TestTopicsRegisterReplaces(t *testing.T) {
	t.Parallel()
	topics := NewTopics()
	topics.Register("aisat", "invite", sampleTopic())
	changed := sampleTopic()
	changed.Essential = false
	changed.DefaultPriority = domain.PriorityInfo
	topics.Register("aisat", "invite", changed)
	def, _, _ := topics.Lookup(t.Context(), "aisat", "invite")
	if !sameTopic(def, changed) {
		t.Fatalf("Lookup = %+v, want the later registration %+v", def, changed)
	}
}

// Both the registry and the caller hold slices. Neither may change what the other
// sees: a planner that sorts a channel list must not reorder the registry's.
func TestTopicsDoNotShareSlicesWithCallers(t *testing.T) {
	t.Parallel()
	topics := NewTopics()
	in := sampleTopic()
	topics.Register("aisat", "invite", in)
	in.DefaultChannels[0] = "tampered-after-register"

	out, _, _ := topics.Lookup(t.Context(), "aisat", "invite")
	if len(out.DefaultChannels) != 2 || len(out.Fallback) != 1 {
		t.Fatalf("Lookup returned %+v, want the registered topic", out)
	}
	if out.DefaultChannels[0] != "in_app" {
		t.Fatalf("a slice the caller kept changed the registration: %v", out.DefaultChannels)
	}
	out.DefaultChannels[0] = "tampered-after-lookup"
	out.Fallback[0] = "tampered-after-lookup"

	again, _, _ := topics.Lookup(t.Context(), "aisat", "invite")
	if !sameTopic(again, sampleTopic()) {
		t.Fatalf("a slice returned by Lookup changed the registration: %+v", again)
	}
}

func TestTopicsRefuseWhatTheRealRegistryWould(t *testing.T) {
	t.Parallel()
	repeated := sampleTopic()
	repeated.Fallback = []domain.ChannelKind{"sms", "sms"}
	cases := []struct {
		name  string
		topic domain.Topic
		def   domain.TopicDef
	}{
		{"a fallback chain that repeats a channel", "invite", repeated},
		{"a topic with no name", "", sampleTopic()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Fatal("Register did not panic")
				}
			}()
			NewTopics().Register("aisat", tc.topic, tc.def)
		})
	}
}
