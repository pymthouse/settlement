package kafkax

import "testing"

func TestDefaultBillingTopicSpecsCoversEveryBillingTopic(t *testing.T) {
	specs := DefaultBillingTopicSpecs(Options{
		TopicOpenMeter:      "billing.openmeter.invoices.v1",
		TopicStripe:         "billing.stripe.events.v1",
		TopicDLQ:            "billing.dlq.v1",
		TopicCollectRequest: "billing.collect.requests.v1",
	})

	if len(specs) != 4 {
		t.Fatalf("got %d specs, want 4", len(specs))
	}
	names := map[string]bool{}
	for _, s := range specs {
		names[s.Name] = true
		if s.Partitions <= 0 {
			t.Errorf("%s: partitions = %d, want > 0", s.Name, s.Partitions)
		}
		if s.RetentionMs <= 0 {
			t.Errorf("%s: retention = %d, want > 0 (billing is the audit trail)", s.Name, s.RetentionMs)
		}
	}
	for _, want := range []string{
		"billing.openmeter.invoices.v1",
		"billing.stripe.events.v1",
		"billing.dlq.v1",
		"billing.collect.requests.v1",
	} {
		if !names[want] {
			t.Errorf("missing spec for %s", want)
		}
	}
}

// An unconfigured topic name (empty string) must not produce a spec asking
// Kafka to create a nameless topic.
func TestDefaultBillingTopicSpecsSkipsUnconfiguredTopics(t *testing.T) {
	specs := DefaultBillingTopicSpecs(Options{
		TopicOpenMeter: "billing.openmeter.invoices.v1",
	})
	if len(specs) != 1 {
		t.Fatalf("got %d specs, want 1", len(specs))
	}
	if specs[0].Name != "billing.openmeter.invoices.v1" {
		t.Errorf("spec name = %q", specs[0].Name)
	}
}
