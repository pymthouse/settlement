// Package kafkax builds the Kafka clients used by the doorman, the worker and
// the CLI, with one place to reason about durability and auth.
//
// The billing topics live apart from OpenMeter's usage-ingest topics on
// purpose: usage is high volume and loss tolerant, billing is financial and
// must never be delayed behind a usage spike. That separation is a deployment
// decision, but the settings here — acks=all, long retention, keyed writes —
// are what make the billing lane trustworthy.
package kafkax

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/pymthouse/settlement/internal/config"
)

// Options are the connection settings shared by every client.
type Options = config.Kafka

func saslMechanism(o Options) (sasl.Mechanism, error) {
	switch o.SASLMechanism {
	case "":
		return nil, nil
	case "plain":
		return plain.Mechanism{Username: o.SASLUsername, Password: o.SASLPassword}, nil
	case "scram-sha-256":
		return scram.Mechanism(scram.SHA256, o.SASLUsername, o.SASLPassword)
	case "scram-sha-512":
		return scram.Mechanism(scram.SHA512, o.SASLUsername, o.SASLPassword)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", o.SASLMechanism)
	}
}

// Transport builds the shared transport used by writers and admin clients.
func Transport(o Options) (*kafka.Transport, error) {
	mech, err := saslMechanism(o)
	if err != nil {
		return nil, err
	}
	t := &kafka.Transport{DialTimeout: o.DialTimeout, SASL: mech}
	if o.TLSEnabled {
		t.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return t, nil
}

// Dialer builds the dialer used by group readers.
func Dialer(o Options) (*kafka.Dialer, error) {
	mech, err := saslMechanism(o)
	if err != nil {
		return nil, err
	}
	d := &kafka.Dialer{Timeout: o.DialTimeout, DualStack: true, SASLMechanism: mech}
	if o.TLSEnabled {
		d.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return d, nil
}

// Publisher writes messages to the billing topics.
//
// It is safe for concurrent use and batches internally; the doorman still
// blocks on the write so a 200 is only returned once the brokers have
// acknowledged the event. Accepting a webhook we failed to persist would mean
// silently losing a financial event, and Stripe would never redeliver it.
type Publisher struct {
	writer *kafka.Writer
}

// NewPublisher builds a synchronous, keyed publisher.
func NewPublisher(o Options) (*Publisher, error) {
	transport, err := Transport(o)
	if err != nil {
		return nil, err
	}
	return &Publisher{writer: &kafka.Writer{
		Addr: kafka.TCP(o.Brokers...),
		// Topic is set per message so one writer serves every billing topic.
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequiredAcks(o.RequiredAcks),
		Async:        false,
		WriteTimeout: o.WriteTimeout,
		Transport:    transport,
		// Auto-creation is off: billing topics are provisioned deliberately
		// with their partition count and retention, never by a typo.
		AllowAutoTopicCreation: false,
	}}, nil
}

// Publish writes one message and waits for the configured acks.
func (p *Publisher) Publish(ctx context.Context, msg kafka.Message) error {
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka publish to %s: %w", msg.Topic, err)
	}
	return nil
}

// Close flushes and releases the writer.
func (p *Publisher) Close() error { return p.writer.Close() }

// NewReader builds a consumer-group reader for one topic.
//
// Commits are manual: the worker commits only offsets whose work is durably
// finished, which is what lets it restart without reprocessing or skipping.
func NewReader(o Options, topic, startOffset string) (*kafka.Reader, error) {
	dialer, err := Dialer(o)
	if err != nil {
		return nil, err
	}
	offset := kafka.LastOffset
	if startOffset == "first" {
		offset = kafka.FirstOffset
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  o.Brokers,
		GroupID:  o.ConsumerGroup,
		Topic:    topic,
		Dialer:   dialer,
		MinBytes: 1,
		MaxBytes: 10 << 20,
		// CommitInterval 0 keeps commits synchronous and explicit.
		CommitInterval: 0,
		StartOffset:    offset,
		MaxWait:        time.Second,
	}), nil
}

// NewPartitionReader builds a reader bound to one partition and offset, with
// no consumer group. It is the replay path: point it at an offset and read the
// history back without disturbing the live group's committed position.
func NewPartitionReader(o Options, topic string, partition int, offset int64) (*kafka.Reader, error) {
	dialer, err := Dialer(o)
	if err != nil {
		return nil, err
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   o.Brokers,
		Topic:     topic,
		Partition: partition,
		Dialer:    dialer,
		MinBytes:  1,
		MaxBytes:  10 << 20,
		MaxWait:   time.Second,
	})
	if err := r.SetOffset(offset); err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("seek %s/%d to %d: %w", topic, partition, offset, err)
	}
	return r, nil
}

// TopicSpec describes a topic to provision.
type TopicSpec struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	// RetentionMs is kept long for billing: the log is the audit trail.
	RetentionMs int64
	// MinInSyncReplicas guards against acking a write that only one broker has.
	MinInSyncReplicas int
	// Compression is the broker-side codec, e.g. "producer" or "zstd".
	Compression string
}

// EnsureTopics creates any topic that does not exist yet. Existing topics are
// left untouched — widening a partition count would re-shard the keyspace and
// break per-customer ordering for in-flight invoices.
func EnsureTopics(ctx context.Context, o Options, specs []TopicSpec) error {
	transport, err := Transport(o)
	if err != nil {
		return err
	}
	client := &kafka.Client{Addr: kafka.TCP(o.Brokers...), Timeout: o.DialTimeout, Transport: transport}

	existing, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return fmt.Errorf("fetch metadata: %w", err)
	}
	have := make(map[string]bool, len(existing.Topics))
	for _, t := range existing.Topics {
		have[t.Name] = true
	}

	var create []kafka.TopicConfig
	for _, s := range specs {
		if have[s.Name] {
			continue
		}
		cfg := kafka.TopicConfig{
			Topic:             s.Name,
			NumPartitions:     s.Partitions,
			ReplicationFactor: s.ReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "retention.ms", ConfigValue: fmt.Sprint(s.RetentionMs)},
				{ConfigName: "cleanup.policy", ConfigValue: "delete"},
			},
		}
		if s.MinInSyncReplicas > 0 {
			cfg.ConfigEntries = append(cfg.ConfigEntries, kafka.ConfigEntry{
				ConfigName: "min.insync.replicas", ConfigValue: fmt.Sprint(s.MinInSyncReplicas),
			})
		}
		if s.Compression != "" {
			cfg.ConfigEntries = append(cfg.ConfigEntries, kafka.ConfigEntry{
				ConfigName: "compression.type", ConfigValue: s.Compression,
			})
		}
		create = append(create, cfg)
	}
	if len(create) == 0 {
		return nil
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: create})
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for name, tErr := range resp.Errors {
		if tErr != nil {
			return fmt.Errorf("create topic %s: %w", name, tErr)
		}
	}
	return nil
}

// Header returns the value of the named header, or "" when absent.
func Header(msg kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
