// Command settlementctl is the operator's tool for the billing lane: it
// provisions topics, replays history after a fix, redrives the dead-letter
// queue, and dumps raw events for an audit.
//
// It exists because the Kafka log *is* the record. When a mapping bug ships,
// the events that were mishandled are still there — the recovery is to read
// them back and run them through the corrected code, which is something a
// consume-and-delete queue could never offer.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/events"
	"github.com/pymthouse/settlement/internal/kafkax"
	"github.com/pymthouse/settlement/internal/ops"
)

const usage = `settlementctl — operate the settlement billing lane

Usage:
  settlementctl topics ensure [flags]     provision the billing topics
  settlementctl replay [flags]            re-publish history so the worker reprocesses it
  settlementctl dlq redrive [flags]       re-publish dead-lettered messages to their source topic
  settlementctl inspect [flags]           print raw events from a topic partition

Connection settings come from the same SETTLEMENT_KAFKA_* environment
variables the worker uses. Run a subcommand with -h for its flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "topics":
		err = topicsCommand(ctx, os.Args[2:])
	case "replay":
		err = replayCommand(ctx, os.Args[2:])
	case "dlq":
		err = dlqCommand(ctx, os.Args[2:])
	case "inspect":
		err = inspectCommand(ctx, os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// kafkaOptions reads the connection settings the worker uses, so the CLI can
// never talk to a different cluster than the service it is fixing.
func kafkaOptions() (config.Kafka, error) {
	cfg, err := config.LoadProducer()
	if err != nil {
		// Only the Kafka half matters here; missing webhook secrets are
		// irrelevant to an operator replaying a topic.
		k := cfg.Kafka
		if len(k.Brokers) == 0 || k.TopicStripe == "" || k.TopicOpenMeter == "" || k.TopicDLQ == "" || k.TopicCollectRequest == "" {
			return config.Kafka{}, err
		}
	}
	return cfg.Kafka, nil
}

func topicsCommand(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "ensure" {
		return errors.New("usage: settlementctl topics ensure [flags]")
	}

	fs := flag.NewFlagSet("topics ensure", flag.ExitOnError)
	partitions := fs.Int("partitions", 12, "partitions per billing topic")
	replication := fs.Int("replication", 1, "replication factor")
	retentionDays := fs.Int("retention-days", 3650, "retention in days for the billing topics")
	minISR := fs.Int("min-isr", 1, "min.insync.replicas")
	compression := fs.String("compression", "producer", "broker compression.type")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	opts, err := kafkaOptions()
	if err != nil {
		return err
	}

	retentionMs := int64(*retentionDays) * 24 * int64(time.Hour/time.Millisecond)
	specs := []kafkax.TopicSpec{
		{Name: opts.TopicOpenMeter, Partitions: *partitions, ReplicationFactor: *replication, RetentionMs: retentionMs, MinInSyncReplicas: *minISR, Compression: *compression},
		{Name: opts.TopicStripe, Partitions: *partitions, ReplicationFactor: *replication, RetentionMs: retentionMs, MinInSyncReplicas: *minISR, Compression: *compression},
		{Name: opts.TopicDLQ, Partitions: *partitions, ReplicationFactor: *replication, RetentionMs: retentionMs, MinInSyncReplicas: *minISR, Compression: *compression},
		{Name: opts.TopicCollectRequest, Partitions: *partitions, ReplicationFactor: *replication, RetentionMs: retentionMs, MinInSyncReplicas: *minISR, Compression: *compression},
	}

	if err := kafkax.EnsureTopics(ctx, opts, specs); err != nil {
		return err
	}
	for _, spec := range specs {
		fmt.Printf("ok  %s  partitions=%d replication=%d retention=%dd\n",
			spec.Name, spec.Partitions, spec.ReplicationFactor, *retentionDays)
	}
	return nil
}

func replayCommand(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	topic := fs.String("topic", "", "source topic to replay (required)")
	partition := fs.Int("partition", 0, "partition to replay")
	offset := fs.Int64("offset", kafka.FirstOffset, "offset to start from")
	since := fs.String("since", "", "start at the first offset at or after this RFC3339 time (overrides -offset)")
	count := fs.Int("count", 0, "stop after this many messages (0 = until the end of the log)")
	batch := fs.String("batch", "", "replay batch id; defaults to a timestamp")
	dryRun := fs.Bool("dry-run", false, "print what would be replayed without publishing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("-topic is required")
	}

	opts, err := kafkaOptions()
	if err != nil {
		return err
	}

	in := ops.ReplayInput{
		Topic:     *topic,
		Partition: *partition,
		Offset:    *offset,
		Count:     *count,
		BatchID:   *batch,
		DryRun:    *dryRun,
		OnMessage: func(msg kafka.Message) {
			fmt.Printf("  offset=%d key=%s event=%s type=%s\n",
				msg.Offset, string(msg.Key),
				kafkax.Header(msg, events.HeaderEventID),
				kafkax.Header(msg, events.HeaderEventType))
		},
	}
	if *since != "" {
		at, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return fmt.Errorf("parse -since: %w", err)
		}
		in.Since = at
	}

	fmt.Printf("replaying %s/%d (batch pending, dry-run=%v)\n", *topic, *partition, *dryRun)

	result, err := ops.Replay(ctx, opts, in)
	if err != nil {
		return err
	}

	fmt.Printf("replaying from offset %d (batch %s)\n", result.Start, result.BatchID)
	fmt.Printf("replayed %d message(s)\n", result.Published)
	if !*dryRun {
		fmt.Printf("the worker will reprocess them: the batch id %q bypasses deduplication\n", result.BatchID)
	}
	return nil
}

func dlqCommand(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "redrive" {
		return errors.New("usage: settlementctl dlq redrive [flags]")
	}

	fs := flag.NewFlagSet("dlq redrive", flag.ExitOnError)
	partition := fs.Int("partition", 0, "DLQ partition to redrive")
	offset := fs.Int64("offset", kafka.FirstOffset, "offset to start from (default: the beginning)")
	count := fs.Int("count", 0, "stop after this many messages (0 = all)")
	reason := fs.String("reason", "", "only redrive messages parked with this reason")
	batch := fs.String("batch", "", "redrive batch id; defaults to a timestamp")
	dryRun := fs.Bool("dry-run", false, "print what would be redriven without publishing")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	opts, err := kafkaOptions()
	if err != nil {
		return err
	}

	fmt.Printf("redriving %s/%d (dry-run=%v)\n", opts.TopicDLQ, *partition, *dryRun)

	result, err := ops.Redrive(ctx, opts, ops.RedriveInput{
		Partition: *partition,
		Offset:    *offset,
		Count:     *count,
		Reason:    *reason,
		BatchID:   *batch,
		DryRun:    *dryRun,
		OnMessage: func(msg kafka.Message, parkedReason, sourceTopic string) {
			fmt.Printf("  offset=%d reason=%s -> %s (event %s)\n",
				msg.Offset, parkedReason, sourceTopic, kafkax.Header(msg, events.HeaderEventID))
		},
		OnSkip: func(msg kafka.Message, why string) {
			fmt.Printf("  skip offset=%d: %s\n", msg.Offset, why)
		},
	})
	if err != nil {
		return err
	}

	fmt.Printf("redriven %d, skipped %d (batch %s)\n", result.Published, result.Skipped, result.BatchID)
	return nil
}

func inspectCommand(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	topic := fs.String("topic", "", "topic to read (required)")
	partition := fs.Int("partition", 0, "partition to read")
	offset := fs.Int64("offset", kafka.LastOffset, "offset to start from")
	count := fs.Int("count", 10, "messages to print")
	full := fs.Bool("full", false, "print the whole raw body instead of a summary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("-topic is required")
	}

	opts, err := kafkaOptions()
	if err != nil {
		return err
	}

	records, err := ops.Inspect(ctx, opts, ops.InspectInput{
		Topic:     *topic,
		Partition: *partition,
		Offset:    *offset,
		Count:     *count,
		Full:      *full,
	})
	if err != nil {
		return err
	}

	for _, rec := range records {
		encoded, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Println(string(encoded))
	}
	return nil
}
