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
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/events"
	"github.com/pymthouse/settlement/internal/kafkax"
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
		if len(k.Brokers) == 0 || k.TopicStripe == "" || k.TopicOpenMeter == "" || k.TopicDLQ == "" {
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

	batchID := *batch
	if batchID == "" {
		batchID = time.Now().UTC().Format("20060102T150405Z")
	}

	start := *offset
	if *since != "" {
		at, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return fmt.Errorf("parse -since: %w", err)
		}
		start, err = offsetAt(ctx, opts, *topic, *partition, at)
		if err != nil {
			return err
		}
		fmt.Printf("resolved %s to offset %d on %s/%d\n", *since, start, *topic, *partition)
	}

	fmt.Printf("replaying %s/%d from offset %d (batch %s, dry-run=%v)\n",
		*topic, *partition, start, batchID, *dryRun)

	replayed, _, err := drain(ctx, drainRequest{
		opts:      opts,
		topic:     *topic,
		partition: *partition,
		start:     start,
		limit:     *count,
		batchID:   batchID,
		dryRun:    *dryRun,
		route: func(msg kafka.Message) (string, bool) {
			fmt.Printf("  offset=%d key=%s event=%s type=%s\n",
				msg.Offset, string(msg.Key),
				kafkax.Header(msg, events.HeaderEventID),
				kafkax.Header(msg, events.HeaderEventType))
			return msg.Topic, true
		},
	})
	if err != nil {
		return err
	}

	fmt.Printf("replayed %d message(s)\n", replayed)
	if !*dryRun {
		fmt.Printf("the worker will reprocess them: the batch id %q bypasses deduplication\n", batchID)
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

	batchID := *batch
	if batchID == "" {
		batchID = "redrive-" + time.Now().UTC().Format("20060102T150405Z")
	}

	fmt.Printf("redriving %s/%d (batch %s, dry-run=%v)\n", opts.TopicDLQ, *partition, batchID, *dryRun)

	redriven, skipped, err := drain(ctx, drainRequest{
		opts:      opts,
		topic:     opts.TopicDLQ,
		partition: *partition,
		start:     *offset,
		limit:     *count,
		batchID:   batchID,
		dryRun:    *dryRun,
		route: func(msg kafka.Message) (string, bool) {
			parkedReason := kafkax.Header(msg, events.HeaderDLQReason)
			if *reason != "" && parkedReason != *reason {
				return "", false
			}

			// Send it back where it came from, so the same handler picks it up.
			sourceTopic := kafkax.Header(msg, events.HeaderDLQTopic)
			if sourceTopic == "" {
				fmt.Printf("  skip offset=%d: no source topic header\n", msg.Offset)
				return "", false
			}

			fmt.Printf("  offset=%d reason=%s -> %s (event %s)\n",
				msg.Offset, parkedReason, sourceTopic, kafkax.Header(msg, events.HeaderEventID))
			return sourceTopic, true
		},
	})
	if err != nil {
		return err
	}

	fmt.Printf("redriven %d, skipped %d\n", redriven, skipped)
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

	reader, err := kafkax.NewPartitionReader(opts, *topic, *partition, *offset)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	for printed := 0; printed < *count; printed++ {
		msg, stop, err := readNext(ctx, reader)
		if err != nil {
			return err
		}
		if stop != readContinue {
			break
		}

		record := map[string]any{
			"offset":     msg.Offset,
			"partition":  msg.Partition,
			"time":       msg.Time.UTC().Format(time.RFC3339Nano),
			"key":        string(msg.Key),
			"source":     kafkax.Header(msg, events.HeaderSource),
			"event_id":   kafkax.Header(msg, events.HeaderEventID),
			"event_type": kafkax.Header(msg, events.HeaderEventType),
			"bytes":      len(msg.Value),
		}
		if reason := kafkax.Header(msg, events.HeaderDLQReason); reason != "" {
			record["dlq_reason"] = reason
			record["dlq_error"] = kafkax.Header(msg, events.HeaderDLQError)
		}
		if *full {
			// Prefer embedding as JSON when the body is valid; otherwise keep
			// the raw bytes as a string so inspect can still print the record.
			if len(msg.Value) > 0 && json.Valid(msg.Value) {
				record["body"] = json.RawMessage(msg.Value)
			} else {
				record["body"] = string(msg.Value)
			}
		}

		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		fmt.Println(string(encoded))
	}
	return nil
}

// readStop explains why readNext returned without a message.
type readStop int

const (
	readContinue readStop = iota
	readEndOfLog
	readCanceled
)

func (s readStop) String() string {
	switch s {
	case readEndOfLog:
		return "end of log"
	case readCanceled:
		return "canceled"
	default:
		return "ok"
	}
}

// readNext reads one message. A distinct stop reason separates end-of-log
// (read timeout with no message), operator cancellation, and real failures.
// The read is bounded so a gap in the log exits rather than blocking a
// terminal indefinitely.
func readNext(ctx context.Context, reader *kafka.Reader) (msg kafka.Message, stop readStop, err error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg, err = reader.ReadMessage(readCtx)
	if err != nil {
		if ctx.Err() != nil {
			return kafka.Message{}, readCanceled, nil
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return kafka.Message{}, readEndOfLog, nil
		}
		return kafka.Message{}, readContinue, err
	}
	return msg, readContinue, nil
}

// drainRequest describes one bounded read-and-republish pass over a partition.
type drainRequest struct {
	opts      config.Kafka
	topic     string
	partition int
	start     int64
	// limit stops after this many republished messages; 0 means no limit.
	limit   int
	batchID string
	dryRun  bool
	// route decides where a message goes, and whether to send it at all. It is
	// also where per-command output is printed.
	route func(kafka.Message) (topic string, send bool)
}

// drain reads a partition from start up to the offsets that existed when it
// began, republishing what route selects.
//
// Stopping at the starting high watermark is the safety property. A redrive
// that tailed the log would race the worker: messages it republishes can fail
// again and land back on the DLQ behind the reader, which would redrive them a
// second time, and a third — an amplification loop that turns two parked
// events into hundreds. "Republish what is there now" is the only safe reading
// of either command.
func drain(ctx context.Context, req drainRequest) (published, skipped int, err error) {
	end, err := highWatermark(ctx, req.opts, req.topic, req.partition)
	if err != nil {
		return 0, 0, err
	}
	if end == 0 {
		fmt.Printf("  %s/%d is empty\n", req.topic, req.partition)
		return 0, 0, nil
	}

	reader, err := kafkax.NewPartitionReader(req.opts, req.topic, req.partition, req.start)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = reader.Close() }()

	var publisher *kafkax.Publisher
	if !req.dryRun {
		publisher, err = kafkax.NewPublisher(req.opts)
		if err != nil {
			return 0, 0, err
		}
		defer func() { _ = publisher.Close() }()
	}

	lastOffset := req.start - 1
	for req.limit == 0 || published < req.limit {
		msg, stop, readErr := readNext(ctx, reader)
		if readErr != nil {
			return published, skipped, fmt.Errorf("read %s/%d: %w", req.topic, req.partition, readErr)
		}
		if stop != readContinue {
			if lastOffset < end-1 {
				return published, skipped, fmt.Errorf(
					"incomplete drain of %s/%d: stopped (%s) at offset %d before end %d",
					req.topic, req.partition, stop, lastOffset, end-1)
			}
			break
		}

		lastOffset = msg.Offset
		target, send := req.route(msg)
		if send {
			if !req.dryRun {
				if err := publisher.Publish(ctx, replayMessage(msg, target, req.batchID)); err != nil {
					return published, skipped, err
				}
			}
			published++
		} else {
			skipped++
		}

		if msg.Offset >= end-1 {
			break
		}
	}
	return published, skipped, nil
}

// replayMessage rebuilds a message for republishing, stamped with the batch id
// that tells the worker to process it again rather than treat it as a
// duplicate. DLQ bookkeeping headers are dropped so the message arrives as the
// event it originally was.
func replayMessage(msg kafka.Message, topic, batchID string) kafka.Message {
	headers := make([]kafka.Header, 0, len(msg.Headers)+1)
	for _, h := range msg.Headers {
		if strings.HasPrefix(h.Key, "settlement-dlq-") {
			continue
		}
		if h.Key == events.HeaderReplayOf {
			continue
		}
		headers = append(headers, h)
	}
	headers = append(headers, kafka.Header{Key: events.HeaderReplayOf, Value: []byte(batchID)})

	return kafka.Message{
		Topic:   topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}
}

// offsetAt resolves a wall-clock time to the first offset at or after it.
func offsetAt(ctx context.Context, opts config.Kafka, topic string, partition int, at time.Time) (int64, error) {
	return withLeader(ctx, opts, topic, partition, func(conn *kafka.Conn) (int64, error) {
		offset, err := conn.ReadOffset(at)
		if err != nil {
			return 0, fmt.Errorf("resolve offset at %s: %w", at.Format(time.RFC3339), err)
		}
		return offset, nil
	})
}

// highWatermark returns the offset one past the last message currently on the
// partition.
//
// Every read command stops here rather than tailing. Without that bound a
// redrive races the worker: messages it republishes fail again, land back on
// the DLQ behind the reader, and get redriven a second time — an amplification
// loop that turns two parked events into hundreds. "Redrive what is parked
// now" is the only safe reading of the command.
func highWatermark(ctx context.Context, opts config.Kafka, topic string, partition int) (int64, error) {
	return withLeader(ctx, opts, topic, partition, func(conn *kafka.Conn) (int64, error) {
		_, last, err := conn.ReadOffsets()
		if err != nil {
			return 0, fmt.Errorf("read offsets for %s/%d: %w", topic, partition, err)
		}
		return last, nil
	})
}

func withLeader(ctx context.Context, opts config.Kafka, topic string, partition int, fn func(*kafka.Conn) (int64, error)) (int64, error) {
	dialer, err := kafkax.Dialer(opts)
	if err != nil {
		return 0, err
	}

	conn, err := dialer.DialLeader(ctx, "tcp", opts.Brokers[0], topic, partition)
	if err != nil {
		return 0, fmt.Errorf("dial leader for %s/%d: %w", topic, partition, err)
	}
	defer func() { _ = conn.Close() }()

	return fn(conn)
}
