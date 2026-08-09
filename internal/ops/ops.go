// Package ops holds the Kafka operator actions shared by settlementctl and the
// worker admin console: inspect, replay, and DLQ redrive.
//
// Stopping drains at the starting high watermark is the safety property. A
// redrive that tailed the log would race the worker and amplify failures.
package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/events"
	"github.com/pymthouse/settlement/internal/kafkax"
)

// InspectRecord is one message summary returned by Inspect.
type InspectRecord struct {
	Offset    int64           `json:"offset"`
	Partition int             `json:"partition"`
	Time      string          `json:"time"`
	Key       string          `json:"key"`
	Source    string          `json:"source,omitempty"`
	EventID   string          `json:"event_id,omitempty"`
	EventType string          `json:"event_type,omitempty"`
	Bytes     int             `json:"bytes"`
	DLQReason string          `json:"dlq_reason,omitempty"`
	DLQError  string          `json:"dlq_error,omitempty"`
	DLQTopic  string          `json:"dlq_topic,omitempty"`
	Body      json.RawMessage `json:"body,omitempty"`
	BodyText  string          `json:"body_text,omitempty"`
}

// InspectInput selects messages to read from a partition.
type InspectInput struct {
	Topic     string
	Partition int
	Offset    int64
	Count     int
	Full      bool
}

// Inspect reads up to Count messages from a topic partition.
func Inspect(ctx context.Context, opts config.Kafka, in InspectInput) ([]InspectRecord, error) {
	if in.Topic == "" {
		return nil, errors.New("topic is required")
	}
	if in.Count <= 0 {
		in.Count = 10
	}

	reader, err := kafkax.NewPartitionReader(opts, in.Topic, in.Partition, in.Offset)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	out := make([]InspectRecord, 0, in.Count)
	for printed := 0; printed < in.Count; printed++ {
		msg, stop, err := readNext(ctx, reader)
		if err != nil {
			return out, err
		}
		if stop != readContinue {
			break
		}
		out = append(out, inspectRecordFromMessage(msg, in.Full))
	}
	return out, nil
}

func inspectRecordFromMessage(msg kafka.Message, full bool) InspectRecord {
	rec := InspectRecord{
		Offset:    msg.Offset,
		Partition: msg.Partition,
		Time:      msg.Time.UTC().Format(time.RFC3339Nano),
		Key:       string(msg.Key),
		Source:    kafkax.Header(msg, events.HeaderSource),
		EventID:   kafkax.Header(msg, events.HeaderEventID),
		EventType: kafkax.Header(msg, events.HeaderEventType),
		Bytes:     len(msg.Value),
		DLQReason: kafkax.Header(msg, events.HeaderDLQReason),
		DLQError:  kafkax.Header(msg, events.HeaderDLQError),
		DLQTopic:  kafkax.Header(msg, events.HeaderDLQTopic),
	}
	if full && len(msg.Value) > 0 {
		if json.Valid(msg.Value) {
			rec.Body = json.RawMessage(append([]byte(nil), msg.Value...))
		} else {
			rec.BodyText = string(msg.Value)
		}
	}
	return rec
}

// ReplayInput describes a bounded replay of a source topic partition.
type ReplayInput struct {
	Topic     string
	Partition int
	Offset    int64
	Since     time.Time // zero = use Offset
	Count     int
	BatchID   string
	DryRun    bool
	// OnMessage is optional progress callback (CLI printing).
	OnMessage func(msg kafka.Message)
}

// ReplayResult is the outcome of a replay pass.
type ReplayResult struct {
	BatchID   string
	Published int
	Skipped   int
	Start     int64
}

// Replay republishes messages so the worker reprocesses them (batch id bypasses dedupe).
func Replay(ctx context.Context, opts config.Kafka, in ReplayInput) (ReplayResult, error) {
	if in.Topic == "" {
		return ReplayResult{}, errors.New("topic is required")
	}
	batchID := in.BatchID
	if batchID == "" {
		batchID = time.Now().UTC().Format("20060102T150405Z")
	}
	start := in.Offset
	if !in.Since.IsZero() {
		var err error
		start, err = offsetAt(ctx, opts, in.Topic, in.Partition, in.Since)
		if err != nil {
			return ReplayResult{}, err
		}
	}

	published, skipped, err := drain(ctx, drainRequest{
		opts:      opts,
		topic:     in.Topic,
		partition: in.Partition,
		start:     start,
		limit:     in.Count,
		batchID:   batchID,
		dryRun:    in.DryRun,
		route: func(msg kafka.Message) (string, bool) {
			if in.OnMessage != nil {
				in.OnMessage(msg)
			}
			return msg.Topic, true
		},
	})
	return ReplayResult{BatchID: batchID, Published: published, Skipped: skipped, Start: start}, err
}

// RedriveInput describes a bounded DLQ redrive.
type RedriveInput struct {
	Partition int
	Offset    int64
	Count     int
	Reason    string // empty = all reasons
	BatchID   string
	DryRun    bool
	OnMessage func(msg kafka.Message, reason, sourceTopic string)
	OnSkip    func(msg kafka.Message, why string)
}

// RedriveResult is the outcome of a DLQ redrive.
type RedriveResult struct {
	BatchID   string
	Published int
	Skipped   int
}

// Redrive republishes DLQ messages back to their source topics.
func Redrive(ctx context.Context, opts config.Kafka, in RedriveInput) (RedriveResult, error) {
	batchID := in.BatchID
	if batchID == "" {
		batchID = "redrive-" + time.Now().UTC().Format("20060102T150405Z")
	}
	start := in.Offset
	if start == 0 {
		start = kafka.FirstOffset
	}

	published, skipped, err := drain(ctx, drainRequest{
		opts:      opts,
		topic:     opts.TopicDLQ,
		partition: in.Partition,
		start:     start,
		limit:     in.Count,
		batchID:   batchID,
		dryRun:    in.DryRun,
		route: func(msg kafka.Message) (string, bool) {
			parkedReason := kafkax.Header(msg, events.HeaderDLQReason)
			if in.Reason != "" && parkedReason != in.Reason {
				if in.OnSkip != nil {
					in.OnSkip(msg, "reason filter")
				}
				return "", false
			}
			sourceTopic := kafkax.Header(msg, events.HeaderDLQTopic)
			if sourceTopic == "" {
				if in.OnSkip != nil {
					in.OnSkip(msg, "no source topic header")
				}
				return "", false
			}
			if in.OnMessage != nil {
				in.OnMessage(msg, parkedReason, sourceTopic)
			}
			return sourceTopic, true
		},
	})
	return RedriveResult{BatchID: batchID, Published: published, Skipped: skipped}, err
}

// FirstOffset / LastOffset re-export kafka-go constants for callers.
const (
	FirstOffset = kafka.FirstOffset
	LastOffset  = kafka.LastOffset
)

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

type drainRequest struct {
	opts      config.Kafka
	topic     string
	partition int
	start     int64
	limit     int
	batchID   string
	dryRun    bool
	route     func(kafka.Message) (topic string, send bool)
}

func drain(ctx context.Context, req drainRequest) (published, skipped int, err error) {
	end, err := highWatermark(ctx, req.opts, req.topic, req.partition)
	if err != nil {
		return 0, 0, err
	}
	if end == 0 {
		return 0, 0, nil
	}

	reader, err := kafkax.NewPartitionReader(req.opts, req.topic, req.partition, req.start)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = reader.Close() }()

	publisher, err := maybePublisher(req)
	if err != nil {
		return 0, 0, err
	}
	if publisher != nil {
		defer func() { _ = publisher.Close() }()
	}

	lastOffset := req.start - 1
	for req.limit == 0 || published < req.limit {
		msg, stop, readErr := readNext(ctx, reader)
		if readErr != nil {
			return published, skipped, fmt.Errorf("read %s/%d: %w", req.topic, req.partition, readErr)
		}
		if stop != readContinue {
			return published, skipped, drainStopError(req, stop, lastOffset, end)
		}

		lastOffset = msg.Offset
		pub, skip, err := applyDrainMessage(ctx, req, publisher, msg)
		if err != nil {
			return published, skipped, err
		}
		published += pub
		skipped += skip

		if msg.Offset >= end-1 {
			break
		}
	}
	return published, skipped, nil
}

func maybePublisher(req drainRequest) (*kafkax.Publisher, error) {
	if req.dryRun {
		return nil, nil
	}
	return kafkax.NewPublisher(req.opts)
}

func drainStopError(req drainRequest, stop readStop, lastOffset, end int64) error {
	if lastOffset < end-1 {
		return fmt.Errorf(
			"incomplete drain of %s/%d: stopped (%s) at offset %d before end %d",
			req.topic, req.partition, stop, lastOffset, end-1)
	}
	return nil
}

func applyDrainMessage(ctx context.Context, req drainRequest, publisher *kafkax.Publisher, msg kafka.Message) (published, skipped int, err error) {
	target, send := req.route(msg)
	if !send {
		return 0, 1, nil
	}
	if publisher != nil {
		if err := publisher.Publish(ctx, replayMessage(msg, target, req.batchID)); err != nil {
			return 0, 0, err
		}
	}
	return 1, 0, nil
}

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

func offsetAt(ctx context.Context, opts config.Kafka, topic string, partition int, at time.Time) (int64, error) {
	return withLeader(ctx, opts, topic, partition, func(conn *kafka.Conn) (int64, error) {
		offset, err := conn.ReadOffset(at)
		if err != nil {
			return 0, fmt.Errorf("resolve offset at %s: %w", at.Format(time.RFC3339), err)
		}
		return offset, nil
	})
}

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
