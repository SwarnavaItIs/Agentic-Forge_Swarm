// Package broker implements one broker only: file-backed NATS JetStream.
// One replica survives process restart with retained storage, not node/disk loss.
package broker

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"forgeswarm/internal/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"net/url"
	"os"
	"time"
)

const Stream = "FORGESWARM_TASKS"
const Subject = "forgeswarm.dispatch.v1"
const Quarantine = "FORGESWARM_QUARANTINE"

type Broker struct {
	Conn     *nats.Conn
	JS       jetstream.JetStream
	Consumer jetstream.Consumer
}

func Open(ctx context.Context, c config.Distributed) (*Broker, error) {
	u, err := url.Parse(c.BrokerURL)
	if err != nil || u.User != nil || len(c.NATSToken) < 32 {
		return nil, errors.New("configured broker credential required")
	}
	opts := []nats.Option{nats.Token(c.NATSToken), nats.Timeout(2 * time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(200 * time.Millisecond), nats.ReconnectBufSize(0), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {})}
	if u.Scheme == "tls" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if c.NATSCA != "" {
			p, err := os.ReadFile(c.NATSCA)
			if err != nil || !roots.AppendCertsFromPEM(p) {
				return nil, errors.New("invalid broker root certificate")
			}
		}
		opts = append(opts, nats.Secure(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}))
	} else if !c.InsecureLocal || !config.Loopback(u.Host) {
		return nil, errors.New("plaintext broker must use explicit loopback test mode")
	}
	nc, err := nats.Connect(c.BrokerURL, opts...)
	if err != nil {
		return nil, errors.New("broker_unavailable")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	for _, cfg := range []jetstream.StreamConfig{{Name: Stream, Subjects: []string{Subject}, Storage: jetstream.FileStorage, Replicas: 1, MaxMsgs: 100000, MaxBytes: 64 << 20, MaxMsgSize: 4096, MaxAge: 24 * time.Hour, Duplicates: 2 * time.Minute}, {Name: Quarantine, Subjects: []string{"forgeswarm.quarantine.v1"}, Storage: jetstream.FileStorage, Replicas: 1, MaxMsgs: 10000, MaxBytes: 16 << 20, MaxMsgSize: 1024, MaxAge: 7 * 24 * time.Hour}} {
		if _, err = js.CreateOrUpdateStream(ctx, cfg); err != nil {
			nc.Close()
			return nil, errors.New("broker_stream_setup_failed")
		}
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, Stream, jetstream.ConsumerConfig{Durable: "DIAGNOSTIC_WORKERS", FilterSubject: Subject, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 2 * time.Second, MaxDeliver: -1, MaxAckPending: 1000, MaxWaiting: 128, MaxRequestBatch: 100})
	if err != nil {
		nc.Close()
		return nil, errors.New("broker_consumer_setup_failed")
	}
	return &Broker{nc, js, consumer}, nil
}
func (b *Broker) Close() { b.Conn.Close() }
func (b *Broker) Publish(ctx context.Context, id string, body []byte) error {
	if len(body) > 4096 {
		return errors.New("dispatch_too_large")
	}
	_, err := b.JS.Publish(ctx, Subject, body, jetstream.WithMsgID(id))
	if err != nil {
		return errors.New("broker_publish_unavailable")
	}
	return nil
}
func (b *Broker) QuarantineMessage(ctx context.Context, msg jetstream.Msg) error {
	// No raw payloads: unknown messages may contain secrets. Preserve only a
	// digest, length and original stream sequence for bounded operator diagnosis.
	metadata, err := msg.Metadata()
	if err != nil {
		return err
	}
	digest := sha256.Sum256(msg.Data())
	body, _ := json.Marshal(map[string]any{"schema_version": 1, "reason": "malformed_or_unsupported", "sha256": hex.EncodeToString(digest[:]), "size": len(msg.Data()), "stream_sequence": metadata.Sequence.Stream})
	_, err = b.JS.Publish(ctx, "forgeswarm.quarantine.v1", body, jetstream.WithMsgID("quarantine:"+hex.EncodeToString(digest[:])))
	if err != nil {
		return err
	}
	return msg.DoubleAck(ctx)
}
