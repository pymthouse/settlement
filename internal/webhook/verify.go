// Package webhook verifies inbound webhook signatures against the raw request
// body.
//
// Every function here takes []byte, never an already-decoded struct. Signatures
// are computed over the exact bytes the sender hashed, and Go's JSON round-trip
// reorders keys, rewrites numbers and drops insignificant whitespace — any one
// of which turns a valid signature into a rejected one. Read the body once,
// verify those bytes, publish those bytes.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Verification failures. They are deliberately coarse: a caller must not be
// able to learn *why* a forged signature failed.
var (
	// ErrNoSignature means the required signature header was absent or empty.
	ErrNoSignature = errors.New("missing signature header")
	// ErrInvalidHeader means the header was present but malformed.
	ErrInvalidHeader = errors.New("malformed signature header")
	// ErrTooOld means the signed timestamp fell outside the tolerance window.
	ErrTooOld = errors.New("signature timestamp outside tolerance")
	// ErrNoMatch means no configured secret produced the presented signature.
	ErrNoMatch = errors.New("no configured secret matches the signature")
	// ErrNoSecrets means the verifier was constructed without any secret.
	ErrNoSecrets = errors.New("no signing secrets configured")
)

// StripeVerifier checks the `Stripe-Signature` header.
//
// Stripe signs "<timestamp>.<raw body>" with HMAC-SHA256, using the webhook
// secret as the raw key, and presents the result hex-encoded in one or more
// `v1=` fields. Several secrets may be configured so an endpoint secret can be
// rotated without a delivery gap.
type StripeVerifier struct {
	secrets   []string
	tolerance time.Duration
	now       func() time.Time
}

// NewStripeVerifier builds a verifier. toleranceSeconds bounds clock skew and
// caps how long a captured request stays replayable; Stripe's own libraries
// default to 300 seconds.
func NewStripeVerifier(secrets []string, toleranceSeconds int64) *StripeVerifier {
	if toleranceSeconds <= 0 {
		toleranceSeconds = 300
	}
	return &StripeVerifier{
		secrets:   nonEmpty(secrets),
		tolerance: time.Duration(toleranceSeconds) * time.Second,
		now:       time.Now,
	}
}

// Enabled reports whether any secret is configured.
func (v *StripeVerifier) Enabled() bool { return len(v.secrets) > 0 }

// Verify checks header against body. It returns nil only when a configured
// secret reproduces one of the presented signatures within the tolerance.
func (v *StripeVerifier) Verify(header string, body []byte) error {
	if len(v.secrets) == 0 {
		return ErrNoSecrets
	}
	header = strings.TrimSpace(header)
	if header == "" {
		return ErrNoSignature
	}

	var timestamp string
	var presented []string
	for _, part := range strings.Split(header, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch key {
		case "t":
			timestamp = value
		case "v1":
			presented = append(presented, value)
		}
	}
	if timestamp == "" || len(presented) == 0 {
		return ErrInvalidHeader
	}

	secs, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrInvalidHeader
	}
	if err := v.checkAge(time.Unix(secs, 0)); err != nil {
		return err
	}

	signed := make([]byte, 0, len(timestamp)+1+len(body))
	signed = append(signed, timestamp...)
	signed = append(signed, '.')
	signed = append(signed, body...)

	for _, secret := range v.secrets {
		want := hmacSHA256([]byte(secret), signed)
		for _, got := range presented {
			raw, err := hex.DecodeString(strings.TrimSpace(got))
			if err != nil {
				continue
			}
			if hmac.Equal(raw, want) {
				return nil
			}
		}
	}
	return ErrNoMatch
}

func (v *StripeVerifier) checkAge(signedAt time.Time) error {
	drift := v.now().Sub(signedAt)
	if drift < 0 {
		drift = -drift
	}
	if drift > v.tolerance {
		return fmt.Errorf("%w: drift %s", ErrTooOld, drift.Round(time.Second))
	}
	return nil
}

// StandardVerifier checks Standard Webhooks / Svix signatures, the scheme
// OpenMeter uses for notification channels.
//
// The signed content is "<webhook-id>.<webhook-timestamp>.<raw body>", the key
// is the base64 payload of a `whsec_`-prefixed secret, and the signature header
// carries one or more space-separated `v1,<base64>` entries.
type StandardVerifier struct {
	secrets   [][]byte
	tolerance time.Duration
	now       func() time.Time
}

// NewStandardVerifier decodes the configured secrets. A secret that is not
// valid base64 after the optional `whsec_` prefix is used as raw bytes, which
// is what self-hosted OpenMeter deployments configured by hand tend to have.
func NewStandardVerifier(secrets []string, toleranceSeconds int64) *StandardVerifier {
	if toleranceSeconds <= 0 {
		toleranceSeconds = 300
	}
	v := &StandardVerifier{
		tolerance: time.Duration(toleranceSeconds) * time.Second,
		now:       time.Now,
	}
	for _, s := range nonEmpty(secrets) {
		trimmed := strings.TrimPrefix(strings.TrimSpace(s), "whsec_")
		if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil && len(decoded) > 0 {
			// Accept both interpretations: the secret may be the raw base64
			// string a self-hosted deploy typed in, or the decoded key bytes
			// Svix/Standard Webhooks expect.
			v.secrets = append(v.secrets, decoded)
			v.secrets = append(v.secrets, []byte(trimmed))
			continue
		}
		v.secrets = append(v.secrets, []byte(trimmed))
	}
	return v
}

// Enabled reports whether any secret is configured.
func (v *StandardVerifier) Enabled() bool { return len(v.secrets) > 0 }

// Verify checks the id/timestamp/signature triple against body.
func (v *StandardVerifier) Verify(id, timestamp, signature string, body []byte) error {
	if len(v.secrets) == 0 {
		return ErrNoSecrets
	}
	id, timestamp, signature = strings.TrimSpace(id), strings.TrimSpace(timestamp), strings.TrimSpace(signature)
	if signature == "" {
		return ErrNoSignature
	}
	if id == "" || timestamp == "" {
		return ErrInvalidHeader
	}

	secs, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrInvalidHeader
	}
	drift := v.now().Sub(time.Unix(secs, 0))
	if drift < 0 {
		drift = -drift
	}
	if drift > v.tolerance {
		return fmt.Errorf("%w: drift %s", ErrTooOld, drift.Round(time.Second))
	}

	signed := make([]byte, 0, len(id)+len(timestamp)+2+len(body))
	signed = append(signed, id...)
	signed = append(signed, '.')
	signed = append(signed, timestamp...)
	signed = append(signed, '.')
	signed = append(signed, body...)

	// The header holds space-separated "<version>,<base64>" entries; only v1
	// is defined today and unknown versions are ignored rather than trusted.
	var presented [][]byte
	for _, field := range strings.Fields(signature) {
		version, encoded, ok := strings.Cut(field, ",")
		if !ok || version != "v1" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			continue
		}
		presented = append(presented, raw)
	}
	if len(presented) == 0 {
		return ErrInvalidHeader
	}

	for _, secret := range v.secrets {
		want := hmacSHA256(secret, signed)
		for _, got := range presented {
			if hmac.Equal(got, want) {
				return nil
			}
		}
	}
	return ErrNoMatch
}

func hmacSHA256(key, message []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(message)
	return mac.Sum(nil)
}

func nonEmpty(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// SharedSecretVerifier authenticates a first-party caller (pymthouse) by
// plain shared-secret comparison rather than an HMAC signature over the body.
//
// This is a deliberately different shape from StripeVerifier/StandardVerifier:
// those verify that a *third party* holding a webhook secret produced *this
// exact body*, which is what makes the Kafka value a durable, re-verifiable
// audit record months later. pymthouse is not a third party signing a payload
// for us to audit — it is asking us to do something, authenticated the same
// way any internal service-to-service call is. Comparing the presented secret
// against the configured list is sufficient and simpler; there is nothing to
// gain from binding a signature to a body only pymthouse itself produced.
type SharedSecretVerifier struct {
	secrets []string
}

// NewSharedSecretVerifier builds a verifier over the configured secret list.
func NewSharedSecretVerifier(secrets []string) *SharedSecretVerifier {
	return &SharedSecretVerifier{secrets: nonEmpty(secrets)}
}

// Enabled reports whether any secret is configured.
func (v *SharedSecretVerifier) Enabled() bool { return len(v.secrets) > 0 }

// Verify checks the presented secret against every configured one in
// constant time, so which position (if any) matched is never observable.
func (v *SharedSecretVerifier) Verify(presented string) error {
	if len(v.secrets) == 0 {
		return ErrNoMatch
	}
	if presented == "" {
		return ErrInvalidHeader
	}
	got := []byte(presented)
	matched := false
	for _, secret := range v.secrets {
		if hmac.Equal(got, []byte(secret)) {
			matched = true
		}
	}
	if !matched {
		return ErrNoMatch
	}
	return nil
}
