package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"
)

const (
	stripeSecret   = "whsec_test_secret_value"
	// Synthetic fixture only — not a real webhook secret.
	standardSecret = "whsec_c2V0dGxlbWVudC10ZXN0LWhtYWMta2V5LXYx"
)

func stripeSignature(t *testing.T, secret string, ts int64, body []byte) string {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10) + "." + string(body)))
	return "t=" + strconv.FormatInt(ts, 10) + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func TestStripeVerifierAcceptsGenuineSignature(t *testing.T) {
	body := []byte(`{"id":"evt_1","type":"invoice.payment_succeeded"}`)
	now := time.Now()

	v := NewStripeVerifier([]string{stripeSecret}, 300)
	if err := v.Verify(stripeSignature(t, stripeSecret, now.Unix(), body), body); err != nil {
		t.Fatalf("genuine signature rejected: %v", err)
	}
}

// A single altered byte must invalidate the signature — this is the property
// the whole trust model rests on.
func TestStripeVerifierRejectsTamperedBody(t *testing.T) {
	body := []byte(`{"id":"evt_1","amount":100}`)
	header := stripeSignature(t, stripeSecret, time.Now().Unix(), body)

	tampered := []byte(`{"id":"evt_1","amount":900}`)
	v := NewStripeVerifier([]string{stripeSecret}, 300)

	if err := v.Verify(header, tampered); !errors.Is(err, ErrNoMatch) {
		t.Fatalf("tampered body accepted or wrong error: %v", err)
	}
}

// Re-serialising JSON reorders keys and changes whitespace. This test pins the
// reason the producer must publish the exact bytes it verified.
func TestStripeVerifierRejectsReserializedBody(t *testing.T) {
	original := []byte(`{"id":"evt_1", "type":"invoice.paid"}`)
	header := stripeSignature(t, stripeSecret, time.Now().Unix(), original)

	reserialized := []byte(`{"id":"evt_1","type":"invoice.paid"}`) // whitespace dropped
	v := NewStripeVerifier([]string{stripeSecret}, 300)

	if err := v.Verify(header, reserialized); err == nil {
		t.Fatal("re-serialised body passed verification; raw bytes are not being preserved")
	}
}

func TestStripeVerifierRejectsWrongSecret(t *testing.T) {
	body := []byte(`{"id":"evt_1"}`)
	header := stripeSignature(t, "whsec_someone_elses_secret", time.Now().Unix(), body)

	v := NewStripeVerifier([]string{stripeSecret}, 300)
	if err := v.Verify(header, body); !errors.Is(err, ErrNoMatch) {
		t.Fatalf("expected ErrNoMatch, got %v", err)
	}
}

func TestStripeVerifierSupportsSecretRotation(t *testing.T) {
	body := []byte(`{"id":"evt_1"}`)
	header := stripeSignature(t, "whsec_new_secret", time.Now().Unix(), body)

	v := NewStripeVerifier([]string{"whsec_old_secret", "whsec_new_secret"}, 300)
	if err := v.Verify(header, body); err != nil {
		t.Fatalf("rotation secret rejected: %v", err)
	}
}

func TestStripeVerifierRejectsStaleTimestamp(t *testing.T) {
	body := []byte(`{"id":"evt_1"}`)
	old := time.Now().Add(-20 * time.Minute).Unix()
	header := stripeSignature(t, stripeSecret, old, body)

	v := NewStripeVerifier([]string{stripeSecret}, 300)
	if err := v.Verify(header, body); !errors.Is(err, ErrTooOld) {
		t.Fatalf("expected ErrTooOld, got %v", err)
	}
}

func TestStripeVerifierRejectsMalformedHeaders(t *testing.T) {
	body := []byte(`{"id":"evt_1"}`)
	v := NewStripeVerifier([]string{stripeSecret}, 300)

	cases := map[string]struct {
		header string
		want   error
	}{
		"empty":         {"", ErrNoSignature},
		"no signature":  {"t=" + strconv.FormatInt(time.Now().Unix(), 10), ErrInvalidHeader},
		"no timestamp":  {"v1=deadbeef", ErrInvalidHeader},
		"bad timestamp": {"t=not-a-number,v1=deadbeef", ErrInvalidHeader},
		"garbage":       {"nonsense", ErrInvalidHeader},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := v.Verify(tc.header, body); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestStripeVerifierWithoutSecretsRefusesEverything(t *testing.T) {
	v := NewStripeVerifier(nil, 300)
	if v.Enabled() {
		t.Fatal("verifier reports enabled with no secrets")
	}
	if err := v.Verify("t=1,v1=ab", []byte(`{}`)); !errors.Is(err, ErrNoSecrets) {
		t.Fatalf("expected ErrNoSecrets, got %v", err)
	}
}

func standardSignature(t *testing.T, secret, id string, ts int64, body []byte) string {
	t.Helper()
	key, err := base64.StdEncoding.DecodeString(secret[len("whsec_"):])
	if err != nil {
		t.Fatalf("decode secret: %v", err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + strconv.FormatInt(ts, 10) + "." + string(body)))
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestStandardVerifierAcceptsGenuineSignature(t *testing.T) {
	body := []byte(`{"id":"01J2K","type":"invoice.updated"}`)
	id := "msg_01J2K"
	ts := time.Now().Unix()

	v := NewStandardVerifier([]string{standardSecret}, 300)
	if err := v.Verify(id, strconv.FormatInt(ts, 10), standardSignature(t, standardSecret, id, ts, body), body); err != nil {
		t.Fatalf("genuine signature rejected: %v", err)
	}
}

// The message id is part of the signed content, so replaying a valid body
// under a different id must fail.
func TestStandardVerifierBindsSignatureToMessageID(t *testing.T) {
	body := []byte(`{"id":"01J2K"}`)
	ts := time.Now().Unix()
	signature := standardSignature(t, standardSecret, "msg_original", ts, body)

	v := NewStandardVerifier([]string{standardSecret}, 300)
	if err := v.Verify("msg_replayed", strconv.FormatInt(ts, 10), signature, body); !errors.Is(err, ErrNoMatch) {
		t.Fatalf("expected ErrNoMatch, got %v", err)
	}
}

func TestStandardVerifierIgnoresUnknownVersions(t *testing.T) {
	body := []byte(`{"id":"01J2K"}`)
	id := "msg_1"
	ts := time.Now().Unix()

	// A v2 entry we cannot check, alongside the v1 we can.
	signature := "v2,AAAA " + standardSignature(t, standardSecret, id, ts, body)

	v := NewStandardVerifier([]string{standardSecret}, 300)
	if err := v.Verify(id, strconv.FormatInt(ts, 10), signature, body); err != nil {
		t.Fatalf("mixed-version header rejected: %v", err)
	}
}

func TestStandardVerifierRejectsUnsignableHeaders(t *testing.T) {
	body := []byte(`{"id":"01J2K"}`)
	v := NewStandardVerifier([]string{standardSecret}, 300)
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	if err := v.Verify("msg_1", ts, "", body); !errors.Is(err, ErrNoSignature) {
		t.Fatalf("expected ErrNoSignature, got %v", err)
	}
	if err := v.Verify("", ts, "v1,AAAA", body); !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("expected ErrInvalidHeader for missing id, got %v", err)
	}
	if err := v.Verify("msg_1", ts, "v2,AAAA", body); !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("expected ErrInvalidHeader when no v1 entry present, got %v", err)
	}
}

func TestStandardVerifierRejectsStaleTimestamp(t *testing.T) {
	body := []byte(`{"id":"01J2K"}`)
	id := "msg_1"
	ts := time.Now().Add(-time.Hour).Unix()

	v := NewStandardVerifier([]string{standardSecret}, 300)
	err := v.Verify(id, strconv.FormatInt(ts, 10), standardSignature(t, standardSecret, id, ts, body), body)
	if !errors.Is(err, ErrTooOld) {
		t.Fatalf("expected ErrTooOld, got %v", err)
	}
}

// Self-hosted deployments sometimes configure a plain string secret rather
// than a base64 one; it must still verify.
func TestStandardVerifierAcceptsRawSecret(t *testing.T) {
	const raw = "plain-text-secret-!!"
	body := []byte(`{"id":"01J2K"}`)
	id := "msg_1"
	ts := time.Now().Unix()

	mac := hmac.New(sha256.New, []byte(raw))
	mac.Write([]byte(id + "." + strconv.FormatInt(ts, 10) + "." + string(body)))
	signature := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))

	v := NewStandardVerifier([]string{raw}, 300)
	if err := v.Verify(id, strconv.FormatInt(ts, 10), signature, body); err != nil {
		t.Fatalf("raw secret rejected: %v", err)
	}
}

// A secret that is valid base64 must verify under both the decoded-bytes and
// the raw-string interpretations, since operators configure either.
func TestStandardVerifierAcceptsBothBase64Interpretations(t *testing.T) {
	const raw = "dGVzdC1zZWNyZXQtdmFsdWU=" // valid base64
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"id":"01J2K"}`)
	id := "msg_1"
	ts := time.Now().Unix()
	signedContent := id + "." + strconv.FormatInt(ts, 10) + "." + string(body)

	sign := func(key []byte) string {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(signedContent))
		return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	}

	v := NewStandardVerifier([]string{"whsec_" + raw}, 300)

	if err := v.Verify(id, strconv.FormatInt(ts, 10), sign(decoded), body); err != nil {
		t.Fatalf("decoded-bytes interpretation rejected: %v", err)
	}
	if err := v.Verify(id, strconv.FormatInt(ts, 10), sign([]byte(raw)), body); err != nil {
		t.Fatalf("raw-string interpretation rejected: %v", err)
	}
}
