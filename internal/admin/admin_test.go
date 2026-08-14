package admin

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/openmeter"
)

func TestAdminDisabledWithoutToken(t *testing.T) {
	h := Handler(Deps{Admin: config.Admin{}})
	req := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when admin token unset", rec.Code)
	}
}

func TestAdminRequiresAuth(t *testing.T) {
	h := Handler(Deps{Admin: config.Admin{Token: "secret"}})
	req := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want redirect to login", rec.Code)
	}
	loc := rec.Header().Get("Location")
	if !strings.HasPrefix(loc, "/admin/login") {
		t.Fatalf("Location = %q", loc)
	}
}

func TestAdminBearerAuth(t *testing.T) {
	h := Handler(Deps{
		Admin: config.Admin{Token: "secret", StripeDashboardURL: "https://dashboard.stripe.com"},
		Kafka: config.Kafka{TopicDLQ: "billing.settlement.dlq.v1", TopicOpenMeter: "billing.openmeter.invoices.v1", TopicStripe: "billing.stripe.events.v1"},
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "What to watch") {
		t.Fatalf("overview missing watch signals: %s", body)
	}
}

func TestLoginSetsCookie(t *testing.T) {
	h := Handler(Deps{Admin: config.Admin{Token: "secret"}})
	req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader("token=secret&next=/admin/"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d", rec.Code)
	}
	cookie := rec.Result().Cookies()
	found := false
	for _, c := range cookie {
		if c.Name == cookieName && c.Value == "secret" && c.HttpOnly {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected HttpOnly admin cookie, got %#v", cookie)
	}
	for _, c := range cookie {
		if c.Name == cookieName && !c.Secure {
			t.Fatalf("expected Secure cookie for admin session")
		}
	}
}

func TestLoginRedirectRejectsOffSiteNext(t *testing.T) {
	// go/unvalidated-url-redirection: "next" must never send the browser
	// off the admin console's own origin.
	cases := []string{
		"https://evil.example/phish",
		"http://evil.example/phish",
		"//evil.example/phish",
		`/\evil.example`,
		"evil.example",
	}
	h := Handler(Deps{Admin: config.Admin{Token: "secret"}})
	for _, next := range cases {
		t.Run(next, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader("token=secret&next="+next))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if loc := rec.Header().Get("Location"); loc != adminHome {
				t.Fatalf("Location = %q, want fallback to %q", loc, adminHome)
			}
		})
	}
}

func TestLoginRedirectAllowsOnSiteNext(t *testing.T) {
	h := Handler(Deps{Admin: config.Admin{Token: "secret"}})
	req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader("token=secret&next=/admin/dlq"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if loc := rec.Header().Get("Location"); loc != "/admin/dlq" {
		t.Fatalf("Location = %q, want /admin/dlq", loc)
	}
}

func TestLoginRedirectPreservesQueryOnAllowedPath(t *testing.T) {
	h := Handler(Deps{Admin: config.Admin{Token: "secret"}})
	req := httptest.NewRequest(http.MethodPost, "/admin/login", strings.NewReader("token=secret&next=/admin/invoice%3Fid%3Dinv_lookup"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if loc := rec.Header().Get("Location"); loc != "/admin/invoice?id=inv_lookup" {
		t.Fatalf("Location = %q, want /admin/invoice?id=inv_lookup", loc)
	}
}

func TestRedriveRequiresConfirm(t *testing.T) {
	h := Handler(Deps{
		Admin: config.Admin{Token: "secret"},
		Kafka: config.Kafka{TopicDLQ: "billing.settlement.dlq.v1"},
	})
	req := httptest.NewRequest(http.MethodPost, "/admin/dlq/redrive", strings.NewReader("csrf=secret&partition=0&confirm=nope&dry_run=1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for bad confirm", rec.Code)
	}
}

func TestReplayRequiresConfirm(t *testing.T) {
	h := Handler(Deps{
		Admin: config.Admin{Token: "secret"},
		Kafka: config.Kafka{TopicOpenMeter: "billing.openmeter.invoices.v1"},
	})
	req := httptest.NewRequest(http.MethodPost, "/admin/replay", strings.NewReader("csrf=secret&topic=billing.openmeter.invoices.v1&confirm=wrong&dry_run=1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for bad confirm", rec.Code)
	}
}

func TestLinksPage(t *testing.T) {
	h := Handler(Deps{Admin: config.Admin{
		Token:              "secret",
		StripeDashboardURL: "https://dashboard.stripe.com",
		OpenMeterUIURL:     "https://cloud.konghq.com",
		RailwayURL:         "https://railway.app/project/x",
		ProducerURL:        "https://producer.example",
	}})
	req := httptest.NewRequest(http.MethodGet, "/admin/links", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"dashboard.stripe.com", "cloud.konghq.com", "railway.app", "producer.example"} {
		if !strings.Contains(body, want) {
			t.Errorf("links page missing %q", want)
		}
	}
}

func TestInvoiceLookup(t *testing.T) {
	om := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "inv_lookup") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"id":"inv_lookup","currency":"USD","status":"paid","number":"INV-9",
			"statusDetails":{"immutable":true,"failed":false,"extendedStatus":"paid"},
			"customer":{"id":"cus_1","key":"owner-1"},
			"totals":{"total":"10.00"},
			"externalIds":{"invoicing":"in_stripe_9","payment":"pi_9"},
			"lines":[]
		}`)
	}))
	t.Cleanup(om.Close)

	h := Handler(Deps{
		Admin: config.Admin{
			Token:              "secret",
			StripeDashboardURL: "https://dashboard.stripe.com",
			OpenMeterUIURL:     "https://cloud.konghq.com/ui",
		},
		OpenMeter: openmeter.New(config.OpenMeter{BaseURL: om.URL, APIKey: "k", Timeout: 5 * time.Second}),
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/invoice?id=inv_lookup", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{"paid", "in_stripe_9", "pi_9", "/invoices/in_stripe_9", "/payments/pi_9"} {
		if !strings.Contains(body, want) {
			t.Errorf("invoice page missing %q in %s", want, body)
		}
	}
}
