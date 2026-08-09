// Package admin serves the authenticated settlement ops console on the worker.
package admin

import (
	"bytes"
	"context"
	"crypto/subtle"
	"embed"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/openmeter"
	"github.com/pymthouse/settlement/internal/ops"
)

const (
	cookieName   = "settlement_admin"
	csrfFormName = "csrf"
	adminRoot    = "/admin"
	adminHome    = "/admin/"
	adminLogin   = "/admin/login"
)

//go:embed all:ui
var uiFS embed.FS

// Deps are the worker-side dependencies the console needs.
type Deps struct {
	Admin     config.Admin
	Kafka     config.Kafka
	OpenMeter *openmeter.Client
	RedisURL  string
	Log       *slog.Logger
}

// Handler returns the /admin mux. When Token is empty, every path 404s.
func Handler(deps Deps) http.Handler {
	mux := http.NewServeMux()
	tmpl := template.Must(template.New("").Funcs(template.FuncMap{
		"join": strings.Join,
	}).ParseFS(uiFS, "ui/*.html"))
	s := &server{
		deps: deps,
		tmpl: tmpl,
	}

	static, err := fs.Sub(uiFS, "ui/static")
	if err != nil {
		panic(err)
	}

	mux.HandleFunc("GET /admin/login", s.handleLoginGet)
	mux.HandleFunc("POST /admin/login", s.handleLoginPost)
	mux.HandleFunc("POST /admin/logout", s.requireAuth(s.handleLogout))

	mux.HandleFunc("GET /admin/", s.requireAuth(s.handleOverview))
	mux.HandleFunc("GET /admin", s.requireAuth(s.handleOverview))
	mux.HandleFunc("GET /admin/invoice", s.requireAuth(s.handleInvoiceGet))
	mux.HandleFunc("GET /admin/dlq", s.requireAuth(s.handleDLQGet))
	mux.HandleFunc("POST /admin/dlq/redrive", s.requireAuth(s.handleDLQRedrive))
	mux.HandleFunc("GET /admin/replay", s.requireAuth(s.handleReplayGet))
	mux.HandleFunc("POST /admin/replay", s.requireAuth(s.handleReplayPost))
	mux.HandleFunc("GET /admin/links", s.requireAuth(s.handleLinks))
	mux.Handle("GET /admin/static/", http.StripPrefix("/admin/static/", http.FileServer(http.FS(static))))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if deps.Admin.Token == "" {
			http.NotFound(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

type server struct {
	deps Deps
	tmpl *template.Template
}

type pageData struct {
	Title   string
	Flash   string
	Error   string
	CSRF    string
	Nav     string
	Links   config.Admin
	Content any
	Body    template.HTML
}

func (s *server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			http.Redirect(w, r, adminLogin+"?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
			return
		}
		if r.Method == http.MethodPost && !s.validCSRF(r) {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodPost && !sameOriginOK(r) {
			http.Error(w, "cross-site request blocked", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func (s *server) authorized(r *http.Request) bool {
	token := s.deps.Admin.Token
	if token == "" {
		return false
	}
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		got := strings.TrimSpace(auth[7:])
		return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
	}
	c, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(c.Value), []byte(token)) == 1
}

func (s *server) validCSRF(r *http.Request) bool {
	token := s.deps.Admin.Token
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		got := strings.TrimSpace(auth[7:])
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
			return true
		}
	}
	_ = r.ParseForm()
	got := r.FormValue(csrfFormName)
	c, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(c.Value)) == 1
}

func sameOriginOK(r *http.Request) bool {
	site := r.Header.Get("Sec-Fetch-Site")
	if site == "" {
		return true // older clients
	}
	return site == "same-origin" || site == "none"
}

func (s *server) handleLoginGet(w http.ResponseWriter, r *http.Request) {
	if s.authorized(r) {
		http.Redirect(w, r, adminHome, http.StatusSeeOther)
		return
	}
	s.render(w, "login.html", pageData{
		Title: "Login",
		Nav:   "login",
		Content: map[string]string{
			"Next": r.URL.Query().Get("next"),
		},
	})
}

func (s *server) handleLoginPost(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	got := r.FormValue("token")
	if subtle.ConstantTimeCompare([]byte(got), []byte(s.deps.Admin.Token)) != 1 {
		s.render(w, "login.html", pageData{
			Title: "Login",
			Nav:   "login",
			Error: "invalid token",
			Content: map[string]string{
				"Next": r.FormValue("next"),
			},
		})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    s.deps.Admin.Token,
		Path:     adminRoot,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   true,
	})
	next := r.FormValue("next")
	if next == "" || !strings.HasPrefix(next, adminRoot) {
		next = adminHome
	}
	http.Redirect(w, r, next, http.StatusSeeOther)
}

func (s *server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    "",
		Path:     adminRoot,
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   true,
	})
	http.Redirect(w, r, adminLogin, http.StatusSeeOther)
}

type overviewContent struct {
	ProducerURL     string
	ProducerOK      string
	RedisConfigured bool
	Signals         []signalRow
	Topics          []string
}

type signalRow struct {
	Name    string
	Help    string
	Value   string
	Details string
}

func (s *server) handleOverview(w http.ResponseWriter, r *http.Request) {
	producerOK := "not configured"
	if s.deps.Admin.ProducerURL != "" {
		producerOK = probeHealth(r.Context(), s.deps.Admin.ProducerURL+"/healthz")
	}
	s.render(w, "overview.html", pageData{
		Title: "Overview",
		Nav:   "overview",
		CSRF:  s.deps.Admin.Token,
		Links: s.deps.Admin,
		Content: overviewContent{
			ProducerURL:     s.deps.Admin.ProducerURL,
			ProducerOK:      producerOK,
			RedisConfigured: s.deps.RedisURL != "",
			Signals:         gatherWatchSignals(),
			Topics: []string{
				s.deps.Kafka.TopicOpenMeter,
				s.deps.Kafka.TopicStripe,
				s.deps.Kafka.TopicDLQ,
			},
		},
	})
}

func probeHealth(ctx context.Context, url string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "error"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "unreachable"
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusOK {
		return "ok"
	}
	return fmt.Sprintf("http %d", resp.StatusCode)
}

func gatherWatchSignals() []signalRow {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return []signalRow{{Name: "gather_error", Value: err.Error()}}
	}
	byName := map[string]*dto.MetricFamily{}
	for _, f := range families {
		byName[f.GetName()] = f
	}
	return []signalRow{
		{Name: "settlement_dead_lettered_total", Help: "DLQ park events", Value: fmt.Sprintf("%.0f", sumCounter(byName, "settlement_dead_lettered_total"))},
		{Name: "settlement_invoices_needing_attention_total", Help: "Failed/overdue/uncollectible", Value: fmt.Sprintf("%.0f", sumCounter(byName, "settlement_invoices_needing_attention_total"))},
		{Name: "settlement_webhooks_received_total", Help: "Doorman inbound (see labels)", Value: fmt.Sprintf("%.0f", sumCounter(byName, "settlement_webhooks_received_total"))},
		{Name: "settlement_committed_offset", Help: "Sum of committed offsets", Value: fmt.Sprintf("%.0f", gaugeVal(byName, "settlement_committed_offset"))},
		{Name: "settlement_reconcile_redriven_total", Help: "Sweeper redrives", Value: fmt.Sprintf("%.0f", sumCounter(byName, "settlement_reconcile_redriven_total"))},
		{Name: "settlement_events_in_flight", Help: "Currently processing", Value: fmt.Sprintf("%.0f", gaugeVal(byName, "settlement_events_in_flight"))},
		{Name: "settlement_upstream_requests_total", Help: "OpenMeter/Stripe calls", Value: fmt.Sprintf("%.0f", sumCounter(byName, "settlement_upstream_requests_total"))},
	}
}

func sumCounter(byName map[string]*dto.MetricFamily, name string) float64 {
	f := byName[name]
	if f == nil {
		return 0
	}
	var sum float64
	for _, m := range f.Metric {
		if m.Counter != nil {
			sum += m.Counter.GetValue()
		}
	}
	return sum
}

func gaugeVal(byName map[string]*dto.MetricFamily, name string) float64 {
	f := byName[name]
	if f == nil {
		return 0
	}
	var sum float64
	for _, m := range f.Metric {
		if m.Gauge != nil {
			sum += m.Gauge.GetValue()
		}
	}
	return sum
}

type invoiceContent struct {
	ID               string
	Invoice          *openmeter.Invoice
	StripeInvoiceURL string
	StripePaymentURL string
	OpenMeterURL     string
	LookupError      string
}

func (s *server) handleInvoiceGet(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	content := invoiceContent{ID: id}
	if id != "" && s.deps.OpenMeter != nil {
		inv, err := s.deps.OpenMeter.GetInvoice(r.Context(), id)
		if err != nil {
			content.LookupError = err.Error()
		} else {
			content.Invoice = inv
			content.OpenMeterURL = openMeterInvoiceURL(s.deps.Admin, id)
			if inv.ExternalIDs.Invoicing != "" {
				content.StripeInvoiceURL = strings.TrimRight(s.deps.Admin.StripeDashboardURL, "/") + "/invoices/" + inv.ExternalIDs.Invoicing
			}
			if inv.ExternalIDs.Payment != "" {
				content.StripePaymentURL = strings.TrimRight(s.deps.Admin.StripeDashboardURL, "/") + "/payments/" + inv.ExternalIDs.Payment
			}
		}
	}
	s.render(w, "invoice.html", pageData{
		Title:   "Invoice",
		Nav:     "invoice",
		CSRF:    s.deps.Admin.Token,
		Links:   s.deps.Admin,
		Content: content,
	})
}

func openMeterInvoiceURL(admin config.Admin, id string) string {
	base := admin.OpenMeterUIURL
	if base == "" {
		return ""
	}
	return strings.TrimRight(base, "/") + "/invoices/" + url.PathEscape(id)
}

type dlqContent struct {
	Partition int
	Offset    int64
	Count     int
	Records   []ops.InspectRecord
	Result    string
	Error     string
	Topic     string
}

func (s *server) handleDLQGet(w http.ResponseWriter, r *http.Request) {
	partition, _ := strconv.Atoi(r.URL.Query().Get("partition"))
	count, _ := strconv.Atoi(r.URL.Query().Get("count"))
	if count <= 0 {
		count = 25
	}
	offset := ops.FirstOffset
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			offset = n
		}
	}
	content := dlqContent{
		Partition: partition,
		Offset:    offset,
		Count:     count,
		Topic:     s.deps.Kafka.TopicDLQ,
	}
	records, err := ops.Inspect(r.Context(), s.deps.Kafka, ops.InspectInput{
		Topic:     s.deps.Kafka.TopicDLQ,
		Partition: partition,
		Offset:    offset,
		Count:     count,
		Full:      false,
	})
	if err != nil {
		content.Error = err.Error()
	} else {
		content.Records = records
	}
	s.render(w, "dlq.html", pageData{
		Title:   "DLQ",
		Nav:     "dlq",
		CSRF:    s.deps.Admin.Token,
		Links:   s.deps.Admin,
		Flash:   r.URL.Query().Get("flash"),
		Content: content,
	})
}

func (s *server) handleDLQRedrive(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	if r.FormValue("confirm") != "REDRIVE" {
		http.Error(w, `confirm must be exactly "REDRIVE"`, http.StatusBadRequest)
		return
	}
	partition, _ := strconv.Atoi(r.FormValue("partition"))
	count, _ := strconv.Atoi(r.FormValue("count"))
	dryRun := r.FormValue("dry_run") == "1" || r.FormValue("dry_run") == "on"
	offset := ops.FirstOffset
	if v := r.FormValue("offset"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			offset = n
		}
	}
	reason := strings.TrimSpace(r.FormValue("reason"))

	result, err := ops.Redrive(r.Context(), s.deps.Kafka, ops.RedriveInput{
		Partition: partition,
		Offset:    offset,
		Count:     count,
		Reason:    reason,
		DryRun:    dryRun,
	})
	if s.deps.Log != nil {
		s.deps.Log.Info("admin_action",
			"action", "dlq_redrive",
			"partition", partition,
			"dry_run", dryRun,
			"published", result.Published,
			"skipped", result.Skipped,
			"batch", result.BatchID,
			"error", errString(err),
		)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	flash := fmt.Sprintf("redriven=%d skipped=%d batch=%s dry_run=%v", result.Published, result.Skipped, result.BatchID, dryRun)
	http.Redirect(w, r, adminRoot+"/dlq?flash="+url.QueryEscape(flash), http.StatusSeeOther)
}

type replayContent struct {
	Topic     string
	Partition int
	Offset    string
	Count     int
	DryRun    bool
	Result    string
	Error     string
	Topics    []string
}

func (s *server) handleReplayGet(w http.ResponseWriter, r *http.Request) {
	s.render(w, "replay.html", pageData{
		Title: "Replay",
		Nav:   "replay",
		CSRF:  s.deps.Admin.Token,
		Links: s.deps.Admin,
		Content: replayContent{
			Topic:  s.deps.Kafka.TopicOpenMeter,
			DryRun: true,
			Count:  10,
			Topics: []string{s.deps.Kafka.TopicOpenMeter, s.deps.Kafka.TopicStripe},
		},
	})
}

func (s *server) handleReplayPost(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	if r.FormValue("confirm") != "REPLAY" {
		http.Error(w, `confirm must be exactly "REPLAY"`, http.StatusBadRequest)
		return
	}
	topic := strings.TrimSpace(r.FormValue("topic"))
	partition, _ := strconv.Atoi(r.FormValue("partition"))
	count, _ := strconv.Atoi(r.FormValue("count"))
	dryRun := r.FormValue("dry_run") == "1" || r.FormValue("dry_run") == "on"
	offset := ops.FirstOffset
	if v := r.FormValue("offset"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			offset = n
		}
	}

	result, err := ops.Replay(r.Context(), s.deps.Kafka, ops.ReplayInput{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Count:     count,
		DryRun:    dryRun,
	})
	if s.deps.Log != nil {
		s.deps.Log.Info("admin_action",
			"action", "replay",
			"topic", topic,
			"partition", partition,
			"dry_run", dryRun,
			"published", result.Published,
			"batch", result.BatchID,
			"error", errString(err),
		)
	}
	content := replayContent{
		Topic:     topic,
		Partition: partition,
		Offset:    r.FormValue("offset"),
		Count:     count,
		DryRun:    dryRun,
		Topics:    []string{s.deps.Kafka.TopicOpenMeter, s.deps.Kafka.TopicStripe},
	}
	if err != nil {
		content.Error = err.Error()
	} else {
		content.Result = fmt.Sprintf("published=%d skipped=%d start=%d batch=%s dry_run=%v",
			result.Published, result.Skipped, result.Start, result.BatchID, dryRun)
	}
	s.render(w, "replay.html", pageData{
		Title:   "Replay",
		Nav:     "replay",
		CSRF:    s.deps.Admin.Token,
		Links:   s.deps.Admin,
		Content: content,
	})
}

func (s *server) handleLinks(w http.ResponseWriter, r *http.Request) {
	s.render(w, "links.html", pageData{
		Title: "Vendor links",
		Nav:   "links",
		CSRF:  s.deps.Admin.Token,
		Links: s.deps.Admin,
	})
}

func (s *server) render(w http.ResponseWriter, name string, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.Body = template.HTML(buf.String())
	if err := s.tmpl.ExecuteTemplate(w, "layout.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
