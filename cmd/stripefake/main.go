package main

import (
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pymthouse/settlement/internal/stripefake"
)

func main() {
	addr := env("SETTLEMENT_STRIPEFAKE_ADDR", ":12111")
	webhookURL := strings.TrimSpace(os.Getenv("SETTLEMENT_STRIPE_WEBHOOK_URL"))
	webhookSecret := firstSecret(os.Getenv("SETTLEMENT_STRIPE_WEBHOOK_SECRETS"))
	if webhookURL != "" && webhookSecret == "" {
		log.Fatal("SETTLEMENT_STRIPE_WEBHOOK_SECRETS is required when SETTLEMENT_STRIPE_WEBHOOK_URL is set")
	}

	server := stripefake.New(stripefake.Config{
		WebhookURL:    webhookURL,
		WebhookSecret: webhookSecret,
		Timeout:       10 * time.Second,
	})

	log.Printf("stripefake listening on %s", addr)
	if err := http.ListenAndServe(addr, server.Handler()); err != nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func firstSecret(raw string) string {
	for _, part := range strings.Split(raw, ",") {
		if v := strings.TrimSpace(part); v != "" {
			return v
		}
	}
	return ""
}
