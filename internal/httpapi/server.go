package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/yash/dispatch/internal/auth"
	"github.com/yash/dispatch/internal/config"
	"github.com/yash/dispatch/internal/delivery"
	"github.com/yash/dispatch/internal/idempotency"
	kafkapkg "github.com/yash/dispatch/internal/kafka"
	"github.com/yash/dispatch/internal/ratelimit"
	"github.com/yash/dispatch/internal/store"
)

type Server struct {
	cfg    config.Config
	store  *store.Store
	limit  *ratelimit.Limiter
	idem   *idempotency.Store
	deliv  *delivery.Deliverer
	prod   *kafkapkg.Producer
	log    *slog.Logger
	mux    *http.ServeMux
}

func New(cfg config.Config, st *store.Store, limit *ratelimit.Limiter, idem *idempotency.Store, deliv *delivery.Deliverer, prod *kafkapkg.Producer, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{cfg: cfg, store: st, limit: limit, idem: idem, deliv: deliv, prod: prod, log: log, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)

	s.mux.HandleFunc("POST /v1/tenants", s.handleCreateTenant)

	s.mux.Handle("POST /v1/subscriptions", s.auth(http.HandlerFunc(s.handleCreateSubscription)))
	s.mux.Handle("GET /v1/subscriptions", s.auth(http.HandlerFunc(s.handleListSubscriptions)))
	s.mux.Handle("GET /v1/subscriptions/{id}", s.auth(http.HandlerFunc(s.handleGetSubscription)))
	s.mux.Handle("DELETE /v1/subscriptions/{id}", s.auth(http.HandlerFunc(s.handleDeleteSubscription)))
	s.mux.Handle("POST /v1/subscriptions/{id}/rotate-secret", s.auth(http.HandlerFunc(s.handleRotateSecret)))
	s.mux.Handle("POST /v1/subscriptions/{id}/activate", s.auth(http.HandlerFunc(s.handleActivate)))
	s.mux.Handle("GET /v1/subscriptions/{id}/deliveries", s.auth(http.HandlerFunc(s.handleListDeliveries)))
	s.mux.Handle("POST /v1/events", s.auth(http.HandlerFunc(s.handleCreateEvent)))
	s.mux.Handle("GET /v1/dead-letters", s.auth(http.HandlerFunc(s.handleListDeadLetters)))
	s.mux.Handle("POST /v1/dead-letters/{id}/replay", s.auth(http.HandlerFunc(s.handleReplayDeadLetter)))
}

type ctxKey int

const tenantKey ctxKey = 1

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "missing or invalid Authorization header")
			return
		}
		key := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
		if key == "" {
			writeError(w, http.StatusUnauthorized, "missing API key")
			return
		}
		hash := auth.HashAPIKey(key)
		tenant, err := s.store.TenantByAPIKeyHash(r.Context(), hash)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, http.StatusUnauthorized, "invalid API key")
				return
			}
			s.log.Error("tenant lookup failed", "err", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		ctx := contextWithTenant(r.Context(), tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "postgres unavailable")
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	apiKey, err := auth.NewAPIKey()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate API key")
		return
	}
	tenant, err := s.store.CreateTenant(r.Context(), body.Name, auth.HashAPIKey(apiKey))
	if err != nil {
		s.log.Error("create tenant failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         tenant.ID,
		"name":       tenant.Name,
		"api_key":    apiKey,
		"created_at": tenant.CreatedAt,
	})
}

func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	var body struct {
		URL        string   `json:"url"`
		EventTypes []string `json:"event_types"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.URL == "" {
		writeError(w, http.StatusBadRequest, "url is required")
		return
	}
	secret, err := auth.NewHMACSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate secret")
		return
	}
	sub, err := s.store.CreateSubscription(r.Context(), tenant.ID, body.URL, body.EventTypes, secret)
	if err != nil {
		s.log.Error("create subscription failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":          sub.ID,
		"tenant_id":   sub.TenantID,
		"url":         sub.URL,
		"event_types": sub.EventTypes,
		"secret":      secret,
		"state":       sub.State,
		"created_at":  sub.CreatedAt,
	})
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	cursor, err := store.ParseCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	subs, err := s.store.ListSubscriptions(r.Context(), tenant.ID, cursor, limit)
	if err != nil {
		s.log.Error("list subscriptions failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	type item struct {
		ID         uuid.UUID `json:"id"`
		URL        string    `json:"url"`
		EventTypes []string  `json:"event_types"`
		State      string    `json:"state"`
		CreatedAt  time.Time `json:"created_at"`
	}
	items := make([]item, 0, len(subs))
	var nextCursor string
	for _, sub := range subs {
		items = append(items, item{
			ID: sub.ID, URL: sub.URL, EventTypes: sub.EventTypes, State: sub.State, CreatedAt: sub.CreatedAt,
		})
	}
	if len(subs) > 0 {
		nextCursor = store.FormatCursor(subs[len(subs)-1].CreatedAt)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nextCursor})
}

func (s *Server) handleGetSubscription(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	sub, err := s.store.GetSubscription(r.Context(), tenant.ID, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":                   sub.ID,
		"tenant_id":            sub.TenantID,
		"url":                  sub.URL,
		"event_types":          sub.EventTypes,
		"state":                sub.State,
		"consecutive_failures": sub.ConsecutiveFailures,
		"state_changed_at":     sub.StateChangedAt,
		"dlq_count":            sub.DLQCount,
		"created_at":           sub.CreatedAt,
	})
}

func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	if err := s.store.DeleteSubscription(r.Context(), tenant.ID, id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRotateSecret(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var body struct {
		GracePeriod string `json:"grace_period"`
	}
	_ = decodeJSON(r, &body)
	grace := 24 * time.Hour
	if body.GracePeriod != "" {
		d, err := time.ParseDuration(body.GracePeriod)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid grace_period")
			return
		}
		grace = d
	}
	secret, err := auth.NewHMACSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate secret")
		return
	}
	sub, err := s.store.RotateSecret(r.Context(), tenant.ID, id, secret, grace)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":                          sub.ID,
		"secret":                      secret,
		"previous_secret_expires_at":  sub.PreviousSecretExpiresAt,
	})
}

func (s *Server) handleActivate(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	sub, err := s.store.ActivateSubscription(r.Context(), tenant.ID, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":    sub.ID,
		"state": sub.State,
	})
}

func (s *Server) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	cursor, err := store.ParseCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := s.store.ListDeliveries(r.Context(), tenant.ID, id, cursor, limit)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	var nextCursor string
	if len(items) > 0 {
		nextCursor = store.FormatCursor(items[len(items)-1].CreatedAt)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nextCursor})
}

func (s *Server) handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())

	allow, retryAfter, _ := s.limit.Allow(r.Context(), tenant.ID.String())
	if !allow {
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxPayloadBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload too large")
			return
		}
		writeError(w, http.StatusBadRequest, "failed to read body")
		return
	}
	if !json.Valid(raw) {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	var envelope struct {
		EventType string          `json:"event_type"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if envelope.EventType == "" {
		writeError(w, http.StatusBadRequest, "event_type is required")
		return
	}
	payload := envelope.Payload
	if payload == nil {
		payload = raw
	}
	if !json.Valid(payload) {
		writeError(w, http.StatusBadRequest, "invalid payload JSON")
		return
	}

	idemKeyHdr := r.Header.Get("Idempotency-Key")
	var idemKey *string
	eventID := uuid.New()

	if idemKeyHdr != "" {
		idemKey = &idemKeyHdr
		existingID, exists, err := s.idem.Reserve(r.Context(), tenant.ID.String(), idemKeyHdr, eventID.String())
		if err == nil && exists && existingID != "" {
			writeJSON(w, http.StatusOK, map[string]any{"id": existingID, "idempotent_replay": true})
			return
		}
		// Redis error: fall through; Postgres unique index is the safety net.
	}

	ev, err := s.store.CreateEvent(r.Context(), eventID, tenant.ID, envelope.EventType, payload, idemKey)
	if err != nil {
		if errors.Is(err, store.ErrConflict) && idemKey != nil {
			existing, err2 := s.store.EventByIdempotencyKey(r.Context(), tenant.ID, *idemKey)
			if err2 == nil {
				_ = s.idem.Set(r.Context(), tenant.ID.String(), *idemKey, existing.ID.String())
				writeJSON(w, http.StatusOK, map[string]any{"id": existing.ID, "idempotent_replay": true})
				return
			}
		}
		s.log.Error("create event failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	log := s.log.With("event_id", ev.ID, "tenant_id", tenant.ID)
	log.Info("event ingested")

	if s.prod == nil {
		writeError(w, http.StatusInternalServerError, "kafka producer not configured")
		return
	}
	if err := s.prod.ProduceIngest(r.Context(), kafkapkg.EventMessage{
		EventID:   ev.ID,
		TenantID:  tenant.ID,
		EventType: ev.EventType,
		Payload:   payload,
	}); err != nil {
		log.Error("kafka produce failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue event")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":         ev.ID,
		"event_type": ev.EventType,
		"status":     "accepted",
		"created_at": ev.CreatedAt,
	})
}

func (s *Server) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	subIDStr := r.URL.Query().Get("subscription_id")
	if subIDStr == "" {
		writeError(w, http.StatusBadRequest, "subscription_id is required")
		return
	}
	subID, err := uuid.Parse(subIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid subscription_id")
		return
	}
	cursor, err := store.ParseCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := s.store.ListDeadLetters(r.Context(), tenant.ID, subID, cursor, limit)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	var nextCursor string
	if len(items) > 0 {
		nextCursor = store.FormatCursor(items[len(items)-1].CreatedAt)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nextCursor})
}

func (s *Server) handleReplayDeadLetter(w http.ResponseWriter, r *http.Request) {
	tenant := tenantFromContext(r.Context())
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	ev, err := s.store.MarkDeadLetterReplayed(r.Context(), tenant.ID, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict, "already replayed")
			return
		}
		s.log.Error("replay dead letter failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if err := s.prod.ProduceIngest(r.Context(), kafkapkg.EventMessage{
		EventID:   ev.ID,
		TenantID:  ev.TenantID,
		EventType: ev.EventType,
		Payload:   ev.Payload,
	}); err != nil {
		s.log.Error("replay produce failed", "err", err, "event_id", ev.ID)
		writeError(w, http.StatusInternalServerError, "failed to enqueue replay")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"dead_letter_id": id,
		"event_id":       ev.ID,
		"status":         "replayed",
	})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func contextWithTenant(ctx context.Context, tenant store.Tenant) context.Context {
	return context.WithValue(ctx, tenantKey, tenant)
}

func tenantFromContext(ctx context.Context) store.Tenant {
	t, _ := ctx.Value(tenantKey).(store.Tenant)
	return t
}
