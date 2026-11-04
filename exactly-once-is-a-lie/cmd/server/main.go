// Command server is a tiny payment API with four ways to handle a charge request:
//
//	POST /none/charges     no idempotency at all
//	POST /naive/charges    SELECT the key, and if it is missing, charge and INSERT it (check-then-insert)
//	POST /correct/charges  claim the key with INSERT ... ON CONFLICT, store the response, replay it
//	POST /outbox/charges   correct mode + a transactional outbox, relayed to two consumers
//
// The "payment provider" is faked in-process. It counts how many times money
// actually moved, which is the number that matters.
package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type chargeReq struct {
	Account     string `json:"account"`
	AmountCents int    `json:"amount_cents"`
}

type chargeResp struct {
	ChargeID    string `json:"charge_id"`
	Account     string `json:"account"`
	AmountCents int    `json:"amount_cents"`
	Status      string `json:"status"`
}

// ---------------------------------------------------------------------------
// Fake payment provider. Like Stripe, it accepts an optional idempotency key:
// the same key returns the first result instead of moving money again.

type provider struct {
	mu       sync.Mutex
	minMs    int
	maxMs    int
	calls    int64             // times money actually moved
	byRef    map[string]int    // request ref -> times money moved for it
	byKey    map[string]string // provider idempotency key -> charge id
	replayed int64
	seq      int64
}

func newProvider(minMs, maxMs int) *provider {
	p := &provider{minMs: minMs, maxMs: maxMs}
	p.reset()
	return p
}

func (p *provider) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls, p.replayed, p.seq = 0, 0, 0
	p.byRef = map[string]int{}
	p.byKey = map[string]string{}
}

// Charge waits like a network call, then moves money (or replays, if idemKey was seen).
func (p *provider) Charge(ctx context.Context, ref, idemKey string, amount int) (string, error) {
	d := time.Duration(p.minMs+rand.Intn(p.maxMs-p.minMs+1)) * time.Millisecond
	select {
	case <-time.After(d):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if idemKey != "" {
		if id, ok := p.byKey[idemKey]; ok {
			p.replayed++
			return id, nil
		}
	}
	p.seq++
	p.calls++
	p.byRef[ref]++
	id := fmt.Sprintf("ch_%06d", p.seq)
	if idemKey != "" {
		p.byKey[idemKey] = id
	}
	return id, nil
}

func (p *provider) stats() (calls, uniqueRefs, replayed int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, int64(len(p.byRef)), p.replayed
}

// ---------------------------------------------------------------------------

type app struct {
	db    *pgxpool.Pool
	prov  *provider
	stale time.Duration
	relay *relay
	pgVer string
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8089", "listen address")
	dsn := flag.String("dsn", "postgres://postgres@127.0.0.1:55433/postgres?sslmode=disable", "postgres DSN")
	minMs := flag.Int("provider-min-ms", 10, "fake provider min latency")
	maxMs := flag.Int("provider-max-ms", 40, "fake provider max latency")
	maxConns := flag.Int("max-conns", 16, "pgx pool size")
	crash := flag.Float64("relay-crash-rate", 0.2, "chance the outbox relay 'crashes' after publishing a batch but before marking it sent")
	flag.Parse()

	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxConns = int32(*maxConns)
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, schema); err != nil {
		log.Fatalf("schema: %v", err)
	}
	a := &app{db: db, prov: newProvider(*minMs, *maxMs), stale: 10 * time.Second}
	_ = db.QueryRow(ctx, "SHOW server_version").Scan(&a.pgVer)
	a.relay = startRelay(db, *crash)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /none/charges", a.handleNone)
	mux.HandleFunc("POST /naive/charges", a.handleNaive)
	mux.HandleFunc("POST /correct/charges", func(w http.ResponseWriter, r *http.Request) { a.handleCorrect(w, r, "correct") })
	mux.HandleFunc("POST /outbox/charges", func(w http.ResponseWriter, r *http.Request) { a.handleCorrect(w, r, "outbox") })
	mux.HandleFunc("POST /admin/reset", a.handleReset)
	mux.HandleFunc("GET /admin/stats", a.handleStats)

	log.Printf("listening on %s (postgres %s)", *addr, a.pgVer)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

// work returns a context that survives the client hanging up.
// This is how most servers behave in practice: the client times out, the server keeps going.
func work(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
}

func readReq(w http.ResponseWriter, r *http.Request) (key string, body []byte, req chargeReq, ok bool) {
	key = r.Header.Get("Idempotency-Key")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil || json.Unmarshal(body, &req) != nil || req.Account == "" || req.AmountCents <= 0 {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return "", nil, req, false
	}
	if key == "" {
		http.Error(w, `{"error":"Idempotency-Key header required"}`, http.StatusBadRequest)
		return "", nil, req, false
	}
	return key, body, req, true
}

func writeJSON(w http.ResponseWriter, code int, body []byte, replayed bool) {
	w.Header().Set("Content-Type", "application/json")
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// ---------------------------------------------------------------------------
// Mode 1: none. Every request moves money.

func (a *app) handleNone(w http.ResponseWriter, r *http.Request) {
	key, _, req, ok := readReq(w, r)
	if !ok {
		return
	}
	ctx, cancel := work(r)
	defer cancel()
	chID, err := a.prov.Charge(ctx, key, "", req.AmountCents)
	if err != nil {
		http.Error(w, `{"error":"provider"}`, http.StatusBadGateway)
		return
	}
	if _, err := a.db.Exec(ctx, `INSERT INTO charges (mode, request_ref, account, amount_cents, provider_charge_id)
		VALUES ('none', $1, $2, $3, $4)`, key, req.Account, req.AmountCents, chID); err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	body, _ := json.Marshal(chargeResp{chID, req.Account, req.AmountCents, "succeeded"})
	writeJSON(w, http.StatusCreated, body, false)
}

// ---------------------------------------------------------------------------
// Mode 2: naive check-then-insert. Looks right, reads right, races.

func (a *app) handleNaive(w http.ResponseWriter, r *http.Request) {
	key, _, req, ok := readReq(w, r)
	if !ok {
		return
	}
	ctx, cancel := work(r)
	defer cancel()

	// 1. check
	var stored []byte
	err := a.db.QueryRow(ctx, `SELECT response FROM naive_keys WHERE key = $1 LIMIT 1`, key).Scan(&stored)
	if err == nil {
		writeJSON(w, http.StatusOK, stored, true)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}

	// 2. act. Every concurrent duplicate that got past step 1 lands here.
	chID, err := a.prov.Charge(ctx, key, "", req.AmountCents)
	if err != nil {
		http.Error(w, `{"error":"provider"}`, http.StatusBadGateway)
		return
	}
	body, _ := json.Marshal(chargeResp{chID, req.Account, req.AmountCents, "succeeded"})

	// 3. remember
	tx, err := a.db.Begin(ctx)
	if err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO charges (mode, request_ref, account, amount_cents, provider_charge_id)
		VALUES ('naive', $1, $2, $3, $4)`, key, req.Account, req.AmountCents, chID); err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	if _, err := tx.Exec(ctx, `INSERT INTO naive_keys (key, response) VALUES ($1, $2)`, key, body); err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, body, false)
}

// ---------------------------------------------------------------------------
// Mode 3 (and 4): claim the key first, with the database deciding who wins.

func fingerprint(r *http.Request, body []byte) string {
	h := sha256.New()
	h.Write([]byte(r.Method + " " + r.URL.Path + "\n"))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func (a *app) handleCorrect(w http.ResponseWriter, r *http.Request, mode string) {
	key, body, req, ok := readReq(w, r)
	if !ok {
		return
	}
	ctx, cancel := work(r)
	defer cancel()
	fp := fingerprint(r, body)

	// 1. Claim. The primary key makes this atomic: exactly one request inserts the row.
	tag, err := a.db.Exec(ctx, `INSERT INTO idempotency_keys (account, key, fingerprint, status)
		VALUES ($1, $2, $3, 'in_progress')
		ON CONFLICT (account, key) DO NOTHING`, req.Account, key, fp)
	if err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}

	if tag.RowsAffected() == 0 {
		// 2. Somebody already has this key. Look at what they left.
		var (
			storedFP, status string
			code             *int
			stored           []byte
			isStale          bool
		)
		err := a.db.QueryRow(ctx, `SELECT fingerprint, status, response_code, response_body,
				locked_at < now() - make_interval(secs => $3)
			FROM idempotency_keys WHERE account = $1 AND key = $2`,
			req.Account, key, a.stale.Seconds()).Scan(&storedFP, &status, &code, &stored, &isStale)
		if errors.Is(err, pgx.ErrNoRows) {
			// owner gave up and released the key (provider error); let the client retry
			writeJSON(w, http.StatusConflict, []byte(`{"error":"retry"}`), false)
			return
		}
		if err != nil {
			http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
			return
		}
		switch {
		case storedFP != fp:
			writeJSON(w, http.StatusUnprocessableEntity,
				[]byte(`{"error":"Idempotency-Key reused with a different request body"}`), false)
			return
		case status == "done":
			writeJSON(w, *code, stored, true)
			return
		case !isStale:
			writeJSON(w, http.StatusConflict, []byte(`{"error":"request with this key is in progress"}`), false)
			return
		}
		// The owner has held the key too long (crashed?). Try to take it over.
		// Safe only because we pass the same key to the provider below.
		tag, err := a.db.Exec(ctx, `UPDATE idempotency_keys SET locked_at = now()
			WHERE account = $1 AND key = $2 AND status = 'in_progress'
			  AND locked_at < now() - make_interval(secs => $3)`, req.Account, key, a.stale.Seconds())
		if err != nil || tag.RowsAffected() == 0 {
			writeJSON(w, http.StatusConflict, []byte(`{"error":"request with this key is in progress"}`), false)
			return
		}
	}

	// 3. We own the key. Call the provider with the same key, so a retry after a
	//    crash here cannot move money twice.
	chID, err := a.prov.Charge(ctx, key, req.Account+":"+key, req.AmountCents)
	if err != nil {
		// Released so the client can retry. A real system would keep it if the
		// outcome is unknown and rely on the provider key during reclaim.
		_, _ = a.db.Exec(ctx, `DELETE FROM idempotency_keys WHERE account = $1 AND key = $2 AND status = 'in_progress'`, req.Account, key)
		http.Error(w, `{"error":"provider"}`, http.StatusBadGateway)
		return
	}
	resp, _ := json.Marshal(chargeResp{chID, req.Account, req.AmountCents, "succeeded"})

	// 4. Record the charge, the stored response (and the outbox event) in one transaction.
	tx, err := a.db.Begin(ctx)
	if err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO charges (mode, request_ref, account, amount_cents, provider_charge_id)
		VALUES ($1, $2, $3, $4, $5)`, mode, key, req.Account, req.AmountCents, chID); err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	if mode == "outbox" {
		evt, _ := json.Marshal(map[string]any{"type": "charge.succeeded", "charge_id": chID, "account": req.Account, "amount_cents": req.AmountCents})
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (event_id, payload) VALUES ($1, $2)`, "evt_"+chID, evt); err != nil {
			http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
			return
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET status = 'done', response_code = 201, response_body = $3
		WHERE account = $1 AND key = $2`, req.Account, key, resp); err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, `{"error":"db"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, resp, false)
}

// ---------------------------------------------------------------------------
// Outbox relay and two consumers.
//
// The relay reads unsent rows, publishes them, then marks them sent. If it
// "crashes" between publish and mark (relay-crash-rate), the batch is published
// again on the next pass. That is at-least-once delivery, on purpose.

type event struct {
	ID      string
	Payload []byte
}

type relay struct {
	published atomic.Int64
	rollbacks atomic.Int64
	naiveCh   chan event
	dedupeCh  chan event
	inflight  atomic.Int64
}

func startRelay(db *pgxpool.Pool, crashRate float64) *relay {
	rl := &relay{naiveCh: make(chan event, 20000), dedupeCh: make(chan event, 20000)}
	go rl.run(db, crashRate)
	go rl.consumeNaive(db)
	go rl.consumeDedupe(db)
	return rl
}

func (rl *relay) run(db *pgxpool.Pool, crashRate float64) {
	ctx := context.Background()
	for {
		n, err := rl.pass(ctx, db, crashRate)
		if err != nil {
			log.Printf("relay: %v", err)
		}
		if n == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (rl *relay) pass(ctx context.Context, db *pgxpool.Pool, crashRate float64) (int, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id, event_id, payload FROM outbox
		WHERE sent_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, err
	}
	var ids []int64
	var evts []event
	for rows.Next() {
		var id int64
		var e event
		if err := rows.Scan(&id, &e.ID, &e.Payload); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		evts = append(evts, e)
	}
	rows.Close()
	if len(evts) == 0 {
		return 0, nil
	}
	for _, e := range evts { // publish to the "broker"
		rl.inflight.Add(2)
		rl.naiveCh <- e
		rl.dedupeCh <- e
		rl.published.Add(1)
	}
	if rand.Float64() < crashRate {
		rl.rollbacks.Add(1)
		return len(evts), nil // crash: published, never marked sent. Deferred rollback.
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, err
	}
	return len(evts), tx.Commit(ctx)
}

// consumeNaive sends a receipt for every delivery it sees.
func (rl *relay) consumeNaive(db *pgxpool.Pool) {
	ctx := context.Background()
	for e := range rl.naiveCh {
		if _, err := db.Exec(ctx, `INSERT INTO receipts (consumer, event_id) VALUES ('naive', $1)`, e.ID); err != nil {
			log.Printf("naive consumer: %v", err)
		}
		rl.inflight.Add(-1)
	}
}

// consumeDedupe records the event id and the side effect in one transaction.
func (rl *relay) consumeDedupe(db *pgxpool.Pool) {
	ctx := context.Background()
	for e := range rl.dedupeCh {
		if err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, e.ID)
			if err != nil || tag.RowsAffected() == 0 {
				return err // seen before: do nothing
			}
			_, err = tx.Exec(ctx, `INSERT INTO receipts (consumer, event_id) VALUES ('dedupe', $1)`, e.ID)
			return err
		}); err != nil {
			log.Printf("dedupe consumer: %v", err)
		}
		rl.inflight.Add(-1)
	}
}

// ---------------------------------------------------------------------------
// Admin

func (a *app) handleReset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// wait for the outbox pipeline to go quiet before wiping it
	for i := 0; i < 200 && a.relay.inflight.Load() > 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := a.db.Exec(ctx, `TRUNCATE charges, naive_keys, idempotency_keys, outbox, processed_events, receipts RESTART IDENTITY`); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.prov.reset()
	a.relay.published.Store(0)
	a.relay.rollbacks.Store(0)
	w.WriteHeader(http.StatusNoContent)
}

type stats struct {
	Postgres         string `json:"postgres"`
	ProviderCalls    int64  `json:"provider_calls"`
	ProviderUnique   int64  `json:"provider_unique_requests"`
	ProviderReplayed int64  `json:"provider_replayed"`
	ChargeRows       int64  `json:"charge_rows"`
	ChargeUniqueRefs int64  `json:"charge_unique_requests"`
	OutboxRows       int64  `json:"outbox_rows"`
	OutboxUnsent     int64  `json:"outbox_unsent"`
	Published        int64  `json:"events_published"`
	RelayCrashes     int64  `json:"relay_crashes"`
	ReceiptsNaive    int64  `json:"receipts_naive_consumer"`
	ReceiptsDedupe   int64  `json:"receipts_dedupe_consumer"`
	ConsumerInflight int64  `json:"consumer_inflight"`
}

func (a *app) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var s stats
	s.Postgres = a.pgVer
	s.ProviderCalls, s.ProviderUnique, s.ProviderReplayed = a.prov.stats()
	err := a.db.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM charges),
		(SELECT count(DISTINCT request_ref) FROM charges),
		(SELECT count(*) FROM outbox),
		(SELECT count(*) FROM outbox WHERE sent_at IS NULL),
		(SELECT count(*) FROM receipts WHERE consumer = 'naive'),
		(SELECT count(*) FROM receipts WHERE consumer = 'dedupe')`).
		Scan(&s.ChargeRows, &s.ChargeUniqueRefs, &s.OutboxRows, &s.OutboxUnsent, &s.ReceiptsNaive, &s.ReceiptsDedupe)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.Published = a.relay.published.Load()
	s.RelayCrashes = a.relay.rollbacks.Load()
	s.ConsumerInflight = a.relay.inflight.Load()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s)
}
