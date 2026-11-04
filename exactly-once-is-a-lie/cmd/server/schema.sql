-- Recreated on every server start. Lab data only.
DROP TABLE IF EXISTS charges, naive_keys, idempotency_keys, outbox, processed_events, receipts;

-- One row per charge we think we made.
-- request_ref is the client's Idempotency-Key, stored ONLY so the lab can count
-- duplicates. It is deliberately not unique.
CREATE TABLE charges (
  id                 bigserial PRIMARY KEY,
  mode               text        NOT NULL,
  request_ref        text        NOT NULL,
  account            text        NOT NULL,
  amount_cents       int         NOT NULL,
  provider_charge_id text        NOT NULL,
  created_at         timestamptz NOT NULL DEFAULT now()
);

-- naive mode: a lookup table with an index but NO unique constraint.
-- "SELECT first, INSERT later" is the whole design.
CREATE TABLE naive_keys (
  key        text        NOT NULL,
  response   jsonb       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX naive_keys_key ON naive_keys (key);

-- correct mode: the key is the primary key, scoped per account.
CREATE TABLE idempotency_keys (
  account       text        NOT NULL,
  key           text        NOT NULL,
  fingerprint   text        NOT NULL,              -- sha256 of method + path + body
  status        text        NOT NULL CHECK (status IN ('in_progress', 'done')),
  response_code int,
  response_body jsonb,
  locked_at     timestamptz NOT NULL DEFAULT now(), -- for reclaiming a key whose owner crashed
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  PRIMARY KEY (account, key)
);

-- transactional outbox: written in the same transaction as the charge.
CREATE TABLE outbox (
  id       bigserial PRIMARY KEY,
  event_id text        NOT NULL UNIQUE,
  payload  jsonb       NOT NULL,
  sent_at  timestamptz
);
CREATE INDEX outbox_unsent ON outbox (id) WHERE sent_at IS NULL;

-- consumer-side dedupe.
CREATE TABLE processed_events (
  event_id     text        PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);

-- one row per receipt email "sent", per consumer.
CREATE TABLE receipts (
  id       bigserial PRIMARY KEY,
  consumer text NOT NULL,
  event_id text NOT NULL
);
