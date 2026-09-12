# notify — Specification

This document reflects decisions made during design discussion — it is
kept in sync with the implementation as it's built (see the status note at
the end of §18), not a one-time snapshot. Items not yet decided are marked
**(open)** and collected again in §18 rather than silently guessed at.

## 1. Purpose

A central notification-sending service. Decouples "send an email" from the
services that need to (starting with `aham`, which currently `log.info`s
tokens instead of emailing them — see its spec's §13) behind a queued,
retried, audited pipeline. Channel today is email only; the design leaves
room for other channels later without being built for them now.

Internal-only. Not exposed outside the private network; callers are
services that know about it. No auth/guards for v1 — see §14, §16.

## 2. Tech stack

- **Runtime:** Bun for local dev; code stays Node-compatible (global rule —
  no `Bun.*`-only APIs without a portable fallback).
- **Framework:** Elysia, same as `aham`.
- **Database:** PostgreSQL, via Drizzle ORM + `@rniverse/connectors/sql`
  (`SQLConnector`) — same as `aham`.
- **Queue:** Kafka wire protocol via `@rniverse/connectors/redpanda`
  (`RedpandaConnector`, built on `kafkajs`) — confirmed protocol-generic,
  not Redpanda-specific, so it runs against real Kafka brokers unchanged
  (in use here against an Aiven-hosted Kafka cluster, not Redpanda itself).
- **Email provider:** `resend`, wrapped behind an internal interface (not
  called directly from service code) so a second provider or a swap later
  touches one file — `aham` will stop calling `resend` directly once this
  service exists and will call `notify` instead.
- **Shared libs:** `@rniverse/utils` — `log`, `ulid`, `retry`, `Result`,
  same as `aham`/`connectors`.
- **Validation:** valibot, same as `aham` (Elysia 1.4+ Standard Schema
  support).
- **Lint/format:** Biome — config copied verbatim from `aham`.

## 3. Core architecture decision: DB-backed fallback at every failure point

Every async notification is buffered through Kafka, but a Kafka outage or a
provider failure must never silently lose it. The rule: **the only place
that writes to Postgres *before* a Kafka publish is a publish failure
itself.** On the happy path (publish succeeds), nothing is written until
the consumer handles the message — so there is exactly one writer per
notification at every point in time, never two contending over the same
row.

This is why the consumer must **commit its Kafka offset after handling a
message regardless of outcome** (sent, or recorded as `pending` /
`failed_notifications`) — it's what makes the retry cron the sole owner of
every subsequent attempt. Without it, a message could still be sitting
un-acked in Kafka while the cron is simultaneously retrying the same row
from Postgres.

**Idempotency** is anchored on one id, not on write ordering:
`notification_id` is generated once, in the API layer (ULID, same
convention as `aham`'s `users.id` etc.), before anything else happens. It
flows unchanged into the Kafka payload, into every Postgres upsert, and
into Resend's idempotency key on every send attempt (consumer *and* retry
cron). That last one is the actual backstop — even the crash-mid-handling
case (consumer sends, dies before the upsert, Kafka redelivers) can't
double-send an email, because Resend itself no-ops the second call under
the same idempotency key. The DB "check if already `sent` before sending"
step is a cheap first-line skip, not the thing preventing duplicates.

## 4. Naming conventions

Same as `aham/spec.md §4` — single word is the default and preferred form,
no vague 2-word verb-noun/noun-noun blends, `service$domain` /
`utils$domain` for public surfaces, `__name` for file-private detail,
camelCase locals, snake_case reserved for DB columns and JSON keys. Layer
split (api / service / util / db / connections / middlewares / schema),
not nested by route — `tsconfig.json`'s path aliases already carry this
shape over from `aham`.

## 5. Database schema

| Table | Columns | Notes |
|---|---|---|
| `notifications` | `id` (ULID, PK — the `notification_id`), `channel` (`'email'` for now, see §6), `payload` (json, shape depends on `channel` — see §6), `status` (`'pending' \| 'sent'`), `current_attempt` (int, default `0`), `max_attempts` (int, default `1`), `send_before` (nullable timestamp), `created_at`, `updated_at` | Row exists here from the moment either (a) an async publish fails, (b) the consumer/cron first touches it, or (c) a sync send is attempted (§9). `send_before` empty/null never expires the row on time — only `current_attempt` vs `max_attempts` applies. |
| `failed_notifications` | Same shape as `notifications`, plus `moved_at` | Terminal. A row is moved here (not copied) once `current_attempt >= max_attempts` or `send_before` has passed, whichever trips first. |
| `failures` | `id` (PK), `notification_id` (simple index, not a FK — confirmed), `reason`, `attempt` (int), `created_at` | Append-only audit of every failed attempt, by consumer, cron, or sync send. This is what the nightly metrics cron reads — it is not the same thing as `failed_notifications`, which is current terminal state. |
| `config` | `id` (PK), `key` (string, **unique index**, not a unique constraint — deliberate), `value` (json), `context` (json, present but unused/ignored for now — meaning deferred), `created_at`, `updated_at` | Generic runtime-editable settings, e.g. the metrics-alert recipient list. |

IDs are ULIDs (`ulid.generate()` from `@rniverse/utils`), same as `aham`,
for the same reason: chronological sort order.

## 6. Notification channel & payload

`channel` is its own column, not buried inside `payload` — so it can be
indexed, filtered, and dispatched on directly (per-channel status queries,
per-channel provider routing, per-channel metrics grouping) without
decoding JSON first. `payload`'s internal shape varies *by* `channel`
(email: `to`, `subject`, `html`/`body`, `from?`; a future `sms`: `to`,
`body`; …) — validated in application code by a discriminated union keyed
on `channel` (valibot), not enforced at the DB level, consistent with how
`aham` keeps shape rules at the app boundary rather than in Postgres (e.g.
username lowercasing).

**One row = one channel = one send.** Sending the same logical event to
multiple channels (email *and* SMS for the same event) is the **caller's**
responsibility — call the send API once per channel, each getting its own
`notification_id`. Rejected the alternative (one call, `channels: [...]`,
fan out internally) because payload requirements differ enough per channel
that a shared object either becomes a superset-of-everything blob (losing
type safety — the same problem restated) or needs N child rows internally
anyway (re-deriving this design, just hidden); and a fan-out makes
`/notification/status/:id` and per-channel retry/attempt semantics
ambiguous — is the parent "sent" when all children succeed, or any? does a
failed SMS retry independently of a succeeded email?

This is what keeps `send()` (§7) genuinely generic: it takes
`{ channel, payload }` and dispatches internally to whichever provider
wrapper handles that channel. Adding `sms` later is a new arm in the
discriminated union plus a new provider wrapper — nothing upstream changes.

**Confirmed.** Discriminated union on `channel`: `{ channel: 'email',
payload: EmailPayload }`, later `{ channel: 'sms', payload: SMSPayload }`,
etc.

`EmailPayload`:

```ts
type EmailPayload = {
	to: string[];
	subject: string;
	html: string;
	from?: string; // optional — omit to use Resend's account default
};
```

`from` is optional and passed through as-given when present; when absent,
the Resend client's own default sender is used (`aham`'s `config.resend
.from` pattern — defaulting to Resend's sandbox sender when unset — is a
candidate for `notify` too, not yet decided).

## 7. `email.send()` vs `notification.send()`

Two distinct layers, not one function called two ways:

- **`email.send(payload)`** (and later `sms.send()`, etc. — one per
  channel) is the pure, channel-specific provider call. No DB, no attempts,
  no bookkeeping. This is the *only* thing that actually talks to Resend.
- **`notification.send(notification, opts?)`** is the orchestration layer.
  It dispatches to the right channel-level `send` based on `channel`,
  wires `notification_id` through as the provider idempotency key, and
  carries the business logic:
  - **Existence check first:** look up `notification_id` — if already
    `status: 'sent'`, no-op. This is *not* the duplicate-prevention
    mechanism (§3's Resend idempotency key is), just a cheap indexed
    lookup that skips a redundant provider call on the routine case
    (Kafka redelivery, or a cron re-touching an already-handled row).
    Lives here, once, rather than duplicated in each caller.
  - On success, upsert `notifications` → `sent`.
  - On failure, apply the attempt/expiry check (§11) and either upsert
    back to `pending` or move the row to `failed_notifications`, plus
    write a `failures` audit row.
  - `opts.source: 'async' | 'sync' | 'cron'` — which path triggered this
    attempt; audit/observability metadata only, doesn't change the
    attempt/expiry logic (that's the point of sharing one function).
    `opts` is optional; an untagged/ad-hoc call (script, test, REPL)
    defaults `source` to `'manual'` — a distinct 4th value, not silently
    mislabeled as one of the three real paths.

  **Confirmed — multi-instance coordination via Postgres advisory lock,
  not per-row claiming:** if `notify` runs more than one instance, each
  firing its own retry/metrics cron on the same schedule, the existence
  check alone doesn't stop two instances racing on the same row
  simultaneously. Rather than an atomic per-row claim
  (`UPDATE ... WHERE status = 'pending' ... RETURNING`), each cron wraps
  its entire run in `pg_try_advisory_xact_lock(<job key>)`: whichever
  instance's connection acquires it runs the job for that tick; every
  other instance gets `false` back immediately and skips the tick
  entirely. Transaction-scoped (`_xact_`), not session-scoped — releases
  automatically when the job's transaction ends, including on a crash, so
  nothing can leave a stuck lock. Two distinct integer keys, one per cron
  (retry, metrics) — **not hardcoded magic numbers.** Same pattern as
  `aham`'s `enums/errors.enum.ts` (`enum$error`): a `list` of symbolic
  keys in `enums/lock.enum.ts`, values auto-generated sequentially via
  `sync$seq.next.seq()` at module load (raw bigints, not the zero-padded
  string form `sync$seq` uses for error codes — `pg_try_advisory_xact_lock`
  takes a real `bigint`):
  ```ts
  const list = [['RETRY_CRON', '...'], ['METRICS_CRON', '...']] as const;
  const next_key = sync$seq.next.seq();
  export const enum$lock = {
    keys: Object.fromEntries(list.map(([k]) => [k, next_key()])),
  };
  ```
  Call sites reference `enum$lock.keys.RETRY_CRON`, never a bare number.

  **No new connector work needed for this** — `@rniverse/connectors/sql`'s
  `SQLConnector.getInstance()` already returns a Drizzle instance whose
  `.transaction(async tx => { ... })` (postgres-js driver) pins the whole
  callback to one held connection, which is what an advisory lock
  requires — run the `pg_try_advisory_xact_lock` raw-SQL check and the
  job's own queries through the same `tx` inside that callback. This is
  application-level usage of the existing connector, not a change to it.

`notification.send()` is what the **consumer** (§8), the **retry cron**
(§11), and the **sync API** (§9) all call — the same function, which is
why "cron and consumer use the same logic" holds structurally, not just by
convention. The **metrics cron** (§12) calls `email.send()` directly,
bypassing `notification.send()` entirely — no DB bookkeeping, since a
report about the pipeline's own failures shouldn't loop back through it.

## 8. Send flow — async

`POST /notification/send/async` (§13):

1. **API layer:** generate `notification_id`. Attempt to publish
   `{ id, channel, payload }` to the Kafka topic (`KAFKA_TOPIC_NAME`,
   unkeyed — §10). On publish failure only: insert a `notifications` row,
   `status: 'pending'`, `current_attempt: 0`. Respond
   `{ ok: true, data: { status: 'accepted', id } }` either way — this means
   "accepted into the pipeline," not "sent."
2. **Consumer**, on message receipt:
   - Look up `notifications` by id. If already `status: 'sent'`, no-op —
     commit the offset and move on (cheap skip; not the correctness
     mechanism, see §3).
   - Otherwise call `notification.send(notification, opts)` (§7) —
     `notification_id` goes through as the idempotency key.
     - **Success:** row is upserted `status: 'sent'` (inside
       `notification.send()` itself, not by the consumer).
     - **Failure:** attempt/expiry check (§11) applied inside
       `notification.send()` — increment `current_attempt`; either upsert
       back to `pending` (attempts remain and not expired) or move the row
       to `failed_notifications` (exhausted). Either branch also inserts a
       `failures` audit row with the reason.
   - **Commit the Kafka offset after handling, unconditionally** — this is
     the mechanism from §3, not optional.

**Poison-message guard, before any of the above:** a message that isn't
valid JSON, or is valid JSON that doesn't match the notification shape
(`id`/`channel`/`payload`), can never become valid by being redelivered —
unlike a genuine infra error, retrying it forever just crash-loops the
consumer. Validated against `schema$notification.message` (the send schema
plus `id`) before dispatch; on failure, recorded directly into
`failed_notifications` (`max_attempts: 0` — "rejected before any attempt
was made", distinct from a row that tried and ran out of attempts) plus a
`failures` row with the parse/validation reason, `channel: 'unknown'` if
none could be read off the payload. The offset still commits — same
"never rethrow for something that can't self-resolve" principle as above.

## 9. Send flow — sync

`POST /notification/send/sync` (§13): generate `notification_id`, call
`notification.send(notification, opts)` (§7) immediately, inline, no Kafka
involved — same bookkeeping as §8's consumer path, just triggered directly
by the request instead of by a Kafka message. With `max_attempts` defaulting
to `1`, a sync failure normally lands directly in `failed_notifications`,
not `pending`.

**Confirmed:** the HTTP response reflects the real outcome —
`{ ok: true, data: { status: 'sent' | 'failed', id } }` — since it's
already known before responding, unlike the async endpoint's generic
`accepted`.

## 10. Partitioning

No partition key. Confirmed: no ordering guarantee is required between any
two notifications, including two to the same recipient — so messages are
left unkeyed for maximum spread across consumers rather than keyed by
recipient.

## 11. Retry cron

Interval sourced from an env var, in **minutes** (var name **open**).
Pulls `pending` rows from `notifications`. For each, applies the *same*
attempt/expiry logic as the consumer (§8) — one shared function, not
reimplemented twice, since it's the one place the "expired vs
attempts-exhausted" rule lives — and calls `notification.send()` (§7)
directly, bypassing Kafka entirely.
Exactly one attempt per row per cron run — never loop attempts within a
single run. Success → `sent`. Failure → same branch as §8 (back to
`pending` with incremented attempt, or moved to `failed_notifications`),
with a `failures` audit row either way.

Because the consumer always commits its offset (§3), a row picked up here
was never going to be retried by Kafka redelivery too — the cron is the
sole retrier for anything past the first attempt.

## 12. Metrics cron

Runs nightly **(open — exact time/schedule)**. Reads the `failures` table,
grouped by `reason` and by quarter-of-day bucket **(open — bucket
boundaries and timezone, and the day-window definition: calendar day vs.
rolling 24h)**. Sends only if the count is non-zero. Recipients come from
`config` (key **open**, e.g. `metrics_admin_emails`). Confirmed: this email
is sent by calling `email.send()` (§7) **directly** — bypassing
`notification.send()` entirely, so no DB bookkeeping, no Kafka, no retry —
since it's a report about the pipeline's own failures, not traffic
through it.

## 13. API surface

| Route | Notes |
|---|---|
| `POST /notification/send/async` | Kafka-buffered (§8). Response: `{ ok: true, data: { status: 'accepted', id } }`. |
| `POST /notification/send/sync` | Calls `notification.send()` inline, no Kafka (§9). Response: `{ ok: true, data: { status: 'sent' \| 'failed', id } }`. |
| `GET /notification/status/:id` | Looks up `id` in `notifications`, then `failed_notifications` — query both, whichever is efficient (implementation detail). **Confirmed:** a plain 404 for an id not (yet) in either table is fine, including during the brief async in-flight window before the consumer's first write — async callers are expected to know sending takes time and to wait/poll rather than treat an immediate 404 as authoritative. |

Response envelope otherwise reuses `aham`'s `{ ok: true, data }` /
`{ ok: false, error: { code, message } }` shape.

Note: `send/sync` and `send/async` nest a mode under the verb, which
doesn't have a precedent in `aham`'s flat `domain/entity/verb` convention
(§4) — a deliberate call for this service, not an oversight, just flagging
the divergence.

## 14. Auth

None. Internal-only, not exposed outside the private network — no guards,
no JWT verification, deliberately, for v1. See §16.

## 15. Config

Env vars needed, mirroring `aham`'s "one file (`config.ts`) reads
`process.env`, everything else takes it as an argument" rule:

- `PORT`, `NODE_ENV` — present.
- `DATABASE_URL` — present (points at a local `notify` DB). `.env.test`
  now exists, same pattern as `aham` (Bun auto-loads `.env.<NODE_ENV>` on
  top of `.env`): `DATABASE_URL` → `notify_test`, `LOG_LEVEL=silent`.
- `LOG_LEVEL` — present (`trace`).
- `NOTIFICATION_RETRY_INTERVAL_MINUTES` (sample: `5`) and
  `NOTIFICATION_METRICS_CRON_HOUR` (sample: `0`) — present with sample
  values; final values still open (§18), format itself is settled.
- Kafka broker config — **done.** `KAFKA_BOOTSTRAP_SERVERS`,
  `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`,
  `KAFKA_SASL_PASSWORD`, `KAFKA_CA_CERTIFICATE`, `KAFKA_TOPIC_NAME`,
  `KAFKA_GROUP_ID` (optional, defaults `notify-consumer`) are translated
  into `RedpandaConnectorConfig` shape inside `config.ts` itself (`ssl`/
  `sasl` split from the protocol, mechanism lowercased, the CA cert's file
  contents read rather than its path forwarded) — verified end to end
  against the real Aiven-hosted broker (connect, topic metadata, produce,
  consume all confirmed working).
- `RESEND_API_KEY` — present. `RESEND_FROM_EMAIL` — not yet added (`aham`
  defaults this to Resend's sandbox sender when unset; same default likely
  makes sense here).
- `.env.example` — updated to mirror `.env`'s current structure
  (`DATABASE_URL`, the two cron vars, `LOG_LEVEL`); Kafka vars intentionally
  left out of it for now, per the above.
- `AUTH_SERVICE_URL` — dropped from `.env`/`.env.example` (was leftover
  from copying `aham`'s file, vestigial per §14).

`connections/email.connection.ts` and `connections/redpanda.connection.ts`
both follow `aham`'s one-line-per-connector pattern — `config.ts` now
exists and both compile against it.

Plus the DB-backed `config` table (§5) for values that should be editable
without a redeploy (e.g. the metrics recipient list).

## 16. Deliberately deferred (not built, not in scope for this version)

- Auth / request guards on the send API (§14) — internal-network-only for
  now.
- Any channel beyond email (SMS, WhatsApp, Telegram, in-app, …) — the
  `channel` column (§6) means adding one later doesn't require a schema
  change, but none beyond email is implemented now.
- A templating/branding layer for email content — content is passed in as
  given, no shared layout system.
- Rate limiting.
- A second email provider — the `email.send()` boundary (§7) exists so this
  is a later, one-file change, not a rebuild.
- CORS — not applicable while nothing outside the private network calls
  this service.

## 17. Tests

Not yet written. Intended shape, mirroring `aham`: integration tests
exercised via `app.handle(new Request(...))`, `import.meta.main` guarding
server startup so tests can import without binding a port, `bun run test`
(not bare `bun test`) so any `pretest` DB setup fires.

## 18. Open decisions

1. **`config.context` (§5)** — deferred/ignored per your call, not
   blocking; may get a meaning or get dropped later.
2. **Final retry-cron interval / metrics-cron hour values** — sample
   values (`5` minutes, hour `0`) are in `.env` now; not necessarily final.
3. **Metrics-cron day-window/bucket details** (§12) — day-window
   definition (calendar day vs. rolling 24h), quarter-of-day bucket
   boundaries + timezone, and the `config` key name for the recipient
   list.
4. **`KAFKA_TOPIC_NAME` is still the placeholder `test`** — broker config
   translation and `.env.example`'s Kafka section (still intentionally
   omitted, §15) are the only pieces of the old "Kafka setup" item left;
   everything else it used to block is now built (below).

**Built and verified** (typechecks, boots, connects to Postgres and Kafka,
migration applied, smoke-tested against the real broker — including a
deliberately malformed message and a deliberate double-SIGINT):
`config.ts`, `db/schema.ts` (migration generated — `config`'s unique index
and `failures`' plain index came out exactly as specified),
`connections/{postgres,email,redpanda,index}.ts` (redpanda `required:
false` in health — sync/status keep working if Kafka is down),
`services/{error,email,notification}.service.ts` (`send()`, the poison-
message-safe `consume()` with a `GROUP_JOIN` log so "hadn't joined yet" is
visible instead of guessed, and the `reject()` path that records a
malformed message into `failed_notifications`/`failures` instead of
crash-looping the consumer), `schema/api/notification.schema.ts`
(includes the Kafka wire-format schema used by that guard),
`api/{notification.api,index}.ts` (all three routes: `send/sync`,
`send/async`, `status/:id`), `enums/lock.enum.ts` (defined, not yet
consumed — the crons that would use it aren't built),
`middlewares/log.middleware.ts`, `index.ts` (idempotent shutdown — safe
under a genuine double signal delivery, confirmed by test), `drizzle
.config.ts`. **Not built yet:** the retry and metrics crons (§11, §12) —
still next, and not blocked on item 4 above (they don't need Kafka at
all, per §7/§11/§12).
