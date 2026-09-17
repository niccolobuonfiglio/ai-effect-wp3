# Data Protection in the Orchestrator

The orchestrator is a control plane: it routes **references** to data, not the
data itself. Datasets stay in each node's own storage (a shared volume, S3, an
HTTP endpoint) and are read directly by the services. This document describes
the measures that keep it that way, and states plainly what they do not cover.

## What the orchestrator can hold

| Data | Where | Protection |
|------|-------|------------|
| Workflow and task state, graph, queues | Redis | Access control through the orchestrator API key |
| Data references (`protocol`, `uri`, `format`) | Redis, API responses | Locations, not content. Inline payloads are capped and masked (below) |
| Inline payloads (`protocol: "inline"`) | Redis, API responses | Size-capped, maskable, and can be disabled |
| Error messages from services | Task state, service health, `alerts.jsonl` | Redacted and length-capped |
| Per-workflow services API key | Redis | Encrypted when `ORCHESTRATOR_SECRET` is set |

## 1. Inline payload limits

`protocol: "inline"` carries base64 data inside the reference, so it is stored
in Redis and returned by the API. It is meant for small configuration values,
not datasets and never for personal data.

- **Submission:** an input above the limit is rejected with `400`, and the
  workflow is not created.
- **Service output:** a task whose service returns an oversized inline payload
  fails with a clear message. The payload is not stored.
- **`ALLOW_INLINE=false`** rejects inline references entirely. Note that the
  `file_based_energy_pipeline` use case sends its configuration inline, so it
  needs `ALLOW_INLINE=true`.

## 2. Masking in API responses

`GET /workflows/{id}/tasks` and `GET /workflows/{id}/tasks/{task_id}` replace an
inline payload with a placeholder:

```json
{"protocol": "inline", "uri": "<inline data, 412 bytes>", "format": "csv", "metadata": {}}
```

Workers keep using the stored value, so execution is unaffected. References that
are locations (`file`, `s3`, `http`, …) are shown unchanged: they are needed to
follow a workflow, and they are not the data.

## 3. Error sanitising

Every error stored on a task, on a workflow, in service health (`last_error`)
and in alerts passes through one function (`services/privacy.py`):

| Pattern | Action |
|---------|--------|
| Email addresses | replaced with `[redacted]` |
| Runs of 9 or more digits (national ids, phone numbers, card-like numbers) | `[redacted]` |
| Base64-looking blobs of 40+ characters that mix case and digits | `[redacted]` |
| Anything longer than `MAX_ERROR_CHARS` (default 500) | truncated with ` ... [truncated]` |

Ordinary messages are left alone, including file paths and short numbers, so
`Connection failed: [Errno 111] Connection refused` stays readable.

## 4. Encrypted services API key

`services_key:{workflow_id}` is encrypted with Fernet (AES-CBC + HMAC) using a
key derived from `ORCHESTRATOR_SECRET`, and stored with an `enc:v1:` prefix.

- Without `ORCHESTRATOR_SECRET` the value is stored in plain text, as before,
  and a warning is logged at startup.
- Decryption always accepts plain text, so values written before the secret was
  configured keep working.
- If the secret changes, the old value cannot be read: the worker logs
  `services_key_unreadable` and calls the service without a key.

Generate one with `openssl rand -hex 32`. It protects against a Redis dump or an
exposed Redis port, **not** against someone who can read the orchestrator's
environment.

## 5. HTTPS to the services

Service URLs are built from the endpoint's scheme instead of a hardcoded
`http://`:

- per entry in `dockerinfo.json`: `{"container_name": "svc", "ip_address": "svc", "port": "8443", "scheme": "https"}`
- or for all entries: `SERVICE_SCHEME=https`
- `SERVICE_TLS_VERIFY` accepts `true` (default), `false`, or a path to a CA
  bundle for private certificates.

This makes encrypted service calls possible; the services must actually serve
TLS for it to take effect.

## Configuration

| Variable | Default | Effect |
|----------|---------|--------|
| `ORCHESTRATOR_SECRET` | unset | Encrypts the stored services API key |
| `ALLOW_INLINE` | `true` | `false` rejects every inline reference |
| `MAX_INLINE_BYTES` | `65536` | Largest accepted inline payload |
| `MAX_ERROR_CHARS` | `500` | Length cap for stored error text |
| `SERVICE_SCHEME` | `http` | Default scheme for service URLs |
| `SERVICE_TLS_VERIFY` | `true` | TLS verification, or a CA bundle path |

## Verified live (2026-09-17)

Checked on the running Docker stack (commit `8dd92c1` plus these changes) with
the `file_based_energy_pipeline` use case, which is the one that actually sends
inline data, and with `germany-node`. Unit and integration suites: 512 + 93
passing.

### No regression on the normal path

```bash
cd use-cases/file_based_energy_pipeline && ./start.sh && ./submit-workflow.sh
curl -s localhost:18000/workflows/<id>/tasks | jq -c '.tasks[] | {node_key, status, input_refs}'
```

`wf-db3bf8e6ed6b` → `completed`, all 4 tasks `completed`. The inline
configuration still reached `data-generator`, which produced
`/app/data/raw_energy.csv`. File references are shown unchanged.

### Masking

The same response shows the inline payload as a placeholder, while the service
received the real bytes:

```json
{"node_key":"data-generator:GenerateData","status":"completed",
 "input_refs":[{"protocol":"inline","uri":"<inline data, 43 bytes>","format":"json","metadata":{}}]}
```

### Services key encryption

```bash
ORCHESTRATOR_SECRET=$(openssl rand -hex 32) docker compose up -d
# submit with "services_api_key": "test-key-123"
docker compose exec redis redis-cli get "services_key:<id>"
```

| Setting | Stored value | Workflow |
|---------|--------------|----------|
| `ORCHESTRATOR_SECRET` set | `enc:v1:gAAAAABqrFFW2ysSXgZ_-S0j…` - no trace of the key | `wf-ad9b51b300ce` → `completed`, so the worker decrypted and used it |
| no secret | `"test-key-123"` | as before this change |

The API logs the active settings at startup:
`Data policy: allow_inline=true max_inline_bytes=65536; service scheme=http; services key encryption=on`.

### Inline size cap

With a deliberately tiny cap (`MAX_INLINE_BYTES=8`):

| Where | Result |
|-------|--------|
| Submission | `HTTP 400` - `Invalid inputs: inline payload of 500 bytes exceeds the limit of 8 bytes; pass data by reference instead`; no workflow created |
| Service output | `input-provider:GetConfiguration` **failed** - `Service 'input-provider' returned data that breaks the orchestrator data policy: inline payload of 43 bytes exceeds the limit of 8 bytes`; the three downstream tasks stayed `pending`, and nothing was stored |

Back at the default 64 KiB, the same pipeline completes again
(`wf-2cf55c19e486`).

### Breaker is not tripped by service-reported failures

In a `germany-node` run where `villas-chronics` reported that its VILLASnode
dependency had crashed, all three services stayed `HEALTHY` / `closed` /
`failure_count: 0` and no `SERVICE DOWN` alert was emitted. The workflow failed,
as it should, but the reachable service was not marked down. See
[RUN_ANALYSIS_GERMANY_NODE.md](RUN_ANALYSIS_GERMANY_NODE.md) run 2 for the
behaviour before this fix.

### Not yet verified live

`ALLOW_INLINE=false`, `SERVICE_SCHEME=https` (needs a service serving TLS) and
error redaction (needs a service that puts an identifier in an error message).
All three are covered by unit tests in `tests/unit/test_privacy.py`.

## What this does not cover

- **Transport encryption is a deployment task.** The orchestrator can *call*
  services over HTTPS, but terminating TLS on its own API needs a reverse proxy
  (nginx, Traefik, Caddy) or uvicorn TLS options plus a certificate.
- **A service can still produce sensitive data.** Sanitising limits how far it
  travels; it cannot prevent it. Services should keep personal data out of error
  messages and out of inline references.
- **The portal stores its copies in plain text.** `orchestrator_api_key` and
  `services_api_key` are plain columns in the portal's PostgreSQL database.
  That is outside this component.
- **One key, full access.** The API key grants read and delete on every workflow
  of an instance. There are no users, roles or audit trail.
- **Redis is not persistent and has no TTL.** Workflow metadata lives until the
  container restarts or `DELETE /workflows/{id}` is called; log and alert files
  rotate after 7 days. Neither is a defined retention policy.
