# Measurement & Reproduction Protocol — `hanzoai/cloud#30` (`tau2-bench` Dual-Control)

Everything in **§1–§3** runs offline in **< 2 minutes** on any Linux, macOS, or Windows host with Go $\ge 1.22$ (`CGO_ENABLED=0`, zero external dependencies, zero paid API keys). **§4** documents the optional live Enso / Zen / OpenAI-compatible model evaluation loop.

---

## 0. Prerequisites

- Go $\ge 1.22$ (`go version`; validated on `go1.26.11`)
- No C compiler required (`CGO_ENABLED=0`)
- No network access or paid LLM API keys required for §1–§3

---

## 1. Build, Static Analysis & Full Verification Suite (~5 seconds)

From the repository root:

```bash
CGO_ENABLED=0 go vet ./research/...
CGO_ENABLED=0 go test -v -count=1 ./research/...
CGO_ENABLED=0 go build -o tau2dualsim ./research/tau2dualsim
```

Expected:
- `26` unit, concurrency, RBAC, BOLA, multi-tenant, tamper-replay, LLM-adapter, schema, and drift-lock tests pass in `research/tau2dualcontrol`.
- `1` end-to-end CLI build-and-JSON test passes in `research/tau2dualsim` (`27` tests total).
- `TestCommittedResultsJSONMatchesLiveCode` verifies bit-for-bit equality between live `RunMatrix(500)` execution and committed `research/tau2dualcontrol/testdata/results.json`.

*(Optional race-detector check on hosts with a C toolchain installed: `CGO_ENABLED=1 go test -race ./research/...`.)*

---

## 2. Regenerate the 36-Cell Benchmark Matrix (~1 second)

```bash
./tau2dualsim -episodes 500 -out research/tau2dualcontrol/testdata/results.json
```

This runs:
- `3` contention regimes (`low_interleaving`, `medium_interleaving`, `high_contention_faults`)
- $\times$ `4` coordination policies (`single_control`, `dual_uncoordinated`, `dual_rbac_lww`, `dual_cas_lease`)
- $\times$ `3` deterministic `SplitMix64` seeds (`20261101`, `20261102`, `20261103`)
- $\times$ `500` episodes/cell = **`18,000` episodes** across `5` business/telecom task families, with inline SHA-256 hash-chained `ReplayAndVerify` on every episode
- Plus **`400` real overlapping goroutine race trials** (`200` on `dual_rbac_lww`, `200` on `dual_cas_lease`).

---

## 3. Verify Determinism Across Runs

```bash
./tau2dualsim -episodes 500 -out /tmp/tau2-run-a.json
./tau2dualsim -episodes 500 -out /tmp/tau2-run-b.json
diff -u /tmp/tau2-run-a.json /tmp/tau2-run-b.json
```

Expected: empty diff (`exit 0`).

---

## 4. Optional Live Model Cross-Check Against Hanzo Enso / Zen (`/v1/chat/completions`)

`research/tau2dualcontrol/llm_adapter.go` provides `OpenAIToolAgent`, a fail-closed standard-library HTTP client targeting any OpenAI-compatible chat-completions gateway (such as **Hanzo Enso** routing to **Zen** checkpoints).

1. When unconfigured (`BaseURL == ""` or `Model == ""`), `NewOpenAIToolAgent` fails closed with `ErrCodeLLMUnconfigured` (`layer="ai_gateway"`, 503-equivalent per `LLM.md` Architecture Invariant #4).
2. Offline deterministic verification of the multi-turn tool-calling CAS recovery loop (`stale expected_version=1` $\rightarrow$ `version_conflict` $\rightarrow$ `read_tenant_state` $\rightarrow$ `expected_version=2` commit) runs automatically inside `go test -run TestOpenAIToolAgent ./research/tau2dualcontrol/...` using `net/http/httptest`.
3. To evaluate a live Enso / Zen endpoint in custom Go harnesses, construct `OpenAIToolAgent` with `OpenAICompatConfig{BaseURL: os.Getenv("ENSO_BASE_URL"), APIKey: os.Getenv("ENSO_API_KEY"), Model: "zen-4-instruct"}` and invoke `ExecuteTurnLoop` while concurrent `Principal{Role: RoleCustomer}` operations mutate shared `SharedStateStore` records.
