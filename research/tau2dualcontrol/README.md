# Dual-Control Business-Agent Benchmark ($\tau^2$-Bench) — Reproducible Experiment (`cloud#30`)

**Bounty**: [hanzoai/cloud#30](https://github.com/hanzoai/cloud/issues/30) — Research Bounty  
**Tracking HIPs**: [HIP-1348](https://github.com/hanzoai/hips/blob/main/HIPs/hip-1348-reliable-economical-and-verifiable-ai-infrastructure.md) (*issue label `hip-1348`, §1.1–§1.3*) · [HIP-1347 §1.1, §1.4, §2.9](https://github.com/hanzoai/hips/blob/main/HIPs/hip-1347-event-driven-agent-reliability.md) (*Dual-Control Business-Agent Benchmark — cataloged as Bounty #39 in `hanzoai/hips`, tracked in this repository as `cloud#30`*)  
**Primary Paper**: V. Barrès, H. Dong, S. Ray, X. Si, K. Narasimhan, *"$\tau^2$-Bench: Evaluating Conversational Agents in a Dual-Control Environment,"* arXiv:2506.07982, 2025. (<https://arxiv.org/abs/2506.07982>)  
**Upstream Reference Repository**: <https://github.com/sierra-research/tau2-bench> (`v2.0.0`, default branch `main`)

This package (`research/tau2dualcontrol`) and companion CLI (`research/tau2dualsim`) provide a **dependency-free, deterministic Go reference benchmark and coordination harness** testing concurrent human + agent mutations over shared multi-tenant business state, enforcing role-based permissions, disjoint credentials, optimistic concurrency (`expected_version` CAS), bounded-TTL authority leases, per-attempt idempotency keys, and SHA-256 hash-chained state-machine replay verification.

> **Scope & Provenance Notice**: Zero code, task prompts, audio assets, or database fixtures were copied from `sierra-research/tau2-bench`. All Go source files, domain schemas, seeded workload generators, and tests in this directory are original clean-room implementations released under `Apache-2.0 OR MIT` with zero modifications to `go.mod`. Published LLM leaderboard scores in `arXiv:2506.07982` are the paper authors' own figures and are not claimed as Hanzo benchmarks here.

---

## 1. Primary Paper Summary & Full Upstream License Audit

### 1.1 Paper & Upstream Architecture Summary

Traditional tool-use benchmarks ($\tau$-bench v1, `arXiv:2406.12045`) evaluate **single-control** environments: only the AI agent invokes backend tools (`tools.py` mutating `db`), while the simulated human user is a passive conversational counterparty with no direct state-mutating tools.

In **$\tau^2$-Bench** (`arXiv:2506.07982`), Barrès et al. formulate collaborative customer service (notably `telecom` troubleshooting) as a **Decentralized Partially Observable Markov Decision Process (Dec-POMDP)** where both the agent and the user possess tools (`tools.py` + `user_tools.py`). However, inspecting the upstream `sierra-research/tau2-bench` implementation reveals four structural gaps relative to production cloud state planes (`hanzoai/cloud`):

1. **Partitioned State (`db` vs. `user_db`) Instead of Contended Shared Records**: Upstream separates backend carrier state (`data_model.py`) from handset state (`user_data_model.py`) and runs strictly alternating sequential turns in one Python loop, so concurrent read-modify-write races (`LostUpdate`, `DoubleRefund`) never occur.
2. **Structural Toolkit Separation Instead of State-Machine RBAC**: Upstream restricts user actions by binding separate Python `ToolKitBase` subclasses rather than enforcing principal role, disjoint credential scope (`HIP-1347 §1.1`), and tenant isolation (`org`) inside a shared transactional store.
3. **No Idempotency Keys Under Transport Retries**: Transient tool timeouts in live MCP/HTTP execution cause duplicate financial side effects unless guarded by deterministic `idempotency_key` tokens (`HIP-1347 §1.4`).
4. **Paid API Coupling**: Upstream `tau2 run` requires live commercial LLM/TTS API keys (`OPENAI_API_KEY`, `elevenlabs`, `deepgram`) and ships Sierra-internal voice IDs, preventing fast, zero-cost CI regression gating.

### 1.2 License, Dependency & Asset Audit (`sierra-research/tau2-bench`)

| Audited Component | Upstream Source / Identifier | License / Terms | Audit Verdict for `hanzoai/cloud` |
| --- | --- | --- | --- |
| Primary paper | Barrès et al., `arXiv:2506.07982` (2025); Yao et al., `arXiv:2406.12045` (2024) | Public academic preprints | Concepts (Dec-POMDP dual-control, compositional task verification) studied; **zero text or figures copied**. |
| Upstream source code | `github.com/sierra-research/tau2-bench` (`LICENSE`, `pyproject.toml`) | **MIT License** (`Copyright (c) 2025 Sierra Research`) | Permissive and compatible with `Apache-2.0 OR MIT`, **but zero Python source was imported or transpiled** (`hanzoai/cloud` is a pure-Go repo). |
| Core Python dependencies | `litellm`, `fastapi`, `uvicorn`, `pydantic`, `deepdiff`, `tenacity`, `typer`, `rich`, `pandas`, `numpy`, `httpx`, `PyYAML` | MIT / BSD-3-Clause / Apache-2.0 | **Not imported.** Our harness uses 100% Go standard library (`CGO_ENABLED=0`, zero `go.mod` changes). |
| Optional `voice` / `live` extras | `elevenlabs`, `deepgram-sdk`, `phonic`, `livekit-agents`, `aiortc`, `google-cloud-aiplatform`, `boto3` | Mixed permissive SDKs wrapping **proprietary commercial SaaS APIs** | **Excluded.** Avoids commercial API lock-in and transitive C/WebRTC dependencies. |
| Datasets & voice assets | `data/tau2/domains/*`, `data/voice/*`, `TAU2_VOICE_ID_*` | Repo root is MIT; README explicitly notes shipped caller voice IDs are **Sierra-internal** | **Excluded.** All 5 task families and seeded fixtures are generated synthetically in Go (`GenerateEpisodeSpec`). |
| Patent notices | None present in `sierra-research/tau2-bench` | MIT carries implicit use/sell grant; no `PATENTS` file | Clean-room Go implementation dual-licensed `Apache-2.0 OR MIT` (`SPDX-License-Identifier` on every file). |

---

## 2. Grounding in `hanzoai/cloud` & Matched Baselines

Inspecting `clients/base/base.go` (`UpdateIn` / `update`, lines 318–346) shows how `hanzoai/cloud` currently handles shared document mutations:

```go
// clients/base/base.go (current OSS behavior)
// Doc replaces the stored document wholesale. There is no partial update...
UPDATE records SET doc = ?, updated = ? WHERE org = ? AND collection = ? AND id = ?
```

While `clients/base` enforces strict tenant isolation (`WHERE org = ?`), `PUT /v1/base/collections/:collection/:id` performs **unversioned wholesale document replacement (Last-Writer-Wins)** with no `if_version` CAS precondition, no workflow lease (`clients/tasks/tasks.go`), and no `idempotency_key` deduplication (`HIP-1347 §1.4`).

To isolate each mechanism cleanly, **four matched protocols** run against the **exact same `SplitMix64` seeds, initial `TenantSnapshot` states, fault schedules, and outcome evaluators**:

1. **`single_control` (Single-Controller Baseline)**: Cooperative prerequisite handshakes followed by sequential agent tool execution with zero concurrent human shared-state writes. Establishes the upper-bound task ceiling (analogous to single-control $\tau$-bench).
2. **`dual_uncoordinated` (Naive Shared-State Baseline)**: Both human and agent interleave `Read -> Modify -> Wholesale PUT` without state-machine RBAC, `expected_version` CAS, workflow leases, or idempotency keys.
3. **`dual_rbac_lww` (Authenticated `clients/base` Baseline)**: Enforces the complete 3-role RBAC permission matrix (`customer`, `support_agent`, `billing_supervisor`), `HIP-1347 §1.1` disjoint credentials (`ai:inference` blocked from state writes), and `HIP-1348 §1.1–§1.2` pre-execution `tenant_ledger` budget checks, **while retaining unversioned wholesale LWW writes**. Tests whether auth middleware alone suffices for dual-control.
4. **`dual_cas_lease` (Proposed Guarded Dual-Control Protocol)**: Adds (a) `expected_version` optimistic Compare-And-Swap on every entity mutation, (b) bounded-TTL workflow leases (`order_settlement`, `line_provisioning`, `ticket_disposition`, `account_governance`) modeled on `clients/tasks/tasks.go`, (c) stateful dual-control preconditions (e.g., customer PIN + handset roaming opt-in before paid carrier pass activation), (d) per-attempt `idempotency_key` deduplication (`HIP-1347 §1.4`), and (e) bounded Conflict-Read-Replan retry loops.

---

## 3. Falsifiable Hypotheses & Expected Failure Modes (Declared Upfront)

- **H1 (RBAC-Only Insufficiency Under Dual-Control)**: Adding stateless RBAC and budget guards (`dual_rbac_lww`) over unversioned wholesale document updates eliminates direct unauthorized actor writes relative to `dual_uncoordinated`, **but still drops task success by $\ge 25$ percentage points below `single_control` and exhibits $\ge 25\%$ safety/accounting invariant violations** due to interleaved lost updates, mutually exclusive double-refunds, and timeout retry duplication.
- **H2 (CAS + Scoped Lease + Idempotency Recovery)**: `dual_cas_lease` reduces safety, ledger-conservation, double-refund, and permission violations to **$0.00\%$ ($0$ breaches across all seeds and goroutine races)** while recovering **$\ge 95\%$** of the `single_control` task-completion rate at bounded coordination cost ($\le 2.5$ additional store operations/episode on average).
- **H3 (Deterministic Audit Replay & Tamper Detection)**: $100\%$ of episode outcomes across all modes are bit-for-bit verifiable by replaying the SHA-256 hash-chained `InteractionRecord` log (`audit/record.go` pattern) from the initial `TenantSnapshot`, and $100\%$ of single-field edits, dropped records, or swapped sequence numbers fail verification (`ErrCodeTamperedLog`).
- **H4 (Expected Failure Mode — Bounded Liveness Trade-Off Under Stalled Human Lease Holds)**: When a human acquires an exclusive `order_settlement` lease (`TTL > agent MaxRetries`) and stalls mid-workflow (`LeaseStallProb` in `medium`/`high` regimes), `dual_cas_lease` **cannot complete the task autonomously**; instead, it fails closed into a verifiable human handoff (`SafeLeaseHandoff`), trading small liveness loss ($\sim 0.3\%$–$1.5\%$ of contended episodes) to preserve $0.00\%$ ledger corruption.

---

## 4. Bounded Clean-Room Implementation & Task Suite

### 4.1 Five Shared-State Business & Telecom Task Families (`tasks.go`)

| Task Family | Shared Entities | Human Actions | Agent / Supervisor Actions | Core Hazard Tested |
| --- | --- | --- | --- | --- |
| `retail_partial_refund_and_shipping` | `Account`, `Order`, `Ledger` | `update_shipping` (address + notes) | `agent_issue_refund` (partial refund + restock) | Wholesale `PUT` clobbering concurrent authorized edits (`Order.RefundedCents` wiped to `0` after ledger credited). |
| `retail_self_cancel_vs_agent_refund` | `Account`, `Order`, `Ledger`, `Lease` | `customer_self_cancel_order` (full refund) | `agent_issue_refund` (goodwill partial refund) | Mutually exclusive settlement race causing **Double-Refund** (`totalCredited > PaidCents`) + **H4** stalled-human lease hold. |
| `telecom_roaming_activation` | `Account`, `TelecomLine`, `Ledger` | `verify_pin`, `toggle_device_roaming` (opt-in, APN, reboot) | `agent_provision_roaming` (`intl_day_pass`, `$15.00`) | Coupled $\tau^2$-Bench handset+carrier ordering, stale handset reboot wiping `ActivePass`, and `HIP-1348` `budget_exhausted` (`layer="tenant_ledger"`). |
| `high_value_refund_and_privilege_boundary` | `Account`, `Order`, `Ledger` | `update_shipping` + adversarial tier/budget probe | `supervisor_approve_high_refund` + `agent_issue_refund` (`>$200`) + `ai:inference` probe | 3-role RBAC enforcement, cumulative `>$200` supervisor co-sign gate, `HIP-1347 §1.1` disjoint credential blocking, and TOCTOU sign-off preservation. |
| `ticket_sla_under_timeout_retries` | `Account`, `SupportTicket`, `Ledger` | `customer_confirm_ticket` | `agent_escalate_ticket` (`+$25.00` SLA credit) under injected ambiguous commit timeouts | `HIP-1347 §1.4` `idempotency_key` deduplication under dropped RPC receipts + resolution-before-close ordering. |

### 4.2 Contention & Fault Regimes (`sim.go`)

Each `(Mode, Seed)` runs `500` episodes in each of three regimes (`3` seeds $\times$ `500` = `1,500` episodes/regime/mode; `4,500` episodes/mode; **`18,000` episodes total**):
- `low_interleaving`: `InterleaveProb = 0.20`, `AmbiguousTimeoutProb = 0.03`, `LeaseStallProb = 0.00`, `MaxRetries = 6`.
- `medium_interleaving`: `InterleaveProb = 0.55`, `AmbiguousTimeoutProb = 0.12`, `LeaseStallProb = 0.02`, `MaxRetries = 5`.
- `high_contention_faults`: `InterleaveProb = 0.85`, `AmbiguousTimeoutProb = 0.25`, `LeaseStallProb = 0.08`, `MaxRetries = 4`.

---

## 5. Correctness, Concurrency, Security & Regression Evidence

`go test -v ./research/...` executes **27 deterministic automated tests** (`26` in `tau2_test.go` + `1` end-to-end CLI build/JSON test in `tau2dualsim/main_test.go`), all passing in `~7s` with `CGO_ENABLED=0`:

- **RBAC, Horizontal Ownership & Disjoint Credentials**: Exhaustive `3 roles × 12 actions` permission matrix (`TestRBACPermissionMatrixExhaustive`); intra-tenant customer BOLA/IDOR isolation (`TestIntraTenantCustomerHorizontalIsolation`); `ai:inference` credential blocking and role-scope mismatch rejection (`TestDisjointCredentialScopeEnforcement`); cumulative refund & SLA smurfing threshold detection (`TestSupervisorCoSignThresholdEnforcement`, `TestCumulativeSLASmurfingAndNegativeParameterGuards`).
- **Multi-Tenant Isolation**: Salted `TenantSnapshot.Digest()` separation, cross-org read/write/lease denial (`ErrCodeCrossTenant`, `TestCrossTenantNamespaceIsolation`), and multi-tenant replay (`TestAuditLogPointerAliasingImmunityAndMultiTenantReplay`).
- **Concurrency & Real OS Goroutine Races**: `200` barrier-synchronized dual-writer goroutine pairs racing self-cancel vs. agent refund (`TestConcurrentGoroutineDualWriterNoDoubleRefund`) + `16`-goroutine parallel stress hammer with post-run replay check (`TestConcurrentGoroutineMultiActorStress`).
- **CAS, Leases, Idempotency & Budgets**: Stale `ExpectedVersion` conflict & recovery (`TestOptimisticCASStaleVersionRejectionAndRecovery`); wholesale LWW clobber reproduction (`TestWholesaleLWWClobberVsCASPreservation`); lease mutual exclusion, scope/target validation, foreign release denial, expired-idempotency takeover safety, and deterministic TTL expiry (`TestAuthorityLeaseMutualExclusionAndTTLExpiry`, `TestExpiredLeaseIdempotencyAndScopeTargetValidation`); `IdempotencyKey` replay vs. mutated-payload rejection (`TestIdempotencyKeyDeduplicationAndMismatchRejection`); `HIP-1348 §1.2` `budget_exhausted` `layer="tenant_ledger"` + `BillingTopUpHint` invariant (`TestTenantLedgerBudgetExhaustedTaxonomy`).
- **Tamper-Evident Audit Replay & Pointer Immunity**: 6 single-field tamper cases, deleted middle entry, swapped sequence order, forged final-state digest, and post-apply pointer-mutation aliasing all verified (`Tests 12–14, 25`).
- **Enso / OpenAI HTTP Adapter, Schema & Drift Lock**: Fail-closed unconfigured check (`TestOpenAIToolAgentFailClosedWhenUnconfigured`), multi-turn CAS conflict recovery over `httptest.NewServer` (`TestOpenAIToolAgentConflictReadReplanOverHTTPTest`), structural `schema.json` validation (`TestCommittedSchemaJSONValid`), and bit-exact verification of committed `testdata/results.json` (`TestCommittedResultsJSONMatchesLiveCode`).

---

## 6. Raw Before/After Measurements & Hypothesis Verdicts

Measured locally on Windows 11 (`amd64`, `go1.26.11`, `CGO_ENABLED=0`; full 18,000-episode matrix + 400 goroutine races executes in **`1.01s`**). Raw machine-readable output is committed at [`testdata/results.json`](testdata/results.json).

### 6.1 Aggregate Results Across All Regimes & Seeds (`4,500` Episodes / Mode)

| Mode | Episodes | Task Success | Safety Violation Rate | Lost Updates | Double Refunds | Dup Timeout Side Effects | Permission / Consent Violations | Fail-Closed Safe Handoffs | Replay Verified | Mean Store Calls / Ep |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `single_control` | 4,500 | **100.00%** | **0.00%** | 0 | 0 | 0 | 0 | 0 | 100.00% | 2.40 |
| `dual_uncoordinated` | 4,500 | **23.20%** | **69.76%** | 2,234 | 1,607 | 990 | 1,191 | 0 | 100.00% | 3.52 |
| `dual_rbac_lww` *(auth over unversioned `base` PUT)* | 4,500 | **39.87%** | **47.62%** | 2,276 | 719 | 990 | 47* | 0 | 100.00% | 3.50 |
| `dual_cas_lease` *(proposed CAS + Lease + Idem)* | 4,500 | **99.69%** | **0.00%** | **0** | **0** | **0** | **0** | **14** *(0.31%)* | **100.00%** | **4.99** |

*\*Note on `dual_rbac_lww` permission/consent count (`47` vs `1,191` in `dual_uncoordinated`): stateless RBAC blocked `100%` of direct actor-role and `ai:inference` violations (`0` unauthorized role actions applied), but `47` episodes still violated stateful consent/approval invariants when wholesale LWW updates clobbered an earlier `SupervisorSignOff` or provisioned carrier passes before customer PIN/opt-in.*

### 6.2 Per-Regime Breakdown (Mean of Rates / Sum of Counts / Max p95 Across 3 Seeds, `1,500` Episodes / Cell Group)

| Contention Regime | Mode | Task Success | Safety Violation Rate | Lost Updates | Double Refunds | Dup Retry Effects | Safe Handoffs | p95 Store Calls / Ep |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `low_interleaving` | `single_control` | 100.00% | 0.00% | 0 | 0 | 0 | 0 | 4 |
| `low_interleaving` | `dual_uncoordinated` | 34.67% | 61.20% | 500 | 488 | 313 | 0 | 6 |
| `low_interleaving` | `dual_rbac_lww` | 56.67% | 37.20% | 523 | 158 | 313 | 0 | 6 |
| `low_interleaving` | `dual_cas_lease` | **100.00%** | **0.00%** | **0** | **0** | **0** | **0** | **7** |
| `medium_interleaving` | `single_control` | 100.00% | 0.00% | 0 | 0 | 0 | 0 | 4 |
| `medium_interleaving` | `dual_uncoordinated` | 21.20% | 70.00% | 794 | 545 | 321 | 0 | 6 |
| `medium_interleaving` | `dual_rbac_lww` | 36.93% | 48.73% | 820 | 250 | 321 | 0 | 6 |
| `medium_interleaving` | `dual_cas_lease` | **99.80%** | **0.00%** | **0** | **0** | **0** | **3** | **8** |
| `high_contention_faults` | `single_control` | 100.00% | 0.00% | 0 | 0 | 0 | 0 | 4 |
| `high_contention_faults` | `dual_uncoordinated` | 13.73% | 78.07% | 940 | 574 | 356 | 0 | 7 |
| `high_contention_faults` | `dual_rbac_lww` | 26.00% | 56.93% | 933 | 311 | 356 | 0 | 6 |
| `high_contention_faults` | `dual_cas_lease` | **99.27%** | **0.00%** | **0** | **0** | **0** | **11** | **8** |

### 6.3 Per-Task-Family Success Rate Ablation (`900` Episodes / Family / Mode)

| Task Family | `single_control` | `dual_uncoordinated` | `dual_rbac_lww` | `dual_cas_lease` | Primary Mechanism Driving Gain |
| --- | ---: | ---: | ---: | ---: | --- |
| `retail_partial_refund_and_shipping` | 100.00% | 41.89% | 41.89% | **100.00%** | `expected_version` CAS stops wholesale `PUT` clobber between two authorized writers (`RBAC` alone gives `0.00 pp` gain). |
| `retail_self_cancel_vs_agent_refund` | 100.00% | 20.67% | 27.11% | **98.56%** *(+1.44% safe handoff)* | `order_settlement` lease + CAS prevents mutually exclusive double-refunds; stalled humans trigger safe fail-closed handoff (**H4**). |
| `telecom_roaming_activation` | 100.00% | 53.44% | 73.44% | **99.89%** *(+0.11% safe handoff)* | `HIP-1348` budget guard lifts `53.4% -> 73.4%`; stateful PIN/opt-in preconditions + CAS lift `73.4% -> 99.9%`. |
| `high_value_refund_and_privilege_boundary` | 100.00% | 0.00% | 56.89% | **100.00%** | RBAC + supervisor threshold + `HIP-1347 §1.1` disjoint scope lift `0% -> 56.9%`; CAS stops RMA note edits from erasing `SupervisorSignOff`. |
| `ticket_sla_under_timeout_retries` | 100.00% | 0.00% | 0.00% | **100.00%** | `HIP-1347 §1.4` `idempotency_key` eliminates `100%` of duplicate SLA credits under dropped transport receipts. |

### 6.4 Real Concurrent Goroutine Race Check (`200` Barrier-Synchronized Trials / Mode)

| Mode | Trials | Double-Refund Conservation Violations | Replay Verified |
| --- | ---: | ---: | ---: |
| `dual_rbac_lww` | 200 | **200 / 200 (100.0%)** | 200 / 200 |
| `dual_cas_lease` | 200 | **0 / 200 (0.0%)** | **200 / 200 (100.0%)** |

### 6.5 Hypothesis Verdicts

- **H1 — CONFIRMED**: `dual_rbac_lww` drops `60.13 pp` below `single_control` (`39.87%` vs `100.00%`, threshold $\ge 25\text{ pp}$) with a `47.62%` safety-violation rate (`threshold >= 25%`). Stateless RBAC alone cannot coordinate shared-state dual-control.
- **H2 — CONFIRMED**: `dual_cas_lease` achieves **`0.00%` safety violations** (`0 / 4,500` episodes and `0 / 200` goroutine races) and **`99.69%` task completion** (`threshold >= 95%`), adding **`+1.49` mean store calls/episode** (`4.99` vs `3.50`, well within the $\le 2.5$ bound).
- **H3 — CONFIRMED**: `18,000 / 18,000` matrix episodes and `400 / 400` concurrent race logs pass deterministic `ReplayAndVerify`, and `100%` of tamper/drop/reorder mutations fail closed in unit tests.
- **H4 — CONFIRMED**: All `14` non-completing episodes out of `4,500` (`0.31%`) in `dual_cas_lease` are clean fail-closed retry/lease handoffs (`4,486 + 14 = 4,500`) with zero state or ledger corruption.

---

## 7. Limitations & Threats to Validity

1. **Structured State Machine vs. Live Natural-Language Ambiguity**: Our deterministic harness isolates **systems-level coordination, permissions, and concurrency hazards** from LLM conversational misunderstandings. Live LLMs evaluated via `OpenAIToolAgent` (`llm_adapter.go`) will exhibit lower absolute task completion when misinterpreting user intent, though state-machine CAS, RBAC, and idempotency still enforce `0%` unauthorized or double-refund corruption regardless of model errors.
2. **In-Memory Mutex vs. Encrypted SQLite Contention**: `SharedStateStore` serializes commits in memory, whereas `clients/base` uses single-writer `cek` SQLite (`db.SetMaxOpenConns(1)`). Under disk fsync latency, CAS retry wall-clock p99 will scale with SQLite write-queue depth and should be measured directly on `base.db` if promoted to core.
3. **Single-Entity Leases vs. Cross-Collection Sagas**: Authority leases here guard one `(org, scope, resource_id)` workflow at a time; multi-order batch transfers would require deterministic lock ordering (`account_id` ascending) to prevent deadlocks.

---

## 8. Integration Recommendation for `hanzoai/cloud`

If maintainers open the follow-up implementation bounty on `hanzoai/cloud#30`, the core win maps into **`clients/base/base.go`** and **`/mcp` typed ops** in ~40 lines with zero new dependencies and 100% backwards compatibility:

1. **Add `version INTEGER NOT NULL DEFAULT 1` to `clients/base` `records` schema** and expose `Version int64` on `base.Record`.
2. **Add optional `IfVersion *int64` and `IdempotencyKey string` to `base.UpdateIn` / `base.CreateIn`** (since `clients/base` uses `zip` typed ops, a single field addition projects automatically into REST `/v1/base`, `/v1/openapi.json`, and `/mcp` JSON-RPC tools per `LLM.md` Invariant #1):
   - When `IfVersion == nil`, existing unversioned callers keep current LWW semantics.
   - When `IfVersion != nil`, execute `UPDATE records SET doc = ?, updated = ?, version = version + 1 WHERE org = ? AND collection = ? AND id = ? AND version = ?` wrapped inside `internal/idem` (`idem.Once` on the same SQLite transaction when `IdempotencyKey != ""`); if `RowsAffected() == 0` while `(org, collection, id)` exists, return `409 Conflict` with the current `Record` version so `Kai` / MCP agents perform Conflict-Read-Replan.
3. **Reuse `clients/tasks` Leases for Mutually Exclusive Human+Agent Workflows** (`order_settlement`, `line_provisioning`) and emit denied CAS/RBAC mutations into `audit.Recorder` (`audit/record.go`).

---

## 9. HIP-1348 & HIP-1347 §2.9 Status Update

- **HIP-1348 / HIP-1347 §2.9 (`cloud#30`, Dual-Control Business-Agent Benchmark)**:
  - **Completed (Research Deliverable)**: Clean-room Go dual-control benchmark (`research/tau2dualcontrol` + `research/tau2dualsim`), upstream `sierra-research/tau2-bench` license/dependency audit, 36-cell reproducible matrix (`18,000` episodes + `400` goroutine race trials), 27 automated verification tests, fail-closed Enso/OpenAI HTTP tool adapter, and committed raw JSON artifacts (`testdata/results.json`).
  - **Key Finding**: Stateless RBAC + `HIP-1348` tenant-ledger checks (`dual_rbac_lww`) eliminate direct unauthorized writes but still suffer a `47.62%` safety-violation rate (`2,276` lost updates, `719` double refunds, `990` duplicate timeout side effects) when paired with unversioned wholesale document updates (`clients/base` `PUT`). Adding `expected_version` CAS, bounded-TTL workflow leases, and `HIP-1347 §1.4` `idempotency_key` deduplication (`dual_cas_lease`) drives safety violations to `0.00%` (`99.69%` task success, `0.31%` safe fail-closed human handoff, `+1.49` calls/episode overhead).
  - **Next Step (Candidate Implementation Bounty)**: Add opt-in `IfVersion` optimistic CAS (`409 Conflict`) and scoped `idempotency_key` retention to `clients/base/base.go` typed operations, and mirror this status note into `hanzoai/hips` (`HIP-1347` / `HIP-1348`).

---

## 10. File Map

| Path | Description |
| --- | --- |
| [`domain.go`](domain.go) | Principals, disjoint `CredentialScope` (`HIP-1347 §1.1`), RBAC matrix, domain entities, `HIP-1348 §1.2` `OpError`, deterministic `SplitMix64` RNG |
| [`store.go`](store.go) | Multi-tenant `SharedStateStore` (LWW vs. CAS+Lease), customer BOLA isolation, idempotency table (`HIP-1347 §1.4`), hash-chained `InteractionRecord` log, `ReplayAndVerifyMulti` |
| [`tasks.go`](tasks.go) | 5 seeded dual-control business/telecom task generators and strict conservation/permission/outcome evaluator |
| [`sim.go`](sim.go) | Read/write micro-step interleaving scheduler, timeout & lease-stall fault injectors, 36-cell matrix runner, goroutine race harness |
| [`llm_adapter.go`](llm_adapter.go) | Optional fail-closed Enso / Zen / OpenAI-compatible (`/v1/chat/completions`) tool-calling agent adapter (`LLM.md` Invariant #4) |
| [`tau2_test.go`](tau2_test.go) | 26 tests: RBAC, customer BOLA, disjoint credentials, tenant isolation, CAS, leases, idempotency, budgets, tamper replay, goroutine races, HTTP LLM, JSON drift lock |
| [`results/schema.json`](results/schema.json) | Draft-07 JSON Schema for `testdata/results.json` |
| [`results/PROTOCOL.md`](results/PROTOCOL.md) | `<2-minute` offline reproduction protocol + optional Enso live check |
| [`testdata/results.json`](testdata/results.json) | Raw committed output of `tau2dualsim` across all 36 cells (`18,000` episodes) + `400` goroutine races |
| [`../tau2dualsim/main.go`](../tau2dualsim/main.go) | CLI benchmark runner (`-episodes`, `-out`) |
| [`../tau2dualsim/main_test.go`](../tau2dualsim/main_test.go) | End-to-end CLI build, execution, and JSON output smoke test |
