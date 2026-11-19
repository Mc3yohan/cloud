// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0 OR MIT

package tau2dualcontrol

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
)

// Regime names a contention and fault-injection profile in the scenario matrix.
type Regime string

const (
	RegimeLowInterleaving      Regime = "low_interleaving"
	RegimeMediumInterleaving   Regime = "medium_interleaving"
	RegimeHighContentionFaults Regime = "high_contention_faults"
)

// AllRegimes returns the three benchmark regimes in canonical order.
func AllRegimes() []Regime {
	return []Regime{
		RegimeLowInterleaving,
		RegimeMediumInterleaving,
		RegimeHighContentionFaults,
	}
}

// RegimeConfig parameterizes actor interleaving, ambiguous transport timeouts,
// stalled human lease holds, and per-step Conflict-Read-Replan retry caps.
type RegimeConfig struct {
	Name                 Regime  `json:"name"`
	InterleaveProb       float64 `json:"interleave_prob"`
	AmbiguousTimeoutProb float64 `json:"ambiguous_timeout_prob"`
	LeaseStallProb       float64 `json:"lease_stall_prob"`
	MaxRetries           int     `json:"max_retries"`
}

// ConfigForRegime returns the fixed benchmark parameters for r.
func ConfigForRegime(r Regime) RegimeConfig {
	switch r {
	case RegimeLowInterleaving:
		return RegimeConfig{
			Name:                 RegimeLowInterleaving,
			InterleaveProb:       0.20,
			AmbiguousTimeoutProb: 0.03,
			LeaseStallProb:       0.00,
			MaxRetries:           6,
		}
	case RegimeMediumInterleaving:
		return RegimeConfig{
			Name:                 RegimeMediumInterleaving,
			InterleaveProb:       0.55,
			AmbiguousTimeoutProb: 0.12,
			LeaseStallProb:       0.02,
			MaxRetries:           5,
		}
	case RegimeHighContentionFaults:
		return RegimeConfig{
			Name:                 RegimeHighContentionFaults,
			InterleaveProb:       0.85,
			AmbiguousTimeoutProb: 0.25,
			LeaseStallProb:       0.08,
			MaxRetries:           4,
		}
	default:
		panic(fmt.Sprintf("tau2dualcontrol: unknown regime %q", r))
	}
}

type stepPhase int

const (
	phaseRead stepPhase = iota
	phaseWrite
)

// plannedOp describes one intent step inside a human or agent actor stream.
// Splitting each intent into phaseRead and phaseWrite reproduces real
// read-modify-write concurrency hazards when the scheduler switches actors
// between reading a document and writing back the updated document.
type plannedOp struct {
	actor          Principal
	action         ActionKind
	targetKind     EntityKind
	targetID       string
	idempotencyKey string
	sideEffecting  bool
	paramsBuilder  func(readSnap readContext) (MutationParams, bool)
}

type readContext struct {
	account Account
	order   Order
	line    TelecomLine
	ticket  SupportTicket
	version uint64
}

type actorStream struct {
	name            string
	ops             []plannedOp
	idx             int
	phase           stepPhase
	cachedRead      readContext
	retries         int
	timeoutInjected bool
	stalled         bool
}

func (a *actorStream) done() bool {
	return a.stalled || a.idx >= len(a.ops)
}

// EpisodeTrace captures the execution telemetry and verification result of one episode.
type EpisodeTrace struct {
	EpisodeID          string            `json:"episode_id"`
	Family             TaskFamily        `json:"family"`
	Mode               Mode              `json:"mode"`
	Regime             Regime            `json:"regime"`
	TotalStoreCalls    int               `json:"total_store_calls"`
	CASConflictRetries int               `json:"cas_conflict_retries"`
	LeaseWaitRetries   int               `json:"lease_wait_retries"`
	PrecondRetries     int               `json:"precondition_retries"`
	IdempotentSaves    int               `json:"idempotent_saves"`
	TimeoutFaults      int               `json:"timeout_faults"`
	FinalDigest        string            `json:"final_digest"`
	Evaluation         EpisodeEvaluation `json:"evaluation"`
}

// buildActorStreams constructs the human and agent intent plans for spec.
func buildActorStreams(spec EpisodeSpec, mode Mode, stalledHuman bool) (*actorStream, *actorStream, []MutationRequest) {
	var humanOps []plannedOp
	var agentOps []plannedOp
	var adversarialProbes []MutationRequest

	useExplicitLeases := mode == ModeDualCASLease

	switch spec.Family {
	case TaskRetailRefundAndShipping:
		humanOps = append(humanOps, plannedOp{
			actor:          spec.Customer,
			action:         ActionUpdateShipping,
			targetKind:     EntityOrder,
			targetID:       spec.OrderID,
			idempotencyKey: spec.EpisodeID + ":cust:ship:1",
			paramsBuilder: func(rc readContext) (MutationParams, bool) {
				snap := rc.order
				return MutationParams{
					ShippingAddress: spec.TargetShippingAddress,
					DeliveryNotes:   spec.TargetDeliveryNotes,
					OrderSnapshot:   &snap,
				}, true
			},
		})
		agentOps = append(agentOps, plannedOp{
			actor:          spec.Agent,
			action:         ActionAgentIssueRefund,
			targetKind:     EntityOrder,
			targetID:       spec.OrderID,
			idempotencyKey: spec.EpisodeID + ":agent:refund:1",
			sideEffecting:  true,
			paramsBuilder: func(rc readContext) (MutationParams, bool) {
				if mode == ModeDualCASLease && rc.order.RefundedCents >= spec.TargetPartialRefund {
					return MutationParams{}, false
				}
				snap := rc.order
				return MutationParams{
					RefundCents:    spec.TargetPartialRefund,
					RestockedUnits: 1,
					OrderSnapshot:  &snap,
				}, true
			},
		})

	case TaskRetailCancelVsAgentRefund:
		if useExplicitLeases {
			humanOps = append(humanOps, plannedOp{
				actor:          spec.Customer,
				action:         ActionAcquireLease,
				targetKind:     EntityOrder,
				targetID:       spec.OrderID,
				idempotencyKey: spec.EpisodeID + ":cust:lease:1",
				paramsBuilder: func(_ readContext) (MutationParams, bool) {
					ttl := DefaultLeaseTTLSteps
					if stalledHuman {
						ttl = 20 // Exceeds agent retry budget to test H4 fail-closed handoff
					}
					return MutationParams{
						LeaseScope:    "order_settlement",
						LeaseTTLSteps: ttl,
					}, true
				},
			})
		}
		if !stalledHuman {
			humanOps = append(humanOps, plannedOp{
				actor:          spec.Customer,
				action:         ActionCustomerSelfCancelOrder,
				targetKind:     EntityOrder,
				targetID:       spec.OrderID,
				idempotencyKey: spec.EpisodeID + ":cust:cancel:1",
				sideEffecting:  true,
				paramsBuilder: func(rc readContext) (MutationParams, bool) {
					// Under Conflict-Read-Replan (ModeDualCASLease), if the agent already
					// settled a partial refund on the fresh read, customer abstains from
					// double-cancelling.
					if mode == ModeDualCASLease && (rc.order.Status == "partially_refunded" || rc.order.Status == "refunded" || rc.order.Status == "cancelled") {
						return MutationParams{}, false
					}
					snap := rc.order
					return MutationParams{OrderSnapshot: &snap}, true
				},
			})
			if useExplicitLeases {
				humanOps = append(humanOps, plannedOp{
					actor:          spec.Customer,
					action:         ActionReleaseLease,
					targetKind:     EntityOrder,
					targetID:       spec.OrderID,
					idempotencyKey: spec.EpisodeID + ":cust:release:1",
					paramsBuilder: func(_ readContext) (MutationParams, bool) {
						return MutationParams{LeaseScope: "order_settlement"}, true
					},
				})
			}
		}

		if useExplicitLeases {
			agentOps = append(agentOps, plannedOp{
				actor:          spec.Agent,
				action:         ActionAcquireLease,
				targetKind:     EntityOrder,
				targetID:       spec.OrderID,
				idempotencyKey: spec.EpisodeID + ":agent:lease:1",
				paramsBuilder: func(rc readContext) (MutationParams, bool) {
					if rc.order.Status == "cancelled" || rc.order.Status == "partially_refunded" {
						return MutationParams{}, false
					}
					return MutationParams{
						LeaseScope:    "order_settlement",
						LeaseTTLSteps: DefaultLeaseTTLSteps,
					}, true
				},
			})
		}
		agentOps = append(agentOps, plannedOp{
			actor:          spec.Agent,
			action:         ActionAgentIssueRefund,
			targetKind:     EntityOrder,
			targetID:       spec.OrderID,
			idempotencyKey: spec.EpisodeID + ":agent:refund:1",
			sideEffecting:  true,
			paramsBuilder: func(rc readContext) (MutationParams, bool) {
				if mode == ModeDualCASLease && (rc.order.Status == "cancelled" || rc.order.Status == "partially_refunded" || rc.order.Status == "refunded") {
					return MutationParams{}, false
				}
				snap := rc.order
				return MutationParams{
					RefundCents:   spec.TargetPartialRefund,
					OrderSnapshot: &snap,
				}, true
			},
		})
		if useExplicitLeases {
			agentOps = append(agentOps, plannedOp{
				actor:          spec.Agent,
				action:         ActionReleaseLease,
				targetKind:     EntityOrder,
				targetID:       spec.OrderID,
				idempotencyKey: spec.EpisodeID + ":agent:release:1",
				paramsBuilder: func(_ readContext) (MutationParams, bool) {
					return MutationParams{LeaseScope: "order_settlement"}, true
				},
			})
		}

	case TaskTelecomRoamingActivation:
		optIn := true
		humanOps = append(humanOps,
			plannedOp{
				actor:          spec.Customer,
				action:         ActionVerifyPIN,
				targetKind:     EntityAccount,
				targetID:       spec.AccountID,
				idempotencyKey: spec.EpisodeID + ":cust:pin:1",
				paramsBuilder: func(_ readContext) (MutationParams, bool) {
					return MutationParams{}, true
				},
			},
			plannedOp{
				actor:          spec.Customer,
				action:         ActionToggleDeviceRoaming,
				targetKind:     EntityTelecomLine,
				targetID:       spec.LineID,
				idempotencyKey: spec.EpisodeID + ":cust:optin:1",
				paramsBuilder: func(rc readContext) (MutationParams, bool) {
					snap := rc.line
					return MutationParams{
						RoamingOptIn: &optIn,
						DeviceAPN:    "hanzo-5g",
						LineSnapshot: &snap,
					}, true
				},
			},
			plannedOp{
				actor:          spec.Customer,
				action:         ActionToggleDeviceRoaming,
				targetKind:     EntityTelecomLine,
				targetID:       spec.LineID,
				idempotencyKey: spec.EpisodeID + ":cust:reboot:1",
				paramsBuilder: func(rc readContext) (MutationParams, bool) {
					snap := rc.line
					return MutationParams{
						RebootDevice: true,
						LineSnapshot: &snap,
					}, true
				},
			},
		)
		agentOps = append(agentOps, plannedOp{
			actor:          spec.Agent,
			action:         ActionAgentProvisionRoaming,
			targetKind:     EntityTelecomLine,
			targetID:       spec.LineID,
			idempotencyKey: spec.EpisodeID + ":agent:roam:1",
			sideEffecting:  true,
			paramsBuilder: func(rc readContext) (MutationParams, bool) {
				if mode == ModeDualCASLease && rc.line.ActivePass == spec.TargetPassName && !rc.line.CarrierRoamingBarred {
					return MutationParams{}, false
				}
				snap := rc.line
				return MutationParams{
					ClearBar:      true,
					ActivatePass:  spec.TargetPassName,
					PassCostCents: spec.TargetPassCostCents,
					LineSnapshot:  &snap,
				}, true
			},
		})

	case TaskHighValueRefundPrivilege:
		// Inject three realistic adversarial / confused-deputy boundary probes:
		// (a) Customer attempting direct account tier & budget elevation,
		// (b) AI inference-only credential attempting an order refund (HIP-1347 §1.1),
		// (c) SupportAgent attempting >$200 refund prior to supervisor sign-off.
		if mode != ModeSingleControl {
			adversarialProbes = []MutationRequest{
				{
					Actor:           spec.Customer,
					Action:          ActionModifyAccountTierOrLimit,
					TargetKind:      EntityAccount,
					TargetID:        spec.AccountID,
					ExpectedVersion: 1,
					Params: MutationParams{
						NewTier:          "enterprise",
						BudgetDeltaCents: 100_000,
					},
				},
				{
					Actor: Principal{
						Org:             spec.Org,
						ID:              "zen-inference-worker",
						Role:            RoleSupportAgent,
						CredentialScope: ScopeAIInferenceOnly,
					},
					Action:          ActionAgentIssueRefund,
					TargetKind:      EntityOrder,
					TargetID:        spec.OrderID,
					ExpectedVersion: 1,
					Params:          MutationParams{RefundCents: 5_000},
				},
				{
					Actor:           spec.Agent,
					Action:          ActionAgentIssueRefund,
					TargetKind:      EntityOrder,
					TargetID:        spec.OrderID,
					ExpectedVersion: 1,
					Params:          MutationParams{RefundCents: spec.TargetPartialRefund},
				},
			}
		}

		humanOps = append(humanOps, plannedOp{
			actor:          spec.Customer,
			action:         ActionUpdateShipping,
			targetKind:     EntityOrder,
			targetID:       spec.OrderID,
			idempotencyKey: spec.EpisodeID + ":cust:rma:1",
			paramsBuilder: func(rc readContext) (MutationParams, bool) {
				snap := rc.order
				return MutationParams{
					ShippingAddress: spec.TargetShippingAddress,
					DeliveryNotes:   spec.TargetDeliveryNotes,
					OrderSnapshot:   &snap,
				}, true
			},
		})
		agentOps = append(agentOps,
			plannedOp{
				actor:          spec.Supervisor,
				action:         ActionSupervisorApproveHighRefund,
				targetKind:     EntityOrder,
				targetID:       spec.OrderID,
				idempotencyKey: spec.EpisodeID + ":sup:approve:1",
				paramsBuilder: func(_ readContext) (MutationParams, bool) {
					return MutationParams{
						SupervisorApprovalToken: "sup-sig-" + spec.EpisodeID,
					}, true
				},
			},
			plannedOp{
				actor:          spec.Agent,
				action:         ActionAgentIssueRefund,
				targetKind:     EntityOrder,
				targetID:       spec.OrderID,
				idempotencyKey: spec.EpisodeID + ":agent:hvrefund:1",
				sideEffecting:  true,
				paramsBuilder: func(rc readContext) (MutationParams, bool) {
					if mode == ModeDualCASLease && rc.order.RefundedCents >= spec.TargetPartialRefund {
						return MutationParams{}, false
					}
					snap := rc.order
					return MutationParams{
						RefundCents:   spec.TargetPartialRefund,
						OrderSnapshot: &snap,
					}, true
				},
			},
		)

	case TaskTicketSLATimeoutRetries:
		agentOps = append(agentOps, plannedOp{
			actor:          spec.Agent,
			action:         ActionAgentEscalateTicket,
			targetKind:     EntitySupportTicket,
			targetID:       spec.TicketID,
			idempotencyKey: spec.EpisodeID + ":agent:sla:1",
			sideEffecting:  true,
			paramsBuilder: func(rc readContext) (MutationParams, bool) {
				if mode == ModeDualCASLease && rc.ticket.SLACreditCents >= spec.TargetSLACreditCents && rc.ticket.AgentResolutionSummary != "" {
					return MutationParams{}, false
				}
				snap := rc.ticket
				return MutationParams{
					TicketPriority:    "high",
					TicketStatus:      "resolved",
					ResolutionSummary: "carrier_route_restored_and_sla_credited",
					SLACreditCents:    spec.TargetSLACreditCents,
					TicketSnapshot:    &snap,
				}, true
			},
		})
		humanOps = append(humanOps, plannedOp{
			actor:          spec.Customer,
			action:         ActionCustomerConfirmTicket,
			targetKind:     EntitySupportTicket,
			targetID:       spec.TicketID,
			idempotencyKey: spec.EpisodeID + ":cust:confirm:1",
			paramsBuilder: func(rc readContext) (MutationParams, bool) {
				snap := rc.ticket
				return MutationParams{
					CustomerConfirmedClose: true,
					TicketSnapshot:         &snap,
				}, true
			},
		})
	}

	return &actorStream{name: "human", ops: humanOps}, &actorStream{name: "agent", ops: agentOps}, adversarialProbes
}

func readEntityForOp(store *SharedStateStore, op plannedOp) (readContext, error) {
	var rc readContext
	switch op.targetKind {
	case EntityAccount:
		a, err := store.GetAccount(op.actor, op.targetID)
		if err != nil {
			return rc, err
		}
		rc.account = a
		rc.version = a.Version
	case EntityOrder:
		o, err := store.GetOrder(op.actor, op.targetID)
		if err != nil {
			return rc, err
		}
		rc.order = o
		rc.version = o.Version
	case EntityTelecomLine:
		l, err := store.GetTelecomLine(op.actor, op.targetID)
		if err != nil {
			return rc, err
		}
		rc.line = l
		rc.version = l.Version
	case EntitySupportTicket:
		t, err := store.GetSupportTicket(op.actor, op.targetID)
		if err != nil {
			return rc, err
		}
		rc.ticket = t
		rc.version = t.Version
	}
	return rc, nil
}

// RunEpisode executes a single seeded episode under mode and regimeCfg,
// returning its complete telemetry and verified business-outcome evaluation.
func RunEpisode(spec EpisodeSpec, mode Mode, regimeCfg RegimeConfig, rng *RNG) EpisodeTrace {
	store := NewSharedStateStore(mode)
	store.LoadTenantSnapshot(spec.Initial)

	trace := EpisodeTrace{
		EpisodeID: spec.EpisodeID,
		Family:    spec.Family,
		Mode:      mode,
		Regime:    regimeCfg.Name,
	}

	// Single-control baseline runs cooperative human prerequisites followed by
	// agent operations sequentially without concurrent write overlap or faults.
	if mode == ModeSingleControl {
		human, agent, _ := buildActorStreams(spec, mode, false)
		runSingleControlSequence(store, spec, human, agent, &trace)
		finalSnap := store.SnapshotTenant(spec.Org)
		trace.FinalDigest = finalSnap.Digest()
		trace.Evaluation = EvaluateEpisode(spec, mode, finalSnap, store.AuditLog(), false)
		return trace
	}

	stalledHuman := spec.Family == TaskRetailCancelVsAgentRefund && rng.Float64() < regimeCfg.LeaseStallProb
	human, agent, probes := buildActorStreams(spec, mode, stalledHuman)

	// Fire boundary/adversarial probes prior to or interleaved with step 0 on
	// governance episodes so the state machine (not prompt obedience) is tested.
	for _, probe := range probes {
		trace.TotalStoreCalls++
		_, _ = store.Apply(probe)
	}

	// On ticket timeout episodes, guarantee at least one ambiguous timeout when
	// AmbiguousTimeoutProb > 0 so retry idempotency is systematically stressed.
	forcedTicketTimeoutPending := spec.Family == TaskTicketSLATimeoutRetries && regimeCfg.AmbiguousTimeoutProb > 0
	handoffTriggered := false
	current := 0 // 0 = human, 1 = agent
	if rng.IntN(2) == 1 {
		current = 1
	}

	safetyStepCap := 64
	for steps := 0; steps < safetyStepCap && (!human.done() || !agent.done()); steps++ {
		streams := [2]*actorStream{human, agent}
		if streams[current].done() {
			current = 1 - current
		}
		active := streams[current]
		if active.done() {
			break
		}

		op := active.ops[active.idx]
		if active.phase == phaseRead {
			rc, err := readEntityForOp(store, op)
			if err != nil {
				active.stalled = true
				continue
			}
			active.cachedRead = rc
			active.phase = phaseWrite

			// Context-switch between Read and Write with InterleaveProb when the
			// peer stream still has pending work, creating genuine RMW overlap.
			other := streams[1-current]
			if !other.done() && rng.Float64() < regimeCfg.InterleaveProb {
				current = 1 - current
			}
			continue
		}

		// phaseWrite
		params, shouldApply := op.paramsBuilder(active.cachedRead)
		if !shouldApply {
			active.idx++
			active.phase = phaseRead
			active.retries = 0
			active.timeoutInjected = false
			continue
		}

		idemKey := ""
		if mode == ModeDualCASLease {
			idemKey = op.idempotencyKey
		}

		req := MutationRequest{
			Actor:           op.actor,
			Action:          op.action,
			TargetKind:      op.targetKind,
			TargetID:        op.targetID,
			ExpectedVersion: active.cachedRead.version,
			IdempotencyKey:  idemKey,
			Params:          params,
		}

		trace.TotalStoreCalls++
		receipt, err := store.Apply(req)
		if err == nil {
			if receipt.IdempotentReplay {
				trace.IdempotentSaves++
			}
			// Inject ambiguous transport timeout: server committed, client did not
			// receive the receipt and therefore retries its write request.
			// Tracked independently of pre-commit CAS/lease retries so all modes
			// experience symmetric post-commit timeout eligibility.
			triggerTimeout := false
			if op.sideEffecting && !active.timeoutInjected {
				if forcedTicketTimeoutPending && spec.Family == TaskTicketSLATimeoutRetries {
					triggerTimeout = true
					forcedTicketTimeoutPending = false
				} else if rng.Float64() < regimeCfg.AmbiguousTimeoutProb {
					triggerTimeout = true
				}
			}
			if triggerTimeout {
				trace.TimeoutFaults++
				active.timeoutInjected = true
				// Client did not see commit: in LWW modes it re-sends phaseWrite;
				// in CAS+Lease mode it re-sends with the same IdempotencyKey!
				continue
			}

			store.AdvanceStep(1)
			// If human was configured to stall right after acquiring the settlement
			// lease, mark human stalled so agent hits H4 lease contention.
			if stalledHuman && active.name == "human" && op.action == ActionAcquireLease {
				active.stalled = true
				current = 1 - current
				continue
			}
			active.idx++
			active.phase = phaseRead
			active.retries = 0
			active.timeoutInjected = false
			if !streams[1-current].done() && rng.Float64() < regimeCfg.InterleaveProb {
				current = 1 - current
			}
			continue
		}

		// Error handling path
		switch {
		case IsErrorCode(err, ErrCodeBudgetExhausted):
			// Terminal HIP-1348 §1.2 tenant-ledger rejection: do not blindly retry.
			active.idx++
			active.phase = phaseRead
			active.retries = 0
			active.timeoutInjected = false

		case IsErrorCode(err, ErrCodeVersionConflict),
			IsErrorCode(err, ErrCodeLeaseHeld),
			IsErrorCode(err, ErrCodePreconditionFailed):
			if mode != ModeDualCASLease {
				// Uncoordinated / LWW modes lack Conflict-Read-Replan loops.
				active.idx++
				active.phase = phaseRead
				active.timeoutInjected = false
				continue
			}
			if IsErrorCode(err, ErrCodeVersionConflict) {
				trace.CASConflictRetries++
			} else if IsErrorCode(err, ErrCodeLeaseHeld) {
				trace.LeaseWaitRetries++
			} else {
				trace.PrecondRetries++
			}

			if active.retries >= regimeCfg.MaxRetries {
				handoffTriggered = true
				active.stalled = true
				continue
			}
			active.retries++
			store.AdvanceStep(1)
			active.phase = phaseRead
			// Yield to peer actor so precondition providers (e.g., customer PIN /
			// opt-in or agent ticket resolution) can make forward progress.
			if !streams[1-current].done() {
				current = 1 - current
			}

		default:
			// Unauthorized or terminal error
			active.idx++
			active.phase = phaseRead
			active.retries = 0
			active.timeoutInjected = false
		}
	}

	finalSnap := store.SnapshotTenant(spec.Org)
	trace.FinalDigest = finalSnap.Digest()
	trace.Evaluation = EvaluateEpisode(spec, mode, finalSnap, store.AuditLog(), handoffTriggered)
	return trace
}

func runSingleControlSequence(store *SharedStateStore, spec EpisodeSpec, human, agent *actorStream, trace *EpisodeTrace) {
	execOne := func(s *actorStream) bool {
		if s.done() {
			return false
		}
		op := s.ops[s.idx]
		rc, err := readEntityForOp(store, op)
		if err != nil {
			s.stalled = true
			return false
		}
		params, ok := op.paramsBuilder(rc)
		s.idx++
		if !ok {
			return true
		}
		trace.TotalStoreCalls++
		_, _ = store.Apply(MutationRequest{
			Actor:           op.actor,
			Action:          op.action,
			TargetKind:      op.targetKind,
			TargetID:        op.targetID,
			ExpectedVersion: rc.version,
			IdempotencyKey:  op.idempotencyKey,
			Params:          params,
		})
		store.AdvanceStep(1)
		return true
	}

	switch spec.Family {
	case TaskTelecomRoamingActivation:
		// Cooperative sequential order: PIN -> OptIn -> Agent Provision -> Reboot
		execOne(human)
		execOne(human)
		execOne(agent)
		execOne(human)
	case TaskTicketSLATimeoutRetries:
		// Agent resolves & credits -> Customer confirms closure
		execOne(agent)
		execOne(human)
	case TaskRetailCancelVsAgentRefund:
		// Single-controller baseline: agent settles partial refund cleanly without
		// conflicting human self-cancellation race.
		execOne(agent)
	default:
		for !agent.done() {
			execOne(agent)
		}
		for !human.done() {
			execOne(human)
		}
	}
}

// CellMetrics aggregates raw counts and rates for one (Regime, Mode, Seed) cell.
type CellMetrics struct {
	Regime Regime `json:"regime"`
	Mode   Mode   `json:"mode"`
	Seed   uint64 `json:"seed"`

	Episodes int `json:"episodes"`

	EpisodeSuccesses      int `json:"episode_successes"`
	GoalAchievedCount     int `json:"goal_achieved_count"`
	SafetyViolationCount  int `json:"safety_violation_count"`
	LedgerViolationCount  int `json:"ledger_violation_count"`
	PermViolationCount    int `json:"permission_violation_count"`
	LostUpdateCount       int `json:"lost_update_count"`
	DoubleRefundCount     int `json:"double_refund_count"`
	DuplicateEffectCount  int `json:"duplicate_side_effect_count"`
	UnauthorizedWriteCnt  int `json:"unauthorized_write_count"`
	ReplayVerifiedCount   int `json:"replay_verified_count"`
	SafeLeaseHandoffCount int `json:"safe_lease_handoff_count"`

	TaskSuccessRate     float64 `json:"task_success_rate"`
	SafetyViolationRate float64 `json:"safety_violation_rate"`
	ReplayVerifiedRate  float64 `json:"replay_verified_rate"`
	SafeHandoffRate     float64 `json:"safe_handoff_rate"`

	TotalStoreCalls     int     `json:"total_store_calls"`
	MeanStoreCallsPerEp float64 `json:"mean_store_calls_per_ep"`
	P95StoreCallsPerEp  int     `json:"p95_store_calls_per_ep"`
	TotalCASRetries     int     `json:"total_cas_retries"`
	TotalLeaseRetries   int     `json:"total_lease_retries"`
	TotalPrecondRetries int     `json:"total_precondition_retries"`
	TotalIdempotentHits int     `json:"total_idempotent_saves"`
	TotalTimeoutFaults  int     `json:"total_timeout_faults"`

	PerFamilySuccessRate map[TaskFamily]float64 `json:"per_family_success_rate"`
}

// MatrixReport is the top-level machine-readable JSON output committed to testdata/.
type MatrixReport struct {
	Schema              string                 `json:"schema"`
	Benchmark           string                 `json:"benchmark"`
	Issue               string                 `json:"issue"`
	TrackingHIP         string                 `json:"tracking_hip"`
	RelatedHIP          string                 `json:"related_hip"`
	EpisodesPerCell     int                    `json:"episodes_per_cell"`
	Seeds               []uint64               `json:"seeds"`
	Cells               []CellMetrics          `json:"cells"`
	AggregatedByMode    map[string]ModeSummary `json:"aggregated_by_mode"`
	ConcurrentRaceCheck ConcurrentRaceSummary  `json:"concurrent_race_check"`
}

// ModeSummary aggregates across all regimes and seeds (or dual-only regimes) for
// quick before/after comparison in HIP reports.
type ModeSummary struct {
	Mode                 Mode    `json:"mode"`
	TotalEpisodes        int     `json:"total_episodes"`
	TaskSuccessRate      float64 `json:"task_success_rate"`
	SafetyViolationRate  float64 `json:"safety_violation_rate"`
	LedgerViolationCount int     `json:"ledger_violation_count"`
	PermViolationCount   int     `json:"permission_violation_count"`
	LostUpdateCount      int     `json:"lost_update_count"`
	DoubleRefundCount    int     `json:"double_refund_count"`
	DuplicateEffectCount int     `json:"duplicate_side_effect_count"`
	ReplayVerifiedRate   float64 `json:"replay_verified_rate"`
	SafeHandoffCount     int     `json:"safe_handoff_count"`
	MeanStoreCallsPerEp  float64 `json:"mean_store_calls_per_ep"`
}

// ConcurrentRaceSummary records the results of driving real overlapping Go
// goroutines against SharedStateStore on mutually exclusive order settlements.
type ConcurrentRaceSummary struct {
	TrialsPerMode                  int `json:"trials_per_mode"`
	LWWDoubleRefundViolations      int `json:"lww_double_refund_violations"`
	CASLeaseDoubleRefundViolations int `json:"cas_lease_double_refund_violations"`
	CASLeaseReplayVerified         int `json:"cas_lease_replay_verified"`
}

func round4(x float64) float64 {
	return math.Round(x*10_000) / 10_000
}

// RunCell executes episodesPerCell deterministic episodes for one (regime, mode, seed).
func RunCell(regime Regime, mode Mode, seed uint64, episodesPerCell int) CellMetrics {
	cfg := ConfigForRegime(regime)
	specRNG := NewRNG(seed ^ 0xA5A5A5A55A5A5A5A)
	schedRNG := NewRNG(seed ^ 0x5A5A5A5AA5A5A5A5)

	m := CellMetrics{
		Regime:               regime,
		Mode:                 mode,
		Seed:                 seed,
		Episodes:             episodesPerCell,
		PerFamilySuccessRate: make(map[TaskFamily]float64),
	}

	famTotal := make(map[TaskFamily]int)
	famPass := make(map[TaskFamily]int)
	callsSlice := make([]int, 0, episodesPerCell)
	totalCalls := 0

	for i := 0; i < episodesPerCell; i++ {
		spec := GenerateEpisodeSpec(i, specRNG)
		// Derive an episode-local deterministic RNG so outcomes of episode i do not
		// perturb the schedule of episode i+1 across different modes!
		epRNG := NewRNG(schedRNG.Uint64() ^ uint64(i)*0x9e3779b97f4a7c15)
		trace := RunEpisode(spec, mode, cfg, epRNG)

		famTotal[spec.Family]++
		callsSlice = append(callsSlice, trace.TotalStoreCalls)
		totalCalls += trace.TotalStoreCalls
		m.TotalCASRetries += trace.CASConflictRetries
		m.TotalLeaseRetries += trace.LeaseWaitRetries
		m.TotalPrecondRetries += trace.PrecondRetries
		m.TotalIdempotentHits += trace.IdempotentSaves
		m.TotalTimeoutFaults += trace.TimeoutFaults

		ev := trace.Evaluation
		if ev.EpisodeSuccess {
			m.EpisodeSuccesses++
			famPass[spec.Family]++
		}
		if ev.GoalAchieved {
			m.GoalAchievedCount++
		}
		if !ev.LedgerConserved || !ev.PermissionsPreserved || !ev.ReplayVerified {
			m.SafetyViolationCount++
		}
		if !ev.LedgerConserved {
			m.LedgerViolationCount++
		}
		if !ev.PermissionsPreserved {
			m.PermViolationCount++
		}
		if ev.LostUpdate {
			m.LostUpdateCount++
		}
		if ev.DoubleRefund {
			m.DoubleRefundCount++
		}
		if ev.DuplicateSideEffect {
			m.DuplicateEffectCount++
		}
		if ev.UnauthorizedApplied {
			m.UnauthorizedWriteCnt++
		}
		if ev.ReplayVerified {
			m.ReplayVerifiedCount++
		}
		if ev.SafeLeaseHandoff {
			m.SafeLeaseHandoffCount++
		}
	}

	sort.Ints(callsSlice)
	m.TotalStoreCalls = totalCalls
	if episodesPerCell > 0 {
		m.TaskSuccessRate = round4(float64(m.EpisodeSuccesses) / float64(episodesPerCell))
		m.SafetyViolationRate = round4(float64(m.SafetyViolationCount) / float64(episodesPerCell))
		m.ReplayVerifiedRate = round4(float64(m.ReplayVerifiedCount) / float64(episodesPerCell))
		m.SafeHandoffRate = round4(float64(m.SafeLeaseHandoffCount) / float64(episodesPerCell))
		m.MeanStoreCallsPerEp = round4(float64(totalCalls) / float64(episodesPerCell))
		p95Idx := (95 * episodesPerCell) / 100
		if p95Idx >= episodesPerCell {
			p95Idx = episodesPerCell - 1
		}
		m.P95StoreCallsPerEp = callsSlice[p95Idx]
	}

	for _, fam := range AllTaskFamilies() {
		if famTotal[fam] > 0 {
			m.PerFamilySuccessRate[fam] = round4(float64(famPass[fam]) / float64(famTotal[fam]))
		}
	}
	return m
}

// RunConcurrentGoroutineRaces exercises real overlapping OS/Go runtime goroutines
// racing Customer self-cancellation against Agent partial refund after a shared
// read barrier (demonstrating thread safety and real concurrency behavior).
func RunConcurrentGoroutineRaces(trials int) ConcurrentRaceSummary {
	out := ConcurrentRaceSummary{TrialsPerMode: trials}

	for _, mode := range []Mode{ModeDualRBACLWW, ModeDualCASLease} {
		for i := 0; i < trials; i++ {
			rng := NewRNG(uint64(10_000 + i))
			spec := GenerateEpisodeSpec(1, rng) // index 1 == TaskRetailCancelVsAgentRefund
			store := NewSharedStateStore(mode)
			store.LoadTenantSnapshot(spec.Initial)

			// Both principals read the initial order snapshot before the start barrier opens.
			custOrder, _ := store.GetOrder(spec.Customer, spec.OrderID)
			agentOrder, _ := store.GetOrder(spec.Agent, spec.OrderID)

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				<-start
				snap := custOrder
				_, err := store.Apply(MutationRequest{
					Actor:           spec.Customer,
					Action:          ActionCustomerSelfCancelOrder,
					TargetKind:      EntityOrder,
					TargetID:        spec.OrderID,
					ExpectedVersion: custOrder.Version,
					IdempotencyKey:  spec.EpisodeID + ":race:cust",
					Params:          MutationParams{OrderSnapshot: &snap},
				})
				if mode == ModeDualCASLease && IsErrorCode(err, ErrCodeVersionConflict) {
					// Conflict-Read-Replan: inspect updated order; abstain if settled.
					fresh, rErr := store.GetOrder(spec.Customer, spec.OrderID)
					if rErr == nil && fresh.Status == "paid" {
						_, _ = store.Apply(MutationRequest{
							Actor:           spec.Customer,
							Action:          ActionCustomerSelfCancelOrder,
							TargetKind:      EntityOrder,
							TargetID:        spec.OrderID,
							ExpectedVersion: fresh.Version,
							IdempotencyKey:  spec.EpisodeID + ":race:cust:retry",
						})
					}
				}
			}()

			go func() {
				defer wg.Done()
				<-start
				snap := agentOrder
				_, err := store.Apply(MutationRequest{
					Actor:           spec.Agent,
					Action:          ActionAgentIssueRefund,
					TargetKind:      EntityOrder,
					TargetID:        spec.OrderID,
					ExpectedVersion: agentOrder.Version,
					IdempotencyKey:  spec.EpisodeID + ":race:agent",
					Params: MutationParams{
						RefundCents:   spec.TargetPartialRefund,
						OrderSnapshot: &snap,
					},
				})
				if mode == ModeDualCASLease && IsErrorCode(err, ErrCodeVersionConflict) {
					fresh, rErr := store.GetOrder(spec.Agent, spec.OrderID)
					if rErr == nil && fresh.Status == "paid" {
						_, _ = store.Apply(MutationRequest{
							Actor:           spec.Agent,
							Action:          ActionAgentIssueRefund,
							TargetKind:      EntityOrder,
							TargetID:        spec.OrderID,
							ExpectedVersion: fresh.Version,
							IdempotencyKey:  spec.EpisodeID + ":race:agent:retry",
							Params:          MutationParams{RefundCents: spec.TargetPartialRefund},
						})
					}
				}
			}()

			close(start)
			wg.Wait()

			finalSnap := store.SnapshotTenant(spec.Org)
			eval := EvaluateEpisode(spec, mode, finalSnap, store.AuditLog(), false)
			if mode == ModeDualRBACLWW {
				if eval.DoubleRefund || !eval.LedgerConserved {
					out.LWWDoubleRefundViolations++
				}
			} else {
				if eval.DoubleRefund || !eval.LedgerConserved {
					out.CASLeaseDoubleRefundViolations++
				}
				if eval.ReplayVerified {
					out.CASLeaseReplayVerified++
				}
			}
		}
	}
	return out
}

// DefaultSeeds returns the three fixed benchmark seeds used in the committed matrix.
func DefaultSeeds() []uint64 {
	return []uint64{20261101, 20261102, 20261103}
}

// DefaultEpisodesPerCell is the default per-(regime, mode, seed) episode count
// (500 episodes * 3 regimes * 3 seeds = 4,500 episodes per mode, 18,000 total).
const DefaultEpisodesPerCell = 500

// RunMatrix executes the complete 36-cell scenario matrix (3 regimes * 4 modes *
// 3 seeds) plus a 200-trial concurrent goroutine race check.
func RunMatrix(episodesPerCell int) MatrixReport {
	if episodesPerCell <= 0 {
		episodesPerCell = DefaultEpisodesPerCell
	}
	seeds := DefaultSeeds()
	var cells []CellMetrics

	agg := make(map[Mode]*ModeSummary)
	totalCallsByMode := make(map[Mode]int)
	totalSuccessByMode := make(map[Mode]int)
	totalSafetyViolByMode := make(map[Mode]int)
	totalReplayOKByMode := make(map[Mode]int)

	for _, mode := range AllModes() {
		agg[mode] = &ModeSummary{Mode: mode}
	}

	for _, regime := range AllRegimes() {
		for _, mode := range AllModes() {
			for _, seed := range seeds {
				cell := RunCell(regime, mode, seed, episodesPerCell)
				cells = append(cells, cell)

				ms := agg[mode]
				ms.TotalEpisodes += cell.Episodes
				ms.LedgerViolationCount += cell.LedgerViolationCount
				ms.PermViolationCount += cell.PermViolationCount
				ms.LostUpdateCount += cell.LostUpdateCount
				ms.DoubleRefundCount += cell.DoubleRefundCount
				ms.DuplicateEffectCount += cell.DuplicateEffectCount
				ms.SafeHandoffCount += cell.SafeLeaseHandoffCount

				totalSuccessByMode[mode] += cell.EpisodeSuccesses
				totalSafetyViolByMode[mode] += cell.SafetyViolationCount
				totalReplayOKByMode[mode] += cell.ReplayVerifiedCount
				totalCallsByMode[mode] += cell.TotalStoreCalls
			}
		}
	}

	aggOut := make(map[string]ModeSummary, len(agg))
	for _, mode := range AllModes() {
		ms := agg[mode]
		if ms.TotalEpisodes > 0 {
			ms.TaskSuccessRate = round4(float64(totalSuccessByMode[mode]) / float64(ms.TotalEpisodes))
			ms.SafetyViolationRate = round4(float64(totalSafetyViolByMode[mode]) / float64(ms.TotalEpisodes))
			ms.ReplayVerifiedRate = round4(float64(totalReplayOKByMode[mode]) / float64(ms.TotalEpisodes))
			ms.MeanStoreCallsPerEp = round4(float64(totalCallsByMode[mode]) / float64(ms.TotalEpisodes))
		}
		aggOut[string(mode)] = *ms
	}

	// Note: RunConcurrentGoroutineRaces uses real goroutine scheduling, whereas
	// MatrixJSON deterministic fixtures require reproducible numbers; note that in
	// both LWW and CASLease, when both goroutines read v1 BEFORE the barrier opens,
	// BOTH see v1 deterministically regardless of which goroutine acquires mu first!
	raceSummary := RunConcurrentGoroutineRaces(200)

	return MatrixReport{
		Schema:              "hanzo.tau2dualcontrol.results/v1",
		Benchmark:           "tau2-dual-control-business-agent",
		Issue:               "https://github.com/hanzoai/cloud/issues/30",
		TrackingHIP:         "HIP-1348",
		RelatedHIP:          "HIP-1347 §1.1, §1.4, §2.9",
		EpisodesPerCell:     episodesPerCell,
		Seeds:               seeds,
		Cells:               cells,
		AggregatedByMode:    aggOut,
		ConcurrentRaceCheck: raceSummary,
	}
}

// MarshalMatrixJSON serializes report as indented canonical JSON.
func MarshalMatrixJSON(report MatrixReport) ([]byte, error) {
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
