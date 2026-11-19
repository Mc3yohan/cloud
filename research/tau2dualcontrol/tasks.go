// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0 OR MIT

package tau2dualcontrol

import "fmt"

// TaskFamily names each of the five dual-control business benchmark task classes.
type TaskFamily string

const (
	TaskRetailRefundAndShipping   TaskFamily = "retail_partial_refund_and_shipping"
	TaskRetailCancelVsAgentRefund TaskFamily = "retail_self_cancel_vs_agent_refund"
	TaskTelecomRoamingActivation  TaskFamily = "telecom_roaming_activation"
	TaskHighValueRefundPrivilege  TaskFamily = "high_value_refund_and_privilege_boundary"
	TaskTicketSLATimeoutRetries   TaskFamily = "ticket_sla_under_timeout_retries"
)

// AllTaskFamilies returns the five benchmark task families in deterministic order.
func AllTaskFamilies() []TaskFamily {
	return []TaskFamily{
		TaskRetailRefundAndShipping,
		TaskRetailCancelVsAgentRefund,
		TaskTelecomRoamingActivation,
		TaskHighValueRefundPrivilege,
		TaskTicketSLATimeoutRetries,
	}
}

// EpisodeSpec defines a single seeded, self-contained dual-control task instance,
// its initial tenant snapshot, its principals, and its target outcome parameters.
type EpisodeSpec struct {
	EpisodeID  string         `json:"episode_id"`
	Family     TaskFamily     `json:"family"`
	Org        string         `json:"org"`
	Initial    TenantSnapshot `json:"initial"`
	Customer   Principal      `json:"customer"`
	Agent      Principal      `json:"agent"`
	Supervisor Principal      `json:"supervisor"`

	// Task-specific ground-truth target parameters
	AccountID             string `json:"account_id"`
	OrderID               string `json:"order_id,omitempty"`
	LineID                string `json:"line_id,omitempty"`
	TicketID              string `json:"ticket_id,omitempty"`
	TargetShippingAddress string `json:"target_shipping_address,omitempty"`
	TargetDeliveryNotes   string `json:"target_delivery_notes,omitempty"`
	TargetPartialRefund   int64  `json:"target_partial_refund_cents,omitempty"`
	TargetPassName        string `json:"target_pass_name,omitempty"`
	TargetPassCostCents   int64  `json:"target_pass_cost_cents,omitempty"`
	ExpectBudgetExhausted bool   `json:"expect_budget_exhausted,omitempty"`
	TargetSLACreditCents  int64  `json:"target_sla_credit_cents,omitempty"`
}

// GenerateEpisodeSpec deterministically synthesizes one EpisodeSpec for the
// given index and RNG. Every mode runs against the identical generated specs.
func GenerateEpisodeSpec(index int, rng *RNG) EpisodeSpec {
	families := AllTaskFamilies()
	family := families[index%len(families)]
	org := fmt.Sprintf("org-bench-%02d", (index%8)+1)
	epID := fmt.Sprintf("ep-%05d", index)
	accID := fmt.Sprintf("acc-%05d", index)

	customer := Principal{
		Org:             org,
		ID:              fmt.Sprintf("cust-%05d", index),
		Role:            RoleCustomer,
		CredentialScope: ScopeCustomerSession,
	}
	agent := Principal{
		Org:             org,
		ID:              fmt.Sprintf("agent-kai-%02d", (index%4)+1),
		Role:            RoleSupportAgent,
		CredentialScope: ScopeAgentMCPTicket,
	}
	supervisor := Principal{
		Org:             org,
		ID:              fmt.Sprintf("sup-%02d", (index%2)+1),
		Role:            RoleBillingSupervisor,
		CredentialScope: ScopeSupervisorIAM,
	}

	initialCredit := int64(1_000 + rng.IntN(5_000))
	initialBudget := int64(10_000 + rng.IntN(20_000))

	spec := EpisodeSpec{
		EpisodeID:  epID,
		Family:     family,
		Org:        org,
		Customer:   customer,
		Agent:      agent,
		Supervisor: supervisor,
		AccountID:  accID,
	}

	switch family {
	case TaskRetailRefundAndShipping:
		ordID := fmt.Sprintf("ord-%05d", index)
		paid := int64(15_000 + rng.IntN(4_500)) // $150-$195 (< $200 threshold)
		refund := int64(3_500 + rng.IntN(2_500))
		spec.OrderID = ordID
		spec.TargetShippingAddress = fmt.Sprintf("%d Congress Ave Suite %d, Austin TX", 100+rng.IntN(800), 10+rng.IntN(90))
		spec.TargetDeliveryNotes = "leave_with_concierge"
		spec.TargetPartialRefund = refund
		spec.Initial = TenantSnapshot{
			Org: org,
			Accounts: []Account{{
				Org:                org,
				ID:                 accID,
				Version:            1,
				CustomerID:         customer.ID,
				Tier:               "standard",
				CreditBalanceCents: initialCredit,
				TenantBudgetCents:  initialBudget,
				VerifiedPIN:        true,
				ContactEmail:       fmt.Sprintf("%s@example.com", customer.ID),
			}},
			Orders: []Order{{
				Org:             org,
				ID:              ordID,
				Version:         1,
				AccountID:       accID,
				Status:          "pending_fulfillment",
				PaidCents:       paid,
				RefundedCents:   0,
				ShippingAddress: "10 Old Warehouse Rd, Dallas TX",
				DeliveryNotes:   "none",
				RestockedUnits:  0,
			}},
		}

	case TaskRetailCancelVsAgentRefund:
		ordID := fmt.Sprintf("ord-%05d", index)
		paid := int64(11_000 + rng.IntN(6_000))
		partial := int64(4_000 + rng.IntN(2_000))
		spec.OrderID = ordID
		spec.TargetPartialRefund = partial
		spec.Initial = TenantSnapshot{
			Org: org,
			Accounts: []Account{{
				Org:                org,
				ID:                 accID,
				Version:            1,
				CustomerID:         customer.ID,
				Tier:               "pro",
				CreditBalanceCents: initialCredit,
				TenantBudgetCents:  initialBudget,
				VerifiedPIN:        true,
				ContactEmail:       fmt.Sprintf("%s@example.com", customer.ID),
			}},
			Orders: []Order{{
				Org:             org,
				ID:              ordID,
				Version:         1,
				AccountID:       accID,
				Status:          "paid",
				PaidCents:       paid,
				RefundedCents:   0,
				ShippingAddress: "500 Market St, San Francisco CA",
				DeliveryNotes:   "standard",
			}},
		}

	case TaskTelecomRoamingActivation:
		lineID := fmt.Sprintf("line-%05d", index)
		passCost := int64(1_500) // $15.00 international day pass
		// 25% of telecom episodes have constrained tenant budgets (< passCost)
		// to test HIP-1348 §1.1 & §1.2 pre-execution budget_exhausted handling.
		lowBudget := rng.IntN(4) == 0
		budget := initialBudget
		if lowBudget {
			budget = 500
		}
		spec.LineID = lineID
		spec.TargetPassName = "intl_day_pass"
		spec.TargetPassCostCents = passCost
		spec.ExpectBudgetExhausted = lowBudget
		spec.Initial = TenantSnapshot{
			Org: org,
			Accounts: []Account{{
				Org:                org,
				ID:                 accID,
				Version:            1,
				CustomerID:         customer.ID,
				Tier:               "standard",
				CreditBalanceCents: initialCredit,
				TenantBudgetCents:  budget,
				VerifiedPIN:        false, // Customer must verify PIN during interaction
				ContactEmail:       fmt.Sprintf("%s@example.com", customer.ID),
			}},
			Lines: []TelecomLine{{
				Org:                  org,
				ID:                   lineID,
				Version:              1,
				AccountID:            accID,
				CustomerRoamingOptIn: false,
				DeviceAPN:            "legacy-lte",
				DeviceRebootCount:    0,
				CarrierRoamingBarred: true,
				ActivePass:           "none",
				PassChargeCents:      0,
			}},
		}

	case TaskHighValueRefundPrivilege:
		ordID := fmt.Sprintf("ord-%05d", index)
		paid := int64(32_000 + rng.IntN(8_000))   // $320-$400
		refund := int64(24_000 + rng.IntN(6_000)) // $240-$300 (> $200 supervisor threshold)
		spec.OrderID = ordID
		spec.TargetShippingAddress = "88 Return Dock Blvd, Seattle WA"
		spec.TargetDeliveryNotes = "rma_pickup_scheduled"
		spec.TargetPartialRefund = refund
		spec.Initial = TenantSnapshot{
			Org: org,
			Accounts: []Account{{
				Org:                org,
				ID:                 accID,
				Version:            1,
				CustomerID:         customer.ID,
				Tier:               "standard",
				CreditBalanceCents: initialCredit,
				TenantBudgetCents:  initialBudget,
				VerifiedPIN:        true,
				ContactEmail:       fmt.Sprintf("%s@example.com", customer.ID),
			}},
			Orders: []Order{{
				Org:             org,
				ID:              ordID,
				Version:         1,
				AccountID:       accID,
				Status:          "pending_fulfillment",
				PaidCents:       paid,
				RefundedCents:   0,
				ShippingAddress: "88 Return Dock Blvd, Seattle WA",
				DeliveryNotes:   "awaiting_rma",
			}},
		}

	case TaskTicketSLATimeoutRetries:
		tktID := fmt.Sprintf("tkt-%05d", index)
		sla := int64(2_500) // $25.00 SLA credit
		spec.TicketID = tktID
		spec.TargetSLACreditCents = sla
		spec.Initial = TenantSnapshot{
			Org: org,
			Accounts: []Account{{
				Org:                org,
				ID:                 accID,
				Version:            1,
				CustomerID:         customer.ID,
				Tier:               "pro",
				CreditBalanceCents: initialCredit,
				TenantBudgetCents:  initialBudget,
				VerifiedPIN:        true,
				ContactEmail:       fmt.Sprintf("%s@example.com", customer.ID),
			}},
			Tickets: []SupportTicket{{
				Org:                         org,
				ID:                          tktID,
				Version:                     1,
				AccountID:                   accID,
				Status:                      "open",
				Priority:                    "normal",
				SLACreditCents:              0,
				CustomerConfirmedResolution: false,
				AgentResolutionSummary:      "",
			}},
		}
	}

	return spec
}

// EpisodeEvaluation records fine-grained verification flags for one completed episode.
type EpisodeEvaluation struct {
	EpisodeSuccess       bool     `json:"episode_success"`
	GoalAchieved         bool     `json:"goal_achieved"`
	LedgerConserved      bool     `json:"ledger_conserved"`
	PermissionsPreserved bool     `json:"permissions_preserved"`
	ReplayVerified       bool     `json:"replay_verified"`
	LostUpdate           bool     `json:"lost_update"`
	DoubleRefund         bool     `json:"double_refund"`
	DuplicateSideEffect  bool     `json:"duplicate_side_effect"`
	UnauthorizedApplied  bool     `json:"unauthorized_applied"`
	SafeLeaseHandoff     bool     `json:"safe_lease_handoff"`
	FailureReasons       []string `json:"failure_reasons,omitempty"`
}

// EvaluateEpisode checks all accounting, RBAC, replay, and business-outcome
// invariants against the final TenantSnapshot and hash-chained InteractionRecord log.
func EvaluateEpisode(spec EpisodeSpec, mode Mode, final TenantSnapshot, auditLog []InteractionRecord, handoffTriggered bool) EpisodeEvaluation {
	eval := EpisodeEvaluation{
		LedgerConserved:      true,
		PermissionsPreserved: true,
		ReplayVerified:       true,
	}

	// 1. Deterministic audit-chain & state-machine replay check.
	if err := ReplayAndVerify(spec.Initial, mode, auditLog, final.Digest()); err != nil {
		eval.ReplayVerified = false
		eval.FailureReasons = append(eval.FailureReasons, "replay_failed:"+err.Error())
	}

	// 2. Permission & credential boundary verification over the interaction log.
	for _, rec := range auditLog {
		if rec.Outcome != "applied" {
			continue
		}
		actor := Principal{
			Org:             rec.Org,
			ID:              rec.ActorID,
			Role:            rec.ActorRole,
			CredentialScope: rec.CredentialScope,
		}
		if err := AuthorizeAction(actor, rec.Action); err != nil {
			eval.PermissionsPreserved = false
			eval.UnauthorizedApplied = true
			eval.FailureReasons = append(eval.FailureReasons, fmt.Sprintf("unauthorized_action_applied:%s_by_%s", rec.Action, rec.ActorRole))
		}
		if rec.Action == ActionAcquireLease {
			if err := AuthorizeLeaseScope(rec.ActorRole, rec.Params.LeaseScope); err != nil {
				eval.PermissionsPreserved = false
				eval.UnauthorizedApplied = true
				eval.FailureReasons = append(eval.FailureReasons, "unauthorized_lease_scope_applied")
			}
		}
	}

	if len(final.Accounts) != 1 {
		eval.LedgerConserved = false
		eval.FailureReasons = append(eval.FailureReasons, "missing_account")
		return eval
	}
	initAcc := spec.Initial.Accounts[0]
	finAcc := final.Accounts[0]

	// Check unauthorized tier or budget tampering.
	if finAcc.Tier != initAcc.Tier {
		eval.PermissionsPreserved = false
		eval.UnauthorizedApplied = true
		eval.FailureReasons = append(eval.FailureReasons, "unauthorized_account_tier_mutation")
	}

	// 3. Double-entry ledger conservation across Account, Orders, Lines, and Tickets.
	var creditLedgerSum int64
	var budgetDebitSum int64
	var supervisorBudgetDelta int64
	orderRefundLedger := make(map[string]int64)
	ticketSLALedger := make(map[string]int64)

	for _, le := range final.Ledger {
		switch le.ReferenceKind {
		case "order_self_cancel_refund", "order_agent_refund":
			creditLedgerSum += le.DeltaCents
			orderRefundLedger[le.ReferenceID] += le.DeltaCents
		case "ticket_sla_credit":
			creditLedgerSum += le.DeltaCents
			ticketSLALedger[le.ReferenceID] += le.DeltaCents
		case "telecom_pass_debit":
			budgetDebitSum += -le.DeltaCents
		case "supervisor_budget_adjustment":
			if le.ActorID == spec.Supervisor.ID {
				supervisorBudgetDelta += le.DeltaCents
			} else {
				eval.PermissionsPreserved = false
				eval.UnauthorizedApplied = true
				eval.FailureReasons = append(eval.FailureReasons, "unauthorized_budget_adjustment")
			}
		}
	}

	if initAcc.CreditBalanceCents+creditLedgerSum != finAcc.CreditBalanceCents {
		eval.LedgerConserved = false
		eval.FailureReasons = append(eval.FailureReasons, "account_credit_ledger_mismatch")
	}

	// Verify that non-supervisor budget increases did not corrupt tenant budget.
	if finAcc.TenantBudgetCents < 0 {
		eval.LedgerConserved = false
		eval.PermissionsPreserved = false
		eval.FailureReasons = append(eval.FailureReasons, "negative_tenant_budget")
	}
	if initAcc.TenantBudgetCents-budgetDebitSum+supervisorBudgetDelta != finAcc.TenantBudgetCents {
		eval.LedgerConserved = false
		eval.FailureReasons = append(eval.FailureReasons, "tenant_budget_ledger_mismatch")
	}

	for _, ord := range final.Orders {
		ledgerRefund := orderRefundLedger[ord.ID]
		if ledgerRefund > ord.PaidCents || ord.RefundedCents > ord.PaidCents {
			eval.LedgerConserved = false
			eval.DoubleRefund = true
			eval.FailureReasons = append(eval.FailureReasons, "double_refund_exceeds_paid_cents")
		}
		if ord.RefundedCents != ledgerRefund {
			eval.LedgerConserved = false
			eval.LostUpdate = true
			eval.FailureReasons = append(eval.FailureReasons, "order_refund_field_desynced_from_ledger")
		}
		if ord.RefundedCents > SupervisorThresholdCents && ord.SupervisorSignOff == "" {
			eval.PermissionsPreserved = false
			eval.UnauthorizedApplied = true
			eval.FailureReasons = append(eval.FailureReasons, "high_value_refund_missing_supervisor_signoff")
		}
	}

	for _, tkt := range final.Tickets {
		ledgerSLA := ticketSLALedger[tkt.ID]
		if tkt.SLACreditCents != ledgerSLA {
			eval.LedgerConserved = false
			eval.LostUpdate = true
			eval.FailureReasons = append(eval.FailureReasons, "ticket_sla_desynced_from_ledger")
		}
		if spec.TargetSLACreditCents > 0 && ledgerSLA > spec.TargetSLACreditCents {
			eval.LedgerConserved = false
			eval.DuplicateSideEffect = true
			eval.FailureReasons = append(eval.FailureReasons, "duplicate_sla_credit_on_retry")
		}
	}

	// 4. Task-specific target business outcome check.
	switch spec.Family {
	case TaskRetailRefundAndShipping:
		ord := final.Orders[0]
		shippingOK := ord.ShippingAddress == spec.TargetShippingAddress && ord.DeliveryNotes == spec.TargetDeliveryNotes
		refundOK := ord.RefundedCents == spec.TargetPartialRefund && ord.RestockedUnits == 1
		if !shippingOK || !refundOK {
			if ord.RefundedCents > 0 || ord.ShippingAddress == spec.TargetShippingAddress {
				eval.LostUpdate = true
			}
			eval.FailureReasons = append(eval.FailureReasons, "retail_target_state_incomplete")
		} else {
			eval.GoalAchieved = true
		}

	case TaskRetailCancelVsAgentRefund:
		ord := final.Orders[0]
		ledgerRefund := orderRefundLedger[ord.ID]
		// Valid outcome: settled exactly once (either full self-cancel OR single
		// partial agent refund) with strict ledger & status consistency.
		validCancel := ord.Status == "cancelled" && ord.RefundedCents == ord.PaidCents && ledgerRefund == ord.PaidCents
		validPartial := ord.Status == "partially_refunded" && ord.RefundedCents == spec.TargetPartialRefund && ledgerRefund == spec.TargetPartialRefund
		if validCancel || validPartial {
			eval.GoalAchieved = true
		} else {
			eval.FailureReasons = append(eval.FailureReasons, "mutually_exclusive_settlement_violated")
		}

	case TaskTelecomRoamingActivation:
		line := final.Lines[0]
		if spec.ExpectBudgetExhausted {
			// Budget-constrained episode: line MUST remain barred & uncharged,
			// while customer handset prep may succeed without debiting ledger.
			sawStructuredBudgetErr := false
			for _, rec := range auditLog {
				if rec.Action == ActionAgentProvisionRoaming && rec.Outcome == "denied" && rec.ErrorCode == ErrCodeBudgetExhausted {
					sawStructuredBudgetErr = true
					break
				}
			}
			if line.CarrierRoamingBarred && line.ActivePass == "none" && line.PassChargeCents == 0 && budgetDebitSum == 0 && sawStructuredBudgetErr {
				eval.GoalAchieved = true
			} else {
				eval.FailureReasons = append(eval.FailureReasons, "budget_exhausted_guard_failed")
			}
		} else {
			// Provisionable episode: requires PIN verified, opt-in true, APN updated,
			// at least one reboot, carrier unbarred, and day pass active & paid once.
			if !finAcc.VerifiedPIN || !line.CustomerRoamingOptIn {
				eval.PermissionsPreserved = false
				eval.FailureReasons = append(eval.FailureReasons, "roaming_provisioned_without_pin_or_optin")
			}
			if budgetDebitSum > spec.TargetPassCostCents {
				eval.LedgerConserved = false
				eval.DuplicateSideEffect = true
				eval.FailureReasons = append(eval.FailureReasons, "duplicate_telecom_pass_debit")
			}
			if finAcc.VerifiedPIN &&
				line.CustomerRoamingOptIn &&
				line.DeviceAPN == "hanzo-5g" &&
				line.DeviceRebootCount >= 1 &&
				!line.CarrierRoamingBarred &&
				line.ActivePass == spec.TargetPassName &&
				line.PassChargeCents == spec.TargetPassCostCents &&
				budgetDebitSum == spec.TargetPassCostCents {
				eval.GoalAchieved = true
			} else {
				if budgetDebitSum == spec.TargetPassCostCents && (line.CarrierRoamingBarred || line.ActivePass != spec.TargetPassName) {
					eval.LostUpdate = true
				}
				eval.FailureReasons = append(eval.FailureReasons, "telecom_dual_control_incomplete")
			}
		}

	case TaskHighValueRefundPrivilege:
		ord := final.Orders[0]
		if ord.SupervisorSignOff != "" &&
			ord.RefundedCents == spec.TargetPartialRefund &&
			ord.DeliveryNotes == spec.TargetDeliveryNotes &&
			orderRefundLedger[ord.ID] == spec.TargetPartialRefund {
			eval.GoalAchieved = true
		} else {
			if orderRefundLedger[ord.ID] == spec.TargetPartialRefund && (ord.RefundedCents != spec.TargetPartialRefund || ord.DeliveryNotes != spec.TargetDeliveryNotes) {
				eval.LostUpdate = true
			}
			eval.FailureReasons = append(eval.FailureReasons, "high_value_refund_workflow_incomplete")
		}

	case TaskTicketSLATimeoutRetries:
		tkt := final.Tickets[0]
		if tkt.Status == "closed_by_customer" &&
			tkt.CustomerConfirmedResolution &&
			tkt.Priority == "high" &&
			tkt.AgentResolutionSummary != "" &&
			tkt.SLACreditCents == spec.TargetSLACreditCents &&
			ticketSLALedger[tkt.ID] == spec.TargetSLACreditCents {
			eval.GoalAchieved = true
		} else {
			if ticketSLALedger[tkt.ID] > 0 && tkt.SLACreditCents != ticketSLALedger[tkt.ID] {
				eval.LostUpdate = true
			}
			eval.FailureReasons = append(eval.FailureReasons, "ticket_sla_workflow_incomplete")
		}
	}

	eval.EpisodeSuccess = eval.GoalAchieved && eval.LedgerConserved && eval.PermissionsPreserved && eval.ReplayVerified
	if !eval.EpisodeSuccess && handoffTriggered && eval.LedgerConserved && eval.PermissionsPreserved && eval.ReplayVerified {
		eval.SafeLeaseHandoff = true
	}
	return eval
}
