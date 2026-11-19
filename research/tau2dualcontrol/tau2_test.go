// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0 OR MIT

package tau2dualcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func seedBasicTenant(org string) (TenantSnapshot, Principal, Principal, Principal) {
	cust := Principal{Org: org, ID: "cust-1", Role: RoleCustomer, CredentialScope: ScopeCustomerSession}
	agent := Principal{Org: org, ID: "agent-1", Role: RoleSupportAgent, CredentialScope: ScopeAgentMCPTicket}
	sup := Principal{Org: org, ID: "sup-1", Role: RoleBillingSupervisor, CredentialScope: ScopeSupervisorIAM}

	snap := TenantSnapshot{
		Org: org,
		Accounts: []Account{{
			Org:                org,
			ID:                 "acc-1",
			Version:            1,
			CustomerID:         cust.ID,
			Tier:               "standard",
			CreditBalanceCents: 2_000,
			TenantBudgetCents:  10_000,
			VerifiedPIN:        false,
			ContactEmail:       "cust-1@example.com",
		}},
		Orders: []Order{{
			Org:             org,
			ID:              "ord-1",
			Version:         1,
			AccountID:       "acc-1",
			Status:          "pending_fulfillment",
			PaidCents:       30_000,
			RefundedCents:   0,
			ShippingAddress: "100 Main St",
			DeliveryNotes:   "none",
		}},
		Lines: []TelecomLine{{
			Org:                  org,
			ID:                   "line-1",
			Version:              1,
			AccountID:            "acc-1",
			CustomerRoamingOptIn: false,
			DeviceAPN:            "legacy-lte",
			DeviceRebootCount:    0,
			CarrierRoamingBarred: true,
			ActivePass:           "none",
		}},
		Tickets: []SupportTicket{{
			Org:       org,
			ID:        "tkt-1",
			Version:   1,
			AccountID: "acc-1",
			Status:    "open",
			Priority:  "normal",
		}},
	}
	return snap, cust, agent, sup
}

// 1. Exhaustive RBAC matrix test across all roles and actions.
func TestRBACPermissionMatrixExhaustive(t *testing.T) {
	_, cust, agent, sup := seedBasicTenant("org-rbac")

	cases := []struct {
		action     ActionKind
		custAllow  bool
		agentAllow bool
		supAllow   bool
	}{
		{ActionVerifyPIN, true, false, false},
		{ActionUpdateShipping, true, false, false},
		{ActionCustomerSelfCancelOrder, true, false, false},
		{ActionToggleDeviceRoaming, true, false, false},
		{ActionCustomerConfirmTicket, true, false, false},
		{ActionAgentIssueRefund, false, true, true},
		{ActionAgentProvisionRoaming, false, true, true},
		{ActionAgentEscalateTicket, false, true, true},
		{ActionSupervisorApproveHighRefund, false, false, true},
		{ActionModifyAccountTierOrLimit, false, false, true},
		{ActionAcquireLease, true, true, true},
		{ActionReleaseLease, true, true, true},
	}

	for _, tc := range cases {
		check := func(p Principal, wantAllow bool) {
			err := AuthorizeAction(p, tc.action)
			if wantAllow && err != nil {
				t.Fatalf("expected %s to be allowed on %s, got err=%v", p.Role, tc.action, err)
			}
			if !wantAllow && !IsErrorCode(err, ErrCodeUnauthorizedRole) {
				t.Fatalf("expected %s on %s to fail with %s, got err=%v", p.Role, tc.action, ErrCodeUnauthorizedRole, err)
			}
		}
		check(cust, tc.custAllow)
		check(agent, tc.agentAllow)
		check(sup, tc.supAllow)
	}

	// Empty org or unknown role must always fail closed.
	if !IsErrorCode(AuthorizeAction(Principal{Org: "", ID: "x", Role: RoleCustomer, CredentialScope: ScopeCustomerSession}, ActionVerifyPIN), ErrCodeUnauthorizedRole) {
		t.Fatal("expected empty org to be rejected")
	}
	if !IsErrorCode(AuthorizeAction(Principal{Org: "org-rbac", ID: "x", Role: "root", CredentialScope: ScopeSupervisorIAM}, ActionVerifyPIN), ErrCodeUnauthorizedRole) {
		t.Fatal("expected unknown role to be rejected")
	}
}

// 2. Disjoint credential scope enforcement (HIP-1347 §1.1).
func TestDisjointCredentialScopeEnforcement(t *testing.T) {
	snap, _, _, _ := seedBasicTenant("org-creds")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	// Per-run ai:inference key must be blocked from mutating business state.
	inferencePrincipal := Principal{
		Org:             "org-creds",
		ID:              "zen-worker-9",
		Role:            RoleSupportAgent,
		CredentialScope: ScopeAIInferenceOnly,
	}
	_, err := store.Apply(MutationRequest{
		Actor:           inferencePrincipal,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params:          MutationParams{RefundCents: 1_000},
	})
	if !IsErrorCode(err, ErrCodeDisjointCredential) {
		t.Fatalf("expected %s for ai:inference key, got %v", ErrCodeDisjointCredential, err)
	}

	// Role/credential mismatch (support_agent presenting customer session cookie).
	mismatched := Principal{
		Org:             "org-creds",
		ID:              "confused-agent",
		Role:            RoleSupportAgent,
		CredentialScope: ScopeCustomerSession,
	}
	_, err = store.Apply(MutationRequest{
		Actor:           mismatched,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params:          MutationParams{RefundCents: 1_000},
	})
	if !IsErrorCode(err, ErrCodeDisjointCredential) {
		t.Fatalf("expected %s for mismatched credential scope, got %v", ErrCodeDisjointCredential, err)
	}
}

// 3. Multi-tenant namespace isolation across reads, writes, leases, and digests.
func TestCrossTenantNamespaceIsolation(t *testing.T) {
	snapA, custA, agentA, _ := seedBasicTenant("org-alpha")
	snapB, custB, _, _ := seedBasicTenant("org-beta")
	// Add an exclusive order in org-beta to test foreign lookup rejection.
	snapB.Orders = append(snapB.Orders, Order{
		Org:       "org-beta",
		ID:        "ord-beta-only",
		Version:   1,
		AccountID: "acc-1",
		Status:    "paid",
		PaidCents: 9_000,
	})

	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snapA)
	store.LoadTenantSnapshot(snapB)

	// Salted digests must differ even on identical initial entity contents before
	// ord-beta-only is considered.
	sA, _, _, _ := seedBasicTenant("org-alpha")
	sB, _, _, _ := seedBasicTenant("org-beta")
	if sA.Digest() == sB.Digest() {
		t.Fatal("cross-tenant snapshots with identical payloads must have distinct salted digests")
	}

	// Cross-tenant read of foreign-only ID must fail closed with ErrCodeCrossTenant.
	if _, err := store.GetOrder(custA, "ord-beta-only"); !IsErrorCode(err, ErrCodeCrossTenant) {
		t.Fatalf("expected %s on foreign order read, got %v", ErrCodeCrossTenant, err)
	}

	// Cross-tenant write of foreign-only ID must fail closed with ErrCodeCrossTenant.
	_, err := store.Apply(MutationRequest{
		Actor:           agentA,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-beta-only",
		ExpectedVersion: 1,
		Params:          MutationParams{RefundCents: 1_000},
	})
	if !IsErrorCode(err, ErrCodeCrossTenant) {
		t.Fatalf("expected %s on foreign order write, got %v", ErrCodeCrossTenant, err)
	}

	// Mutating ord-1 in org-alpha must not touch ord-1 in org-beta.
	if _, err := store.Apply(MutationRequest{
		Actor:           agentA,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params:          MutationParams{RefundCents: 5_000},
	}); err != nil {
		t.Fatalf("org-alpha refund failed: %v", err)
	}

	ordBeta, err := store.GetOrder(custB, "ord-1")
	if err != nil {
		t.Fatalf("org-beta read failed: %v", err)
	}
	if ordBeta.RefundedCents != 0 || ordBeta.Version != 1 {
		t.Fatalf("org-alpha mutation leaked into org-beta: %+v", ordBeta)
	}
}

// 4. Optimistic CAS stale-version rejection and Conflict-Read-Replan recovery.
func TestOptimisticCASStaleVersionRejectionAndRecovery(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-cas")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	// Both read v1.
	custRead, _ := store.GetOrder(cust, "ord-1")
	agentRead, _ := store.GetOrder(agent, "ord-1")

	// Customer commits v2.
	rec1, err := store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionUpdateShipping,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: custRead.Version,
		Params:          MutationParams{ShippingAddress: "200 New St", DeliveryNotes: "ring_bell"},
	})
	if err != nil || rec1.NewVersion != 2 {
		t.Fatalf("customer update failed: rec=%+v err=%v", rec1, err)
	}

	// Agent submits stale ExpectedVersion=1 -> rejected with CurrentVersion=2.
	_, err = store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: agentRead.Version,
		Params:          MutationParams{RefundCents: 4_000, RestockedUnits: 1},
	})
	var opErr *OpError
	if !IsErrorCode(err, ErrCodeVersionConflict) {
		t.Fatalf("expected %s, got %v", ErrCodeVersionConflict, err)
	}
	opErr = err.(*OpError)
	if opErr.CurrentVersion != 2 {
		t.Fatalf("expected CurrentVersion=2 on conflict, got %d", opErr.CurrentVersion)
	}

	// Agent refreshes state (v2) and commits cleanly (v3), preserving both edits.
	fresh, _ := store.GetOrder(agent, "ord-1")
	rec2, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: fresh.Version,
		Params:          MutationParams{RefundCents: 4_000, RestockedUnits: 1},
	})
	if err != nil || rec2.NewVersion != 3 {
		t.Fatalf("agent retry failed: %v", err)
	}

	finalOrd, _ := store.GetOrder(cust, "ord-1")
	if finalOrd.ShippingAddress != "200 New St" || finalOrd.RefundedCents != 4_000 || finalOrd.RestockedUnits != 1 {
		t.Fatalf("expected merged updates on v3, got %+v", finalOrd)
	}
}

// 5. Direct comparison: wholesale LWW clobber (clients/base today) vs CAS.
func TestWholesaleLWWClobberVsCASPreservation(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-lww")
	lww := NewSharedStateStore(ModeDualRBACLWW)
	lww.LoadTenantSnapshot(snap)

	staleCustSnap, _ := lww.GetOrder(cust, "ord-1")
	agentSnap, _ := lww.GetOrder(agent, "ord-1")

	// Agent issues 4,500-cent refund first.
	if _, err := lww.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: agentSnap.Version,
		Params:          MutationParams{RefundCents: 4_500, OrderSnapshot: &agentSnap},
	}); err != nil {
		t.Fatalf("lww agent refund failed: %v", err)
	}

	// Customer writes wholesale update from stale v1 snapshot -> clobbers RefundedCents back to 0!
	if _, err := lww.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionUpdateShipping,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: staleCustSnap.Version,
		Params: MutationParams{
			ShippingAddress: "999 Clobber Ave",
			OrderSnapshot:   &staleCustSnap,
		},
	}); err != nil {
		t.Fatalf("lww customer update failed: %v", err)
	}

	corrupted, _ := lww.GetOrder(cust, "ord-1")
	acc, _ := lww.GetAccount(cust, "acc-1")
	if corrupted.RefundedCents != 0 || acc.CreditBalanceCents != 6_500 {
		t.Fatalf("expected LWW wholesale write to desync Order.RefundedCents(0) from Account.CreditBalanceCents(6500), got ord=%+v acc=%+v", corrupted, acc)
	}
}

// 6. Supervisor co-sign threshold enforcement (including split-refund smurfing).
func TestSupervisorCoSignThresholdEnforcement(t *testing.T) {
	snap, _, agent, sup := seedBasicTenant("org-sup")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	// First partial refund of 15,000 cents (<= 20,000 threshold) succeeds without supervisor.
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params:          MutationParams{RefundCents: 15_000},
	}); err != nil {
		t.Fatalf("15000 refund should succeed: %v", err)
	}

	// Second partial refund of 8,000 cents pushes cumulative refund to 23,000 (> 20,000) -> blocked!
	_, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 2,
		Params:          MutationParams{RefundCents: 8_000},
	})
	if !IsErrorCode(err, ErrCodeSupervisorRequired) {
		t.Fatalf("expected cumulative split refund > threshold to require supervisor, got %v", err)
	}

	// Supervisor co-signs (v2 -> v3).
	if _, err := store.Apply(MutationRequest{
		Actor:           sup,
		Action:          ActionSupervisorApproveHighRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 2,
		Params:          MutationParams{SupervisorApprovalToken: "sup-ok-1"},
	}); err != nil {
		t.Fatalf("supervisor sign-off failed: %v", err)
	}

	// Now agent's 8,000-cent refund succeeds cleanly on v3 -> v4.
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 3,
		Params:          MutationParams{RefundCents: 8_000},
	}); err != nil {
		t.Fatalf("co-signed refund failed: %v", err)
	}
}

// 7. Authority lease mutual exclusion, foreign release denial, and TTL expiry.
func TestAuthorityLeaseMutualExclusionAndTTLExpiry(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-lease")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	// Non-supervisor cannot claim account_governance lease.
	_, err := store.Apply(MutationRequest{
		Actor:      agent,
		Action:     ActionAcquireLease,
		TargetKind: EntityAccount,
		TargetID:   "acc-1",
		Params:     MutationParams{LeaseScope: "account_governance", LeaseTTLSteps: 4},
	})
	if !IsErrorCode(err, ErrCodeUnauthorizedRole) {
		t.Fatalf("expected unauthorized lease scope error, got %v", err)
	}

	// Agent acquires order_settlement lease for 3 steps.
	if _, err := store.Apply(MutationRequest{
		Actor:      agent,
		Action:     ActionAcquireLease,
		TargetKind: EntityOrder,
		TargetID:   "ord-1",
		Params:     MutationParams{LeaseScope: "order_settlement", LeaseTTLSteps: 3},
	}); err != nil {
		t.Fatalf("agent lease acquire failed: %v", err)
	}

	// Customer cannot self-cancel while agent holds lease.
	_, err = store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionCustomerSelfCancelOrder,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
	})
	if !IsErrorCode(err, ErrCodeLeaseHeld) {
		t.Fatalf("expected %s while lease active, got %v", ErrCodeLeaseHeld, err)
	}

	// Customer cannot release agent's lease.
	_, err = store.Apply(MutationRequest{
		Actor:      cust,
		Action:     ActionReleaseLease,
		TargetKind: EntityOrder,
		TargetID:   "ord-1",
		Params:     MutationParams{LeaseScope: "order_settlement"},
	})
	if !IsErrorCode(err, ErrCodeLeaseNotOwned) {
		t.Fatalf("expected %s on foreign release, got %v", ErrCodeLeaseNotOwned, err)
	}

	// Advance logical clock past TTL (3 steps) -> customer can now self-cancel!
	store.AdvanceStep(3)
	if _, err := store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionCustomerSelfCancelOrder,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("expected lease expiry to allow customer self-cancel, got %v", err)
	}
}

// 8. HIP-1347 §1.4 idempotency key replay protection and payload-mismatch rejection.
func TestIdempotencyKeyDeduplicationAndMismatchRejection(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-idem")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	req := MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		IdempotencyKey:  "idem-refund-001",
		Params:          MutationParams{RefundCents: 6_000},
	}

	r1, err := store.Apply(req)
	if err != nil || r1.IdempotentReplay {
		t.Fatalf("first apply failed: r1=%+v err=%v", r1, err)
	}

	// Even after customer updates shipping (bumping Order version to 3), retrying
	// the already-committed req with stale ExpectedVersion=1 and same IdempotencyKey
	// returns the cached idempotent receipt without double-crediting!
	if _, err := store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionUpdateShipping,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 2,
		Params:          MutationParams{ShippingAddress: "55 Safe Way"},
	}); err != nil {
		t.Fatalf("shipping update failed: %v", err)
	}

	r2, err := store.Apply(req)
	if err != nil || !r2.IdempotentReplay || r2.NewVersion != r1.NewVersion {
		t.Fatalf("expected idempotent replay with original version, got r2=%+v err=%v", r2, err)
	}

	acc, _ := store.GetAccount(cust, "acc-1")
	if acc.CreditBalanceCents != 8_000 { // 2000 initial + 6000 once
		t.Fatalf("expected single 6000 credit (total 8000), got %d", acc.CreditBalanceCents)
	}

	// Reusing the same IdempotencyKey with different RefundCents MUST fail closed.
	mutated := req
	mutated.Params.RefundCents = 9_000
	if _, err := store.Apply(mutated); !IsErrorCode(err, ErrCodeIdempotencyMismatch) {
		t.Fatalf("expected %s on payload mismatch, got %v", ErrCodeIdempotencyMismatch, err)
	}
}

// 9. HIP-1348 §1.1 & §1.2 pre-execution tenant-ledger budget_exhausted taxonomy.
func TestTenantLedgerBudgetExhaustedTaxonomy(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-budget")
	snap.Accounts[0].TenantBudgetCents = 800
	snap.Accounts[0].VerifiedPIN = true
	snap.Lines[0].CustomerRoamingOptIn = true

	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	_, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentProvisionRoaming,
		TargetKind:      EntityTelecomLine,
		TargetID:        "line-1",
		ExpectedVersion: 1,
		Params: MutationParams{
			ClearBar:      true,
			ActivatePass:  "intl_day_pass",
			PassCostCents: 1_500,
		},
	})
	if !IsErrorCode(err, ErrCodeBudgetExhausted) {
		t.Fatalf("expected %s, got %v", ErrCodeBudgetExhausted, err)
	}
	opErr := err.(*OpError)
	if opErr.Layer != "tenant_ledger" || !opErr.BillingTopUpHint {
		t.Fatalf("HIP-1348 §1.2 violation: expected layer=tenant_ledger and BillingTopUpHint=true, got %+v", opErr)
	}

	// Non-tenant layers MUST NOT emit billing top-up hints (HIP-1348 §1.2).
	gwErr := NewBudgetExhaustedError("upstream_provider", "upstream circuit breaker")
	if gwErr.BillingTopUpHint {
		t.Fatalf("HIP-1348 §1.2 violation: upstream_provider must not set BillingTopUpHint")
	}

	// Verify line & budget remained untouched on pre-execution rejection.
	line, _ := store.GetTelecomLine(cust, "line-1")
	acc, _ := store.GetAccount(cust, "acc-1")
	if !line.CarrierRoamingBarred || acc.TenantBudgetCents != 800 {
		t.Fatalf("state mutated despite pre-execution budget failure: line=%+v acc=%+v", line, acc)
	}
}

// 10. Telecom dual-control stateful preconditions (PIN + handset opt-in).
func TestTelecomDualControlStatefulPreconditions(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-telecom")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	// Fails before PIN verified.
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentProvisionRoaming,
		TargetKind:      EntityTelecomLine,
		TargetID:        "line-1",
		ExpectedVersion: 1,
		Params:          MutationParams{ClearBar: true, ActivatePass: "intl_day_pass", PassCostCents: 1_500},
	}); !IsErrorCode(err, ErrCodePreconditionFailed) {
		t.Fatalf("expected precondition failure without PIN, got %v", err)
	}

	// Verify PIN, still fails without handset roaming opt-in.
	_, _ = store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionVerifyPIN,
		TargetKind:      EntityAccount,
		TargetID:        "acc-1",
		ExpectedVersion: 1,
	})
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentProvisionRoaming,
		TargetKind:      EntityTelecomLine,
		TargetID:        "line-1",
		ExpectedVersion: 1,
		Params:          MutationParams{ClearBar: true, ActivatePass: "intl_day_pass", PassCostCents: 1_500},
	}); !IsErrorCode(err, ErrCodePreconditionFailed) {
		t.Fatalf("expected precondition failure without handset opt-in, got %v", err)
	}

	// Customer opts in -> agent provisioning now succeeds on v2.
	optIn := true
	_, _ = store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionToggleDeviceRoaming,
		TargetKind:      EntityTelecomLine,
		TargetID:        "line-1",
		ExpectedVersion: 1,
		Params:          MutationParams{RoamingOptIn: &optIn, DeviceAPN: "hanzo-5g"},
	})
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentProvisionRoaming,
		TargetKind:      EntityTelecomLine,
		TargetID:        "line-1",
		ExpectedVersion: 2,
		Params:          MutationParams{ClearBar: true, ActivatePass: "intl_day_pass", PassCostCents: 1_500},
	}); err != nil {
		t.Fatalf("expected provisioning to succeed after PIN + opt-in, got %v", err)
	}
}

// 11. Support ticket resolution ordering precondition.
func TestTicketResolutionOrderingPrecondition(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-ticket")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	// Customer cannot confirm closure before agent writes resolution summary.
	_, err := store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionCustomerConfirmTicket,
		TargetKind:      EntitySupportTicket,
		TargetID:        "tkt-1",
		ExpectedVersion: 1,
		Params:          MutationParams{CustomerConfirmedClose: true},
	})
	if !IsErrorCode(err, ErrCodePreconditionFailed) {
		t.Fatalf("expected precondition error before resolution, got %v", err)
	}

	// Agent resolves -> customer confirms on v2.
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentEscalateTicket,
		TargetKind:      EntitySupportTicket,
		TargetID:        "tkt-1",
		ExpectedVersion: 1,
		Params:          MutationParams{TicketPriority: "high", TicketStatus: "resolved", ResolutionSummary: "fixed"},
	}); err != nil {
		t.Fatalf("agent resolve failed: %v", err)
	}
	if _, err := store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionCustomerConfirmTicket,
		TargetKind:      EntitySupportTicket,
		TargetID:        "tkt-1",
		ExpectedVersion: 2,
		Params:          MutationParams{CustomerConfirmedClose: true},
	}); err != nil {
		t.Fatalf("customer confirm failed: %v", err)
	}
}

// 12. Audit log single-field tamper matrix (6 distinct field mutations).
func TestAuditChainSingleFieldTamperRejection(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-tamper")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	_, _ = store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionUpdateShipping,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params:          MutationParams{ShippingAddress: "300 Oak Ave"},
	})
	_, _ = store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 2,
		Params:          MutationParams{RefundCents: 3_000},
	})

	finalDigest := store.SnapshotTenant("org-tamper").Digest()
	origLog := store.AuditLog()
	if err := ReplayAndVerify(snap, ModeDualCASLease, origLog, finalDigest); err != nil {
		t.Fatalf("untampered replay failed: %v", err)
	}

	mutators := []struct {
		name string
		fn   func([]InteractionRecord)
	}{
		{"actor_id", func(l []InteractionRecord) { l[1].ActorID = "forged-agent" }},
		{"refund_cents", func(l []InteractionRecord) { l[1].Params.RefundCents = 9_999 }},
		{"outcome", func(l []InteractionRecord) { l[0].Outcome = "denied" }},
		{"result_version", func(l []InteractionRecord) { l[1].ResultVersion = 99 }},
		{"step", func(l []InteractionRecord) { l[1].Step = 42 }},
		{"target_id", func(l []InteractionRecord) { l[0].TargetID = "ord-999" }},
	}

	for _, m := range mutators {
		tampered := make([]InteractionRecord, len(origLog))
		copy(tampered, origLog)
		m.fn(tampered)
		if err := ReplayAndVerify(snap, ModeDualCASLease, tampered, finalDigest); !IsErrorCode(err, ErrCodeTamperedLog) {
			t.Fatalf("tamper case %q was not rejected with %s: got %v", m.name, ErrCodeTamperedLog, err)
		}
	}
}

// 13. Audit log entry deletion and sequence reordering rejection.
func TestAuditChainDeletionAndReorderRejection(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-del")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	_, _ = store.Apply(MutationRequest{Actor: cust, Action: ActionVerifyPIN, TargetKind: EntityAccount, TargetID: "acc-1", ExpectedVersion: 1})
	_, _ = store.Apply(MutationRequest{Actor: cust, Action: ActionUpdateShipping, TargetKind: EntityOrder, TargetID: "ord-1", ExpectedVersion: 1, Params: MutationParams{ShippingAddress: "A"}})
	_, _ = store.Apply(MutationRequest{Actor: agent, Action: ActionAgentIssueRefund, TargetKind: EntityOrder, TargetID: "ord-1", ExpectedVersion: 2, Params: MutationParams{RefundCents: 1_000}})

	log := store.AuditLog()
	// Delete middle entry.
	dropped := []InteractionRecord{log[0], log[2]}
	if err := VerifyAuditChain(dropped); !IsErrorCode(err, ErrCodeTamperedLog) {
		t.Fatalf("expected dropped middle entry to fail chain check, got %v", err)
	}

	// Swap first two entries.
	swapped := []InteractionRecord{log[1], log[0], log[2]}
	if err := VerifyAuditChain(swapped); !IsErrorCode(err, ErrCodeTamperedLog) {
		t.Fatalf("expected swapped entries to fail chain check, got %v", err)
	}
}

// 14. Replay detects final state digest mismatch even when chain hashes are intact.
func TestReplayDetectsFinalDigestTampering(t *testing.T) {
	snap, cust, _, _ := seedBasicTenant("org-digest")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)
	_, _ = store.Apply(MutationRequest{Actor: cust, Action: ActionVerifyPIN, TargetKind: EntityAccount, TargetID: "acc-1", ExpectedVersion: 1})

	forgedDigest := "0000000000000000000000000000000000000000000000000000000000000000"
	if err := ReplayAndVerify(snap, ModeDualCASLease, store.AuditLog(), forgedDigest); !IsErrorCode(err, ErrCodeTamperedLog) {
		t.Fatalf("expected forged final digest to fail ReplayAndVerify, got %v", err)
	}
}

// 15. Concurrent OS goroutines: 200 dual-writer settlement races.
func TestConcurrentGoroutineDualWriterNoDoubleRefund(t *testing.T) {
	summary := RunConcurrentGoroutineRaces(200)
	if summary.LWWDoubleRefundViolations == 0 {
		t.Fatalf("expected wholesale LWW concurrent races to exhibit double-refund violations, got 0")
	}
	if summary.CASLeaseDoubleRefundViolations != 0 {
		t.Fatalf("expected 0 double-refund violations under ModeDualCASLease, got %d", summary.CASLeaseDoubleRefundViolations)
	}
	if summary.CASLeaseReplayVerified != 200 {
		t.Fatalf("expected all 200 concurrent race logs to verify on replay, got %d", summary.CASLeaseReplayVerified)
	}
}

// 16. Multi-goroutine hammer test (16 concurrent readers/writers + replay check).
func TestConcurrentGoroutineMultiActorStress(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-stress")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	var wg sync.WaitGroup
	var refundWins atomic.Int32
	start := make(chan struct{})

	for g := 0; g < 8; g++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			<-start
			ord, err := store.GetOrder(cust, "ord-1")
			if err != nil {
				return
			}
			_, _ = store.Apply(MutationRequest{
				Actor:           cust,
				Action:          ActionUpdateShipping,
				TargetKind:      EntityOrder,
				TargetID:        "ord-1",
				ExpectedVersion: ord.Version,
				IdempotencyKey:  fmt.Sprintf("stress-ship-%d", idx),
				Params:          MutationParams{ShippingAddress: fmt.Sprintf("%d Parallel Way", idx)},
			})
		}(g)

		go func(idx int) {
			defer wg.Done()
			<-start
			ord, err := store.GetOrder(agent, "ord-1")
			if err != nil {
				return
			}
			if _, err := store.Apply(MutationRequest{
				Actor:           agent,
				Action:          ActionAgentIssueRefund,
				TargetKind:      EntityOrder,
				TargetID:        "ord-1",
				ExpectedVersion: ord.Version,
				IdempotencyKey:  fmt.Sprintf("stress-ref-%d", idx),
				Params:          MutationParams{RefundCents: 2_000},
			}); err == nil {
				refundWins.Add(1)
			}
		}(g)
	}

	close(start)
	wg.Wait()

	final := store.SnapshotTenant("org-stress")
	if err := ReplayAndVerify(snap, ModeDualCASLease, store.AuditLog(), final.Digest()); err != nil {
		t.Fatalf("concurrent stress log failed replay: %v", err)
	}
	expectedCredit := int64(2_000) + int64(refundWins.Load())*2_000
	if final.Accounts[0].CreditBalanceCents != expectedCredit || final.Orders[0].RefundedCents != int64(refundWins.Load())*2_000 {
		t.Fatalf("ledger conservation broken under concurrent stress: wins=%d acc=%+v ord=%+v",
			refundWins.Load(), final.Accounts[0], final.Orders[0])
	}
}

// 17. Deterministic SplitMix64 reproducibility across runs.
func TestDeterministicSeedReproducibility(t *testing.T) {
	c1 := RunCell(RegimeHighContentionFaults, ModeDualCASLease, 20261101, 150)
	c2 := RunCell(RegimeHighContentionFaults, ModeDualCASLease, 20261101, 150)
	c3 := RunCell(RegimeHighContentionFaults, ModeDualUncoordinated, 20261199, 150)

	b1, _ := json.Marshal(c1)
	b2, _ := json.Marshal(c2)
	if !bytes.Equal(b1, b2) {
		t.Fatalf("identical seeds produced divergent CellMetrics:\n%s\nvs\n%s", string(b1), string(b2))
	}
	if c1.TaskSuccessRate == c3.TaskSuccessRate && c1.SafetyViolationCount == c3.SafetyViolationCount {
		t.Fatal("expected distinct mode/seed runs to produce distinct metrics")
	}
}

// 18. OpenAI/Enso adapter fail-closed check when unconfigured (LLM.md Invariant #4).
func TestOpenAIToolAgentFailClosedWhenUnconfigured(t *testing.T) {
	_, err := NewOpenAIToolAgent(OpenAICompatConfig{})
	if !IsErrorCode(err, ErrCodeLLMUnconfigured) {
		t.Fatalf("expected fail-closed %s when unconfigured, got %v", ErrCodeLLMUnconfigured, err)
	}
}

// 19. OpenAI/Enso adapter end-to-end CAS conflict recovery over local httptest.Server.
func TestOpenAIToolAgentConflictReadReplanOverHTTPTest(t *testing.T) {
	snap, cust, agent, _ := seedBasicTenant("org-llm")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	// Human concurrently bumps Order ord-1 to Version=2 before LLM turn 1 commits!
	if _, err := store.Apply(MutationRequest{
		Actor:           cust,
		Action:          ActionUpdateShipping,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params:          MutationParams{ShippingAddress: "777 Concurrent Human Ln", DeliveryNotes: "side_door"},
	}); err != nil {
		t.Fatalf("human setup update failed: %v", err)
	}

	var callNum atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		step := callNum.Add(1)
		w.Header().Set("Content-Type", "application/json")

		var resp chatCompletionResponse
		switch step {
		case 1:
			// Model initially tries stale expected_version=1 -> gets version_conflict.
			resp.Choices = append(resp.Choices, struct {
				Message      chatMessage `json:"message"`
				FinishReason string      `json:"finish_reason"`
			}{
				Message: chatMessage{
					Role: "assistant",
					ToolCalls: []chatToolCall{{
						ID:   "call-1",
						Type: "function",
						Function: chatFunctionCall{
							Name:      "apply_state_mutation",
							Arguments: `{"action":"agent_issue_refund","target_kind":"order","target_id":"ord-1","expected_version":1,"params":{"refund_cents":3500,"restocked_units":1}}`,
						},
					}},
				},
				FinishReason: "tool_calls",
			})
		case 2:
			// Model reacts to version_conflict by calling read_tenant_state.
			resp.Choices = append(resp.Choices, struct {
				Message      chatMessage `json:"message"`
				FinishReason string      `json:"finish_reason"`
			}{
				Message: chatMessage{
					Role: "assistant",
					ToolCalls: []chatToolCall{{
						ID:       "call-2",
						Type:     "function",
						Function: chatFunctionCall{Name: "read_tenant_state", Arguments: `{}`},
					}},
				},
				FinishReason: "tool_calls",
			})
		case 3:
			// Model submits refreshed expected_version=2 -> succeeds!
			resp.Choices = append(resp.Choices, struct {
				Message      chatMessage `json:"message"`
				FinishReason string      `json:"finish_reason"`
			}{
				Message: chatMessage{
					Role: "assistant",
					ToolCalls: []chatToolCall{{
						ID:   "call-3",
						Type: "function",
						Function: chatFunctionCall{
							Name:      "apply_state_mutation",
							Arguments: `{"action":"agent_issue_refund","target_kind":"order","target_id":"ord-1","expected_version":2,"params":{"refund_cents":3500,"restocked_units":1}}`,
						},
					}},
				},
				FinishReason: "tool_calls",
			})
		default:
			resp.Choices = append(resp.Choices, struct {
				Message      chatMessage `json:"message"`
				FinishReason string      `json:"finish_reason"`
			}{
				Message:      chatMessage{Role: "assistant", Content: "Refund completed without clobbering customer shipping address."},
				FinishReason: "stop",
			})
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	llmAgent, err := NewOpenAIToolAgent(OpenAICompatConfig{
		BaseURL:    srv.URL,
		APIKey:     "test-token",
		Model:      "zen-4-instruct",
		HTTPClient: srv.Client(),
		MaxTurns:   6,
	})
	if err != nil {
		t.Fatalf("NewOpenAIToolAgent: %v", err)
	}

	turns, err := llmAgent.ExecuteTurnLoop(context.Background(), store, agent, "ep-llm-1", "Refund $35.00 on ord-1")
	if err != nil {
		t.Fatalf("ExecuteTurnLoop failed: %v", err)
	}
	if turns != 4 {
		t.Fatalf("expected 4 turns (stale try -> read -> fresh commit -> stop), got %d", turns)
	}

	finalOrd, _ := store.GetOrder(cust, "ord-1")
	if finalOrd.Version != 3 || finalOrd.RefundedCents != 3_500 || finalOrd.ShippingAddress != "777 Concurrent Human Ln" {
		t.Fatalf("expected merged v3 order preserving human shipping + LLM refund, got %+v", finalOrd)
	}
}

// 20. Verify declared research hypotheses H1-H4 quantitatively.
func TestHypothesesVerdictsHoldOnBenchmarkMatrix(t *testing.T) {
	rep := RunMatrix(120)
	single := rep.AggregatedByMode[string(ModeSingleControl)]
	uncoord := rep.AggregatedByMode[string(ModeDualUncoordinated)]
	rbacLWW := rep.AggregatedByMode[string(ModeDualRBACLWW)]
	casLease := rep.AggregatedByMode[string(ModeDualCASLease)]

	// H1: RBAC-only LWW drops below single-control by >= 25 percentage points due
	// to lost updates, double refunds, and duplicate retry effects.
	if single.TaskSuccessRate-rbacLWW.TaskSuccessRate < 0.25 {
		t.Fatalf("H1 violated: expected >=25pp gap between single_control (%.4f) and dual_rbac_lww (%.4f)",
			single.TaskSuccessRate, rbacLWW.TaskSuccessRate)
	}
	if rbacLWW.SafetyViolationRate < 0.25 || uncoord.PermViolationCount <= rbacLWW.PermViolationCount {
		t.Fatalf("H1 violated: unexpected safety/perm profile uncoord=%+v rbacLWW=%+v", uncoord, rbacLWW)
	}

	// H2: CAS + Lease eliminates 100% of safety violations and recovers >= 95% of
	// single-control task completion with bounded overhead.
	if casLease.SafetyViolationRate != 0 || casLease.LedgerViolationCount != 0 || casLease.PermViolationCount != 0 {
		t.Fatalf("H2 violated: expected zero safety violations in dual_cas_lease, got %+v", casLease)
	}
	if casLease.TaskSuccessRate < 0.95 {
		t.Fatalf("H2 violated: expected >=95%% task success in dual_cas_lease, got %.4f", casLease.TaskSuccessRate)
	}
	if casLease.MeanStoreCallsPerEp-rbacLWW.MeanStoreCallsPerEp > 2.5 {
		t.Fatalf("H2 overhead bound exceeded: cas=%.2f vs rbacLWW=%.2f",
			casLease.MeanStoreCallsPerEp, rbacLWW.MeanStoreCallsPerEp)
	}

	// H3: 100% of episodes across all modes pass deterministic hash-chain replay.
	if casLease.ReplayVerifiedRate != 1.0 {
		t.Fatalf("H3 violated: replay verified rate = %.4f", casLease.ReplayVerifiedRate)
	}

	// H4: Under high contention with stalled-human lease holds, fail-closed safe
	// handoffs occur (>0) with zero ledger corruption.
	if casLease.SafeHandoffCount == 0 {
		t.Fatalf("H4 expected failure mode not exercised: SafeHandoffCount=0")
	}
}

// 21. Drift lock: committed testdata/results.json must match live RunMatrix bit-for-bit.
func TestCommittedResultsJSONMatchesLiveCode(t *testing.T) {
	path := filepath.Join("testdata", "results.json")
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	liveReport := RunMatrix(DefaultEpisodesPerCell)
	liveBytes, err := MarshalMatrixJSON(liveReport)
	if err != nil {
		t.Fatalf("marshal live matrix: %v", err)
	}

	if !bytes.Equal(bytes.TrimSpace(committed), bytes.TrimSpace(liveBytes)) {
		t.Fatalf("testdata/results.json is out of sync with RunMatrix(%d); re-run tau2dualsim -out research/tau2dualcontrol/testdata/results.json", DefaultEpisodesPerCell)
	}
}

// 22. Validate committed JSON Schema file and cross-check required keys against testdata/results.json.
func TestCommittedSchemaJSONValid(t *testing.T) {
	schemaPath := filepath.Join("results", "schema.json")
	rawSchema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read %s: %v", schemaPath, err)
	}
	var schemaDoc struct {
		Schema      string   `json:"$schema"`
		ID          string   `json:"$id"`
		Required    []string `json:"required"`
		Definitions map[string]struct {
			Required   []string                   `json:"required"`
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"definitions"`
	}
	if err := json.Unmarshal(rawSchema, &schemaDoc); err != nil {
		t.Fatalf("invalid JSON in %s: %v", schemaPath, err)
	}
	if schemaDoc.Schema == "" || schemaDoc.ID == "" || len(schemaDoc.Required) == 0 {
		t.Fatalf("incomplete top-level schema metadata: %+v", schemaDoc)
	}
	for _, defName := range []string{"cell_metrics", "mode_summary", "concurrent_race_summary"} {
		def, ok := schemaDoc.Definitions[defName]
		if !ok || len(def.Required) == 0 || len(def.Properties) == 0 {
			t.Fatalf("schema definition %q missing required or properties block", defName)
		}
		for _, reqKey := range def.Required {
			if _, propExists := def.Properties[reqKey]; !propExists {
				t.Fatalf("schema definition %q lists required key %q with no property schema", defName, reqKey)
			}
		}
	}

	resultsRaw, err := os.ReadFile(filepath.Join("testdata", "results.json"))
	if err != nil {
		t.Fatalf("read results.json: %v", err)
	}
	var topMap map[string]json.RawMessage
	if err := json.Unmarshal(resultsRaw, &topMap); err != nil {
		t.Fatalf("unmarshal results.json map: %v", err)
	}
	for _, reqKey := range schemaDoc.Required {
		if _, ok := topMap[reqKey]; !ok {
			t.Fatalf("results.json missing top-level required key %q", reqKey)
		}
	}
}

// 23. Intra-tenant customer horizontal isolation (BOLA/IDOR defense).
func TestIntraTenantCustomerHorizontalIsolation(t *testing.T) {
	snap, _, _, _ := seedBasicTenant("org-shared")
	otherCust := Principal{
		Org:             "org-shared",
		ID:              "cust-other-99",
		Role:            RoleCustomer,
		CredentialScope: ScopeCustomerSession,
	}

	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	if _, err := store.GetAccount(otherCust, "acc-1"); !IsErrorCode(err, ErrCodeForbiddenOwner) {
		t.Fatalf("expected %s on horizontal account read, got %v", ErrCodeForbiddenOwner, err)
	}
	if _, err := store.GetOrder(otherCust, "ord-1"); !IsErrorCode(err, ErrCodeForbiddenOwner) {
		t.Fatalf("expected %s on horizontal order read, got %v", ErrCodeForbiddenOwner, err)
	}
	if _, err := store.GetTelecomLine(otherCust, "line-1"); !IsErrorCode(err, ErrCodeForbiddenOwner) {
		t.Fatalf("expected %s on horizontal line read, got %v", ErrCodeForbiddenOwner, err)
	}
	if _, err := store.GetSupportTicket(otherCust, "tkt-1"); !IsErrorCode(err, ErrCodeForbiddenOwner) {
		t.Fatalf("expected %s on horizontal ticket read, got %v", ErrCodeForbiddenOwner, err)
	}

	// Horizontal write & lease attempts by another customer in the same org must fail.
	if _, err := store.Apply(MutationRequest{
		Actor:           otherCust,
		Action:          ActionCustomerSelfCancelOrder,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
	}); !IsErrorCode(err, ErrCodeForbiddenOwner) {
		t.Fatalf("expected %s on horizontal self-cancel, got %v", ErrCodeForbiddenOwner, err)
	}
	if _, err := store.Apply(MutationRequest{
		Actor:      otherCust,
		Action:     ActionAcquireLease,
		TargetKind: EntityOrder,
		TargetID:   "ord-1",
		Params:     MutationParams{LeaseScope: "order_settlement", LeaseTTLSteps: 3},
	}); !IsErrorCode(err, ErrCodeForbiddenOwner) {
		t.Fatalf("expected %s on horizontal lease claim, got %v", ErrCodeForbiddenOwner, err)
	}
}

// 24. Cumulative SLA credit smurfing cap and negative input rejection.
func TestCumulativeSLASmurfingAndNegativeParameterGuards(t *testing.T) {
	snap, _, agent, sup := seedBasicTenant("org-bounds")
	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snap)

	// Negative parameters must fail closed with ErrCodeInvalidArgument.
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentEscalateTicket,
		TargetKind:      EntitySupportTicket,
		TargetID:        "tkt-1",
		ExpectedVersion: 1,
		Params:          MutationParams{SLACreditCents: -500},
	}); !IsErrorCode(err, ErrCodeInvalidArgument) {
		t.Fatalf("expected negative SLA credit rejection, got %v", err)
	}
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params:          MutationParams{RefundCents: 1_000, RestockedUnits: -2},
	}); !IsErrorCode(err, ErrCodeInvalidArgument) {
		t.Fatalf("expected negative restocked_units rejection, got %v", err)
	}
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentProvisionRoaming,
		TargetKind:      EntityTelecomLine,
		TargetID:        "line-1",
		ExpectedVersion: 1,
		Params:          MutationParams{PassCostCents: -100},
	}); !IsErrorCode(err, ErrCodeInvalidArgument) {
		t.Fatalf("expected negative pass_cost_cents rejection, got %v", err)
	}

	// Cumulative SLA smurfing: 3,000 + 3,000 = 6,000 > MaxSLACreditCents (5,000) -> blocked!
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentEscalateTicket,
		TargetKind:      EntitySupportTicket,
		TargetID:        "tkt-1",
		ExpectedVersion: 1,
		Params:          MutationParams{TicketPriority: "high", SLACreditCents: 3_000},
	}); err != nil {
		t.Fatalf("first 3,000 SLA credit failed: %v", err)
	}
	if _, err := store.Apply(MutationRequest{
		Actor:           agent,
		Action:          ActionAgentEscalateTicket,
		TargetKind:      EntitySupportTicket,
		TargetID:        "tkt-1",
		ExpectedVersion: 2,
		Params:          MutationParams{SLACreditCents: 3_000},
	}); !IsErrorCode(err, ErrCodeSupervisorRequired) {
		t.Fatalf("expected cumulative SLA smurfing > 5000 to require supervisor, got %v", err)
	}

	// Supervisor budget adjustment posts ledger entry and rejects negative resulting budget.
	if _, err := store.Apply(MutationRequest{
		Actor:           sup,
		Action:          ActionModifyAccountTierOrLimit,
		TargetKind:      EntityAccount,
		TargetID:        "acc-1",
		ExpectedVersion: 2, // bumped when first 3,000 SLA credit posted
		Params:          MutationParams{BudgetDeltaCents: -50_000},
	}); !IsErrorCode(err, ErrCodeInvalidArgument) {
		t.Fatalf("expected negative resulting budget rejection, got %v", err)
	}
	if _, err := store.Apply(MutationRequest{
		Actor:           sup,
		Action:          ActionModifyAccountTierOrLimit,
		TargetKind:      EntityAccount,
		TargetID:        "acc-1",
		ExpectedVersion: 2,
		Params:          MutationParams{NewTier: "enterprise", BudgetDeltaCents: 5_000},
	}); err != nil {
		t.Fatalf("supervisor budget top-up failed: %v", err)
	}
}

// 25. Deep-clone pointer aliasing immunity and multi-tenant ReplayAndVerifyMulti.
func TestAuditLogPointerAliasingImmunityAndMultiTenantReplay(t *testing.T) {
	snapA, custA, _, _ := seedBasicTenant("org-multi-a")
	snapB, _, agentB, _ := seedBasicTenant("org-multi-b")

	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snapA)
	store.LoadTenantSnapshot(snapB)

	ordCopy, _ := store.GetOrder(custA, "ord-1")
	optIn := true
	if _, err := store.Apply(MutationRequest{
		Actor:           custA,
		Action:          ActionUpdateShipping,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params: MutationParams{
			ShippingAddress: "123 Immutable Way",
			RoamingOptIn:    &optIn,
			OrderSnapshot:   &ordCopy,
		},
	}); err != nil {
		t.Fatalf("org-multi-a apply failed: %v", err)
	}

	// Mutate caller's local pointers AFTER Apply returns -> must NOT corrupt store audit log!
	ordCopy.ShippingAddress = "MUTATED_AFTER_APPLY"
	optIn = false

	if _, err := store.Apply(MutationRequest{
		Actor:           agentB,
		Action:          ActionAgentIssueRefund,
		TargetKind:      EntityOrder,
		TargetID:        "ord-1",
		ExpectedVersion: 1,
		Params:          MutationParams{RefundCents: 2_500},
	}); err != nil {
		t.Fatalf("org-multi-b apply failed: %v", err)
	}

	// Mutate returned AuditLog() pointer fields -> must NOT corrupt internal store log!
	exported := store.AuditLog()
	if exported[0].Params.OrderSnapshot != nil {
		exported[0].Params.OrderSnapshot.PaidCents = 999_999
	}

	expectedDigests := map[string]string{
		"org-multi-a": store.SnapshotTenant("org-multi-a").Digest(),
		"org-multi-b": store.SnapshotTenant("org-multi-b").Digest(),
	}
	if err := ReplayAndVerifyMulti([]TenantSnapshot{snapA, snapB}, ModeDualCASLease, store.AuditLog(), expectedDigests); err != nil {
		t.Fatalf("multi-tenant ReplayAndVerifyMulti failed: %v", err)
	}
}

// 26. Lease scope/entity validation, cross-tenant lease denial, and expired-lease idempotency safety.
func TestExpiredLeaseIdempotencyAndScopeTargetValidation(t *testing.T) {
	snapA, custA, agentA, _ := seedBasicTenant("org-lease-a")
	snapB, _, _, _ := seedBasicTenant("org-lease-b")
	snapB.Orders = append(snapB.Orders, Order{Org: "org-lease-b", ID: "ord-foreign", Version: 1, AccountID: "acc-1", Status: "paid", PaidCents: 5_000})

	store := NewSharedStateStore(ModeDualCASLease)
	store.LoadTenantSnapshot(snapA)
	store.LoadTenantSnapshot(snapB)

	// Foreign-org lease attempt -> ErrCodeCrossTenant.
	if _, err := store.Apply(MutationRequest{
		Actor:      agentA,
		Action:     ActionAcquireLease,
		TargetKind: EntityOrder,
		TargetID:   "ord-foreign",
		Params:     MutationParams{LeaseScope: "order_settlement", LeaseTTLSteps: 3},
	}); !IsErrorCode(err, ErrCodeCrossTenant) {
		t.Fatalf("expected %s on foreign lease, got %v", ErrCodeCrossTenant, err)
	}

	// Nonexistent entity lease attempt -> ErrCodeNotFound.
	if _, err := store.Apply(MutationRequest{
		Actor:      agentA,
		Action:     ActionAcquireLease,
		TargetKind: EntityOrder,
		TargetID:   "ord-missing",
		Params:     MutationParams{LeaseScope: "order_settlement", LeaseTTLSteps: 3},
	}); !IsErrorCode(err, ErrCodeNotFound) {
		t.Fatalf("expected %s on phantom entity lease, got %v", ErrCodeNotFound, err)
	}

	// Mismatched (EntityTelecomLine, "order_settlement") -> ErrCodeInvalidArgument.
	if _, err := store.Apply(MutationRequest{
		Actor:      agentA,
		Action:     ActionAcquireLease,
		TargetKind: EntityTelecomLine,
		TargetID:   "line-1",
		Params:     MutationParams{LeaseScope: "order_settlement", LeaseTTLSteps: 3},
	}); !IsErrorCode(err, ErrCodeInvalidArgument) {
		t.Fatalf("expected %s on mismatched target_kind/lease_scope, got %v", ErrCodeInvalidArgument, err)
	}

	// Non-supervisor cannot call ActionReleaseLease on "account_governance".
	if _, err := store.Apply(MutationRequest{
		Actor:      agentA,
		Action:     ActionReleaseLease,
		TargetKind: EntityAccount,
		TargetID:   "acc-1",
		Params:     MutationParams{LeaseScope: "account_governance"},
	}); !IsErrorCode(err, ErrCodeUnauthorizedRole) {
		t.Fatalf("expected %s on unauthorized scope release, got %v", ErrCodeUnauthorizedRole, err)
	}

	// Agent acquires lease with IdempotencyKey, TTL expires, Customer acquires lease,
	// then Agent replays old IdempotencyKey -> MUST fail with ErrCodeLeaseHeld!
	leaseReq := MutationRequest{
		Actor:          agentA,
		Action:         ActionAcquireLease,
		TargetKind:     EntityOrder,
		TargetID:       "ord-1",
		IdempotencyKey: "stale-lease-idem-key",
		Params:         MutationParams{LeaseScope: "order_settlement", LeaseTTLSteps: 2},
	}
	if _, err := store.Apply(leaseReq); err != nil {
		t.Fatalf("initial lease acquire failed: %v", err)
	}
	store.AdvanceStep(3)
	if _, err := store.Apply(MutationRequest{
		Actor:      custA,
		Action:     ActionAcquireLease,
		TargetKind: EntityOrder,
		TargetID:   "ord-1",
		Params:     MutationParams{LeaseScope: "order_settlement", LeaseTTLSteps: 4},
	}); err != nil {
		t.Fatalf("customer takeover lease failed: %v", err)
	}
	if _, err := store.Apply(leaseReq); !IsErrorCode(err, ErrCodeLeaseHeld) {
		t.Fatalf("replaying expired lease idempotency key must fail with %s when held by another principal, got %v", ErrCodeLeaseHeld, err)
	}
}
