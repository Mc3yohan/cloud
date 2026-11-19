// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0 OR MIT

// Package tau2dualcontrol implements a clean-room, deterministic Go benchmark
// and shared-state coordination harness for hanzoai/cloud#30 ("Dual-control
// business-agent benchmark (tau2-bench)", tracked under HIP-1348 and HIP-1347
// §2.9).
//
// Upstream reference (paper & benchmark conceptual baseline only; zero upstream
// code or dataset files imported):
//   - V. Barres, H. Dong, S. Ray, X. Si, K. Narasimhan, "tau2-Bench: Evaluating
//     Conversational Agents in a Dual-Control Environment," arXiv:2506.07982,
//     2025. Repository: https://github.com/sierra-research/tau2-bench (MIT).
//
// Architectural alignment with hanzoai/cloud:
//   - clients/base/base.go currently replaces JSON documents wholesale via
//     unversioned PUT (last-writer-wins) scoped by validated tenant org.
//   - clients/tasks/tasks.go implements guarded lease-and-acknowledge claims.
//   - audit/record.go implements SHA-256 hash-chained append-only audit records.
//   - HIP-1347 §1.1 & §1.4 mandate disjoint per-actor credentials and
//     per-attempt idempotency_key deduplication on retried tool invocations.
//   - HIP-1348 §1.1 & §1.2 mandate pre-execution tenant-ledger authorization
//     and structured budget_exhausted (layer="tenant_ledger") attribution.
//
// Standard library only; zero external dependencies and zero changes to go.mod.
package tau2dualcontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// SupervisorThresholdCents is the cumulative per-order refund ceiling (USD
// $200.00 = 20,000 cents) above which a support_agent mutation requires an
// explicit billing_supervisor co-signature on the target Order.
const SupervisorThresholdCents int64 = 20_000

// MaxSLACreditCents bounds support-agent discretionary SLA ticket credits (USD
// $50.00 = 5,000 cents) without supervisor escalation.
const MaxSLACreditCents int64 = 5_000

// DefaultLeaseTTLSteps is the bounded logical step duration of an authority
// lease before automatic expiration returns the resource to the unleased pool
// (mirroring clients/tasks/tasks.go lease expiry semantics).
const DefaultLeaseTTLSteps uint64 = 6

// Mode selects the shared-state coordination protocol under evaluation.
type Mode string

const (
	// ModeSingleControl models the classical tau-bench single-controller setting:
	// the agent executes tool mutations sequentially while the human counterparty
	// provides cooperative state handshakes without overlapping concurrent writes.
	ModeSingleControl Mode = "single_control"

	// ModeDualUncoordinated models naive shared-state dual-control where both
	// human and agent perform wholesale document read-modify-write (matching
	// unversioned PUT /v1/base/collections/:c/:id) without state-machine RBAC,
	// optimistic version guards, authority leases, or idempotency keys.
	ModeDualUncoordinated Mode = "dual_uncoordinated"

	// ModeDualRBACLWW enforces the full state-machine RBAC matrix, disjoint
	// credential checks (HIP-1347 §1.1), and tenant-ledger budget checks
	// (HIP-1348 §1.1), while retaining unversioned wholesale last-writer-wins
	// document updates (no expected_version CAS, leases, or idempotency keys).
	// This ablation isolates concurrency/coordination bugs from permission bugs.
	ModeDualRBACLWW Mode = "dual_rbac_lww"

	// ModeDualCASLease enforces the complete proposed dual-control protocol:
	// state-machine RBAC + disjoint credentials + tenant-ledger precheck +
	// optimistic expected_version CAS + scoped authority leases + per-intent
	// idempotency_key deduplication + bounded Conflict-Read-Replan recovery.
	ModeDualCASLease Mode = "dual_cas_lease"
)

// AllModes returns the four benchmark modes in canonical comparison order.
func AllModes() []Mode {
	return []Mode{
		ModeSingleControl,
		ModeDualUncoordinated,
		ModeDualRBACLWW,
		ModeDualCASLease,
	}
}

// Role identifies the trust class of an actor mutating shared business state.
type Role string

const (
	RoleCustomer          Role = "customer"
	RoleSupportAgent      Role = "support_agent"
	RoleBillingSupervisor Role = "billing_supervisor"
)

// CredentialScope models HIP-1347 §1.1 disjoint credentials across trust boundaries.
type CredentialScope string

const (
	ScopeCustomerSession CredentialScope = "session:customer"
	ScopeAgentMCPTicket  CredentialScope = "mcp:support_ticket"
	ScopeSupervisorIAM   CredentialScope = "iam:billing_supervisor"
	// ScopeAIInferenceOnly represents a per-run model-inference credential that
	// HIP-1347 §1.1 explicitly blocks from mutating business or KMS state.
	ScopeAIInferenceOnly CredentialScope = "ai:inference"
)

// Principal represents an authenticated caller whose Org and Role were minted
// from a verified identity boundary (see audit/record.go and org_scope.go).
type Principal struct {
	Org             string          `json:"org"`
	ID              string          `json:"id"`
	Role            Role            `json:"role"`
	CredentialScope CredentialScope `json:"credential_scope"`
}

// ActionKind enumerates all typed operations against shared business state.
type ActionKind string

const (
	ActionVerifyPIN                   ActionKind = "verify_pin"
	ActionUpdateShipping              ActionKind = "update_shipping"
	ActionCustomerSelfCancelOrder     ActionKind = "customer_self_cancel_order"
	ActionToggleDeviceRoaming         ActionKind = "toggle_device_roaming"
	ActionCustomerConfirmTicket       ActionKind = "customer_confirm_ticket"
	ActionAgentIssueRefund            ActionKind = "agent_issue_refund"
	ActionAgentProvisionRoaming       ActionKind = "agent_provision_roaming"
	ActionAgentEscalateTicket         ActionKind = "agent_escalate_ticket"
	ActionSupervisorApproveHighRefund ActionKind = "supervisor_approve_high_refund"
	ActionModifyAccountTierOrLimit    ActionKind = "modify_account_tier_or_limit"
	ActionAcquireLease                ActionKind = "acquire_lease"
	ActionReleaseLease                ActionKind = "release_lease"
)

// EntityKind names the shared record collection.
type EntityKind string

const (
	EntityAccount       EntityKind = "account"
	EntityOrder         EntityKind = "order"
	EntityTelecomLine   EntityKind = "telecom_line"
	EntitySupportTicket EntityKind = "support_ticket"
)

// ErrorCode classifies deterministic rejection reasons in the state machine.
type ErrorCode string

const (
	ErrCodeUnauthorizedRole    ErrorCode = "unauthorized_role"
	ErrCodeForbiddenOwner      ErrorCode = "forbidden_resource_owner"
	ErrCodeDisjointCredential  ErrorCode = "disjoint_credential_violation"
	ErrCodeCrossTenant         ErrorCode = "cross_tenant_denied"
	ErrCodeSupervisorRequired  ErrorCode = "supervisor_approval_required"
	ErrCodeVersionConflict     ErrorCode = "version_conflict"
	ErrCodeLeaseHeld           ErrorCode = "lease_conflict"
	ErrCodeLeaseNotOwned       ErrorCode = "lease_not_owned"
	ErrCodePreconditionFailed  ErrorCode = "precondition_failed"
	ErrCodeBudgetExhausted     ErrorCode = "budget_exhausted"
	ErrCodeIdempotencyMismatch ErrorCode = "idempotency_payload_mismatch"
	ErrCodeTamperedLog         ErrorCode = "tampered_audit_log"
	ErrCodeTransientTimeout    ErrorCode = "transient_transport_timeout"
	ErrCodeNotFound            ErrorCode = "not_found"
	ErrCodeInvalidArgument     ErrorCode = "invalid_argument"
)

// OpError is the structured error returned by shared-state operations.
// When Code == ErrCodeBudgetExhausted, Layer is populated per HIP-1348 §1.2
// and BillingTopUpHint is set exclusively when Layer == "tenant_ledger".
type OpError struct {
	Code             ErrorCode `json:"code"`
	Layer            string    `json:"layer,omitempty"`
	BillingTopUpHint bool      `json:"billing_top_up_hint,omitempty"`
	CurrentVersion   uint64    `json:"current_version,omitempty"`
	LeaseHolder      string    `json:"lease_holder,omitempty"`
	Message          string    `json:"message"`
}

func (e *OpError) Error() string {
	if e.Layer != "" {
		return fmt.Sprintf("%s (layer=%s): %s", e.Code, e.Layer, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// IsErrorCode checks whether err wraps an *OpError with the given code.
func IsErrorCode(err error, code ErrorCode) bool {
	var opErr *OpError
	if errors.As(err, &opErr) {
		return opErr.Code == code
	}
	return false
}

// NewBudgetExhaustedError constructs a HIP-1348 §1.2 compliant budget failure.
// Billing top-up hints are emitted strictly when layer == "tenant_ledger".
func NewBudgetExhaustedError(layer, msg string) *OpError {
	return &OpError{
		Code:             ErrCodeBudgetExhausted,
		Layer:            layer,
		BillingTopUpHint: layer == "tenant_ledger",
		Message:          msg,
	}
}

// AuthorizeAction enforces disjoint credential scopes (HIP-1347 §1.1) and the
// role-based action permission matrix inside the state machine.
func AuthorizeAction(p Principal, action ActionKind) error {
	if p.Org == "" || p.ID == "" {
		return &OpError{
			Code:    ErrCodeUnauthorizedRole,
			Message: "principal org and id must be non-empty",
		}
	}
	if p.CredentialScope == ScopeAIInferenceOnly {
		return &OpError{
			Code:    ErrCodeDisjointCredential,
			Message: "ai:inference credentials cannot mutate business or tenant state (HIP-1347 §1.1)",
		}
	}

	// Verify that the credential scope matches the asserted role.
	switch p.Role {
	case RoleCustomer:
		if p.CredentialScope != ScopeCustomerSession {
			return &OpError{
				Code:    ErrCodeDisjointCredential,
				Message: fmt.Sprintf("customer principal requires %s, got %s", ScopeCustomerSession, p.CredentialScope),
			}
		}
	case RoleSupportAgent:
		if p.CredentialScope != ScopeAgentMCPTicket {
			return &OpError{
				Code:    ErrCodeDisjointCredential,
				Message: fmt.Sprintf("support_agent principal requires %s, got %s", ScopeAgentMCPTicket, p.CredentialScope),
			}
		}
	case RoleBillingSupervisor:
		if p.CredentialScope != ScopeSupervisorIAM {
			return &OpError{
				Code:    ErrCodeDisjointCredential,
				Message: fmt.Sprintf("billing_supervisor principal requires %s, got %s", ScopeSupervisorIAM, p.CredentialScope),
			}
		}
	default:
		return &OpError{
			Code:    ErrCodeUnauthorizedRole,
			Message: fmt.Sprintf("unknown principal role %q", p.Role),
		}
	}

	// Action-to-Role permission matrix.
	switch action {
	case ActionVerifyPIN,
		ActionUpdateShipping,
		ActionCustomerSelfCancelOrder,
		ActionToggleDeviceRoaming,
		ActionCustomerConfirmTicket:
		if p.Role != RoleCustomer {
			return &OpError{
				Code:    ErrCodeUnauthorizedRole,
				Message: fmt.Sprintf("action %s restricted to customer role, caller is %s", action, p.Role),
			}
		}
		return nil

	case ActionAgentIssueRefund,
		ActionAgentProvisionRoaming,
		ActionAgentEscalateTicket:
		if p.Role != RoleSupportAgent && p.Role != RoleBillingSupervisor {
			return &OpError{
				Code:    ErrCodeUnauthorizedRole,
				Message: fmt.Sprintf("action %s restricted to support_agent or billing_supervisor, caller is %s", action, p.Role),
			}
		}
		return nil

	case ActionSupervisorApproveHighRefund,
		ActionModifyAccountTierOrLimit:
		if p.Role != RoleBillingSupervisor {
			return &OpError{
				Code:    ErrCodeUnauthorizedRole,
				Message: fmt.Sprintf("action %s restricted to billing_supervisor, caller is %s", action, p.Role),
			}
		}
		return nil

	case ActionAcquireLease, ActionReleaseLease:
		// Any authenticated principal with valid disjoint scope may request a lease
		// on a workflow scope permitted to its role (checked in lease validation).
		return nil

	default:
		return &OpError{
			Code:    ErrCodeInvalidArgument,
			Message: fmt.Sprintf("unrecognized action %q", action),
		}
	}
}

// AuthorizeLeaseScope checks whether a principal's role may claim a named workflow lease.
func AuthorizeLeaseScope(role Role, scope string) error {
	switch scope {
	case "order_settlement", "line_provisioning", "ticket_disposition":
		if role == RoleCustomer || role == RoleSupportAgent || role == RoleBillingSupervisor {
			return nil
		}
	case "account_governance":
		if role == RoleBillingSupervisor {
			return nil
		}
		return &OpError{
			Code:    ErrCodeUnauthorizedRole,
			Message: fmt.Sprintf("lease scope %q restricted to billing_supervisor", scope),
		}
	default:
		return &OpError{
			Code:    ErrCodeInvalidArgument,
			Message: fmt.Sprintf("unknown lease scope %q", scope),
		}
	}
	return &OpError{
		Code:    ErrCodeUnauthorizedRole,
		Message: fmt.Sprintf("role %q not permitted on lease scope %q", role, scope),
	}
}

// ValidateLeaseTarget ensures that a named lease scope matches its target entity collection.
func ValidateLeaseTarget(kind EntityKind, scope string) error {
	expected := map[string]EntityKind{
		"order_settlement":   EntityOrder,
		"line_provisioning":  EntityTelecomLine,
		"ticket_disposition": EntitySupportTicket,
		"account_governance": EntityAccount,
	}
	wantKind, ok := expected[scope]
	if !ok {
		return &OpError{
			Code:    ErrCodeInvalidArgument,
			Message: fmt.Sprintf("unknown lease scope %q", scope),
		}
	}
	if kind != wantKind {
		return &OpError{
			Code:    ErrCodeInvalidArgument,
			Message: fmt.Sprintf("lease scope %q requires target_kind=%s, got %s", scope, wantKind, kind),
		}
	}
	return nil
}

// Account holds customer identity, store-credit wallet, and pre-execution
// tenant budget state (HIP-1348 §1.1).
type Account struct {
	Org                string `json:"org"`
	ID                 string `json:"id"`
	Version            uint64 `json:"version"`
	CustomerID         string `json:"customer_id"`
	Tier               string `json:"tier"`
	CreditBalanceCents int64  `json:"credit_balance_cents"`
	TenantBudgetCents  int64  `json:"tenant_budget_cents"`
	VerifiedPIN        bool   `json:"verified_pin"`
	ContactEmail       string `json:"contact_email"`
}

func (a Account) Clone() Account { return a }

// Order represents a retail/commerce order shared between customer self-service
// controls and support/supervisor fulfillment and refund tools.
type Order struct {
	Org               string `json:"org"`
	ID                string `json:"id"`
	Version           uint64 `json:"version"`
	AccountID         string `json:"account_id"`
	Status            string `json:"status"`
	PaidCents         int64  `json:"paid_cents"`
	RefundedCents     int64  `json:"refunded_cents"`
	ShippingAddress   string `json:"shipping_address"`
	DeliveryNotes     string `json:"delivery_notes"`
	RestockedUnits    int    `json:"restocked_units"`
	SupervisorSignOff string `json:"supervisor_sign_off,omitempty"`
}

func (o Order) Clone() Order { return o }

// TelecomLine models the flagship tau2-Bench dual-control telecom domain on a
// unified shared record where handset toggles (customer) and carrier provisioning
// flags (agent) coexist on the same state entity.
type TelecomLine struct {
	Org                  string `json:"org"`
	ID                   string `json:"id"`
	Version              uint64 `json:"version"`
	AccountID            string `json:"account_id"`
	CustomerRoamingOptIn bool   `json:"customer_roaming_opt_in"`
	DeviceAPN            string `json:"device_apn"`
	DeviceRebootCount    int    `json:"device_reboot_count"`
	CarrierRoamingBarred bool   `json:"carrier_roaming_barred"`
	ActivePass           string `json:"active_pass"`
	PassChargeCents      int64  `json:"pass_charge_cents"`
}

func (l TelecomLine) Clone() TelecomLine { return l }

// SupportTicket models shared case escalation, SLA compensation, and customer
// resolution confirmation.
type SupportTicket struct {
	Org                         string `json:"org"`
	ID                          string `json:"id"`
	Version                     uint64 `json:"version"`
	AccountID                   string `json:"account_id"`
	Status                      string `json:"status"`
	Priority                    string `json:"priority"`
	SLACreditCents              int64  `json:"sla_credit_cents"`
	CustomerConfirmedResolution bool   `json:"customer_confirmed_resolution"`
	AgentResolutionSummary      string `json:"agent_resolution_summary"`
}

func (t SupportTicket) Clone() SupportTicket { return t }

// AuthorityLease models a bounded-TTL exclusive workflow claim on (Org, Scope, ResourceID),
// following the guarded lease semantics of clients/tasks/tasks.go.
type AuthorityLease struct {
	Org        string `json:"org"`
	Scope      string `json:"scope"`
	ResourceID string `json:"resource_id"`
	HolderID   string `json:"holder_id"`
	HolderRole Role   `json:"holder_role"`
	ExpiresAt  uint64 `json:"expires_at_step"`
}

// LedgerEntry is an append-only monetary credit/debit posting used to verify
// double-entry conservation against entity state at episode completion.
type LedgerEntry struct {
	Seq            uint64 `json:"seq"`
	Org            string `json:"org"`
	AccountID      string `json:"account_id"`
	ReferenceKind  string `json:"reference_kind"`
	ReferenceID    string `json:"reference_id"`
	DeltaCents     int64  `json:"delta_cents"`
	ActorID        string `json:"actor_id"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// TenantSnapshot captures the deterministic canonical view of one tenant's state.
type TenantSnapshot struct {
	Org      string          `json:"org"`
	Accounts []Account       `json:"accounts"`
	Orders   []Order         `json:"orders"`
	Lines    []TelecomLine   `json:"lines"`
	Tickets  []SupportTicket `json:"tickets"`
	Ledger   []LedgerEntry   `json:"ledger"`
}

// Digest computes a deterministic SHA-256 hex digest over the sorted tenant
// snapshot. Because Org is embedded first, two tenants with identical entity IDs
// still produce distinct salted digests (HIP-1348 §1.3 / LLM.md Invariant #2).
func (s TenantSnapshot) Digest() string {
	norm := TenantSnapshot{
		Org:      s.Org,
		Accounts: append([]Account(nil), s.Accounts...),
		Orders:   append([]Order(nil), s.Orders...),
		Lines:    append([]TelecomLine(nil), s.Lines...),
		Tickets:  append([]SupportTicket(nil), s.Tickets...),
		Ledger:   append([]LedgerEntry(nil), s.Ledger...),
	}
	sort.Slice(norm.Accounts, func(i, j int) bool { return norm.Accounts[i].ID < norm.Accounts[j].ID })
	sort.Slice(norm.Orders, func(i, j int) bool { return norm.Orders[i].ID < norm.Orders[j].ID })
	sort.Slice(norm.Lines, func(i, j int) bool { return norm.Lines[i].ID < norm.Lines[j].ID })
	sort.Slice(norm.Tickets, func(i, j int) bool { return norm.Tickets[i].ID < norm.Tickets[j].ID })
	sort.Slice(norm.Ledger, func(i, j int) bool { return norm.Ledger[i].Seq < norm.Ledger[j].Seq })

	raw, err := json.Marshal(norm)
	if err != nil {
		panic(fmt.Sprintf("tau2dualcontrol: marshal TenantSnapshot: %v", err))
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// RNG is a deterministic SplitMix64 pseudo-random generator used across all
// workload generators and fault injectors so runs are bit-exact across platforms
// and Go versions.
type RNG struct {
	state uint64
}

// NewRNG initializes a deterministic SplitMix64 generator from seed.
func NewRNG(seed uint64) *RNG {
	return &RNG{state: seed}
}

// Uint64 advances the SplitMix64 state and returns the next 64-bit value.
func (r *RNG) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// IntN returns a deterministic uniform integer in [0, n). Panics if n <= 0.
func (r *RNG) IntN(n int) int {
	if n <= 0 {
		panic("tau2dualcontrol: IntN called with n <= 0")
	}
	return int(r.Uint64() % uint64(n))
}

// Float64 returns a deterministic uniform float64 in [0.0, 1.0).
func (r *RNG) Float64() float64 {
	return float64(r.Uint64()>>11) / (1 << 53)
}

// Shuffle deterministically permutes n elements in place using Fisher-Yates.
func (r *RNG) Shuffle(n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		j := r.IntN(i + 1)
		swap(i, j)
	}
}
