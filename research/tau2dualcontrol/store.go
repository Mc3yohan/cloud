// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0 OR MIT

package tau2dualcontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
)

// MutationParams holds the deterministic, typed parameters for any shared-state
// operation. Keeping parameters in one explicit struct allows both live
// execution and deterministic audit-log replay without reflection or untyped maps.
type MutationParams struct {
	// Shipping updates (ActionUpdateShipping)
	ShippingAddress string `json:"shipping_address,omitempty"`
	DeliveryNotes   string `json:"delivery_notes,omitempty"`

	// Order refund / cancellation (ActionAgentIssueRefund, ActionCustomerSelfCancelOrder)
	RefundCents    int64 `json:"refund_cents,omitempty"`
	RestockedUnits int   `json:"restocked_units,omitempty"`

	// Telecom dual-control handset & carrier actions
	RoamingOptIn  *bool  `json:"roaming_opt_in,omitempty"`
	DeviceAPN     string `json:"device_apn,omitempty"`
	RebootDevice  bool   `json:"reboot_device,omitempty"`
	ClearBar      bool   `json:"clear_bar,omitempty"`
	ActivatePass  string `json:"activate_pass,omitempty"`
	PassCostCents int64  `json:"pass_cost_cents,omitempty"`

	// Ticket escalation & confirmation
	TicketPriority         string `json:"ticket_priority,omitempty"`
	TicketStatus           string `json:"ticket_status,omitempty"`
	ResolutionSummary      string `json:"resolution_summary,omitempty"`
	SLACreditCents         int64  `json:"sla_credit_cents,omitempty"`
	CustomerConfirmedClose bool   `json:"customer_confirmed_close,omitempty"`

	// Governance & supervisor actions
	SupervisorApprovalToken string `json:"supervisor_approval_token,omitempty"`
	NewTier                 string `json:"new_tier,omitempty"`
	BudgetDeltaCents        int64  `json:"budget_delta_cents,omitempty"`

	// Lease controls (ActionAcquireLease, ActionReleaseLease)
	LeaseScope    string `json:"lease_scope,omitempty"`
	LeaseTTLSteps uint64 `json:"lease_ttl_steps,omitempty"`

	// StaleReadSnapshot carries the caller's read-time copy of the target record
	// when modeling wholesale document replacement (clients/base/base.go UpdateIn).
	// Under ModeDualUncoordinated and ModeDualRBACLWW, non-target fields from a
	// stale snapshot overwrite concurrent edits made since the caller's read.
	OrderSnapshot  *Order         `json:"order_snapshot,omitempty"`
	LineSnapshot   *TelecomLine   `json:"line_snapshot,omitempty"`
	TicketSnapshot *SupportTicket `json:"ticket_snapshot,omitempty"`
}

// Clone returns a deep copy of MutationParams so caller pointer mutations
// never alias records stored inside SharedStateStore's immutable audit log.
func (p MutationParams) Clone() MutationParams {
	out := p
	if p.RoamingOptIn != nil {
		v := *p.RoamingOptIn
		out.RoamingOptIn = &v
	}
	if p.OrderSnapshot != nil {
		v := p.OrderSnapshot.Clone()
		out.OrderSnapshot = &v
	}
	if p.LineSnapshot != nil {
		v := p.LineSnapshot.Clone()
		out.LineSnapshot = &v
	}
	if p.TicketSnapshot != nil {
		v := p.TicketSnapshot.Clone()
		out.TicketSnapshot = &v
	}
	return out
}

// PayloadHash computes a canonical SHA-256 digest of the semantic intent of a
// mutation (excluding StaleReadSnapshot) for HIP-1347 §1.4 idempotency checks.
func (p MutationParams) PayloadHash(action ActionKind, targetKind EntityKind, targetID string) string {
	clean := p
	clean.OrderSnapshot = nil
	clean.LineSnapshot = nil
	clean.TicketSnapshot = nil
	envelope := struct {
		Action     ActionKind     `json:"action"`
		TargetKind EntityKind     `json:"target_kind"`
		TargetID   string         `json:"target_id"`
		Params     MutationParams `json:"params"`
	}{
		Action:     action,
		TargetKind: targetKind,
		TargetID:   targetID,
		Params:     clean,
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		panic(fmt.Sprintf("tau2dualcontrol: marshal idempotency envelope: %v", err))
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// MutationRequest is one atomic tool or user UI write submitted to SharedStateStore.
type MutationRequest struct {
	Actor           Principal      `json:"actor"`
	Action          ActionKind     `json:"action"`
	TargetKind      EntityKind     `json:"target_kind"`
	TargetID        string         `json:"target_id"`
	ExpectedVersion uint64         `json:"expected_version"`
	IdempotencyKey  string         `json:"idempotency_key,omitempty"`
	Params          MutationParams `json:"params"`
}

// MutationReceipt is returned on committed or idempotently replayed writes.
type MutationReceipt struct {
	Seq              uint64 `json:"seq"`
	NewVersion       uint64 `json:"new_version"`
	IdempotentReplay bool   `json:"idempotent_replay"`
	RecordHash       string `json:"record_hash"`
}

// InteractionRecord is one hash-chained, tamper-evident audit entry (modeled
// after audit/record.go in hanzoai/cloud). Both permitted and denied attempts
// are recorded inline under the store lock.
type InteractionRecord struct {
	Seq             uint64          `json:"seq"`
	Step            uint64          `json:"step"`
	Org             string          `json:"org"`
	ActorID         string          `json:"actor_id"`
	ActorRole       Role            `json:"actor_role"`
	CredentialScope CredentialScope `json:"credential_scope"`
	Action          ActionKind      `json:"action"`
	TargetKind      EntityKind      `json:"target_kind"`
	TargetID        string          `json:"target_id"`
	ExpectedVersion uint64          `json:"expected_version"`
	ResultVersion   uint64          `json:"result_version"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	Outcome         string          `json:"outcome"` // "applied" | "idempotent_replay" | "denied"
	ErrorCode       ErrorCode       `json:"error_code,omitempty"`
	Params          MutationParams  `json:"params"`
	PrevHash        string          `json:"prev_hash"`
	Hash            string          `json:"hash"`
}

// computeHash calculates the canonical SHA-256 chain digest over all fields of
// InteractionRecord except Hash itself.
func (r InteractionRecord) computeHash() string {
	unsigned := r
	unsigned.Hash = ""
	raw, err := json.Marshal(unsigned)
	if err != nil {
		panic(fmt.Sprintf("tau2dualcontrol: marshal InteractionRecord: %v", err))
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type orgEntityKey struct {
	org string
	id  string
}

type leaseKey struct {
	org        string
	scope      string
	resourceID string
}

type idempotencyKey struct {
	org     string
	actorID string
	key     string
}

type idempotencyEntry struct {
	payloadHash string
	receipt     MutationReceipt
}

// SharedStateStore is the concurrent, multi-tenant business state machine.
// All public methods acquire mu so the store is safe under both deterministic
// single-threaded schedules and overlapping goroutine race tests (-race).
type SharedStateStore struct {
	mu   sync.Mutex
	mode Mode
	step uint64

	accounts    map[orgEntityKey]Account
	orders      map[orgEntityKey]Order
	lines       map[orgEntityKey]TelecomLine
	tickets     map[orgEntityKey]SupportTicket
	leases      map[leaseKey]AuthorityLease
	idempotency map[idempotencyKey]idempotencyEntry

	ledgerSeq uint64
	ledger    []LedgerEntry

	auditHead string
	auditLog  []InteractionRecord
}

// NewSharedStateStore creates an empty store configured for the specified Mode.
func NewSharedStateStore(mode Mode) *SharedStateStore {
	return &SharedStateStore{
		mode:        mode,
		accounts:    make(map[orgEntityKey]Account),
		orders:      make(map[orgEntityKey]Order),
		lines:       make(map[orgEntityKey]TelecomLine),
		tickets:     make(map[orgEntityKey]SupportTicket),
		leases:      make(map[leaseKey]AuthorityLease),
		idempotency: make(map[idempotencyKey]idempotencyEntry),
	}
}

// LoadTenantSnapshot seeds a tenant's entities into the store prior to an episode.
func (s *SharedStateStore) LoadTenantSnapshot(snap TenantSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, a := range snap.Accounts {
		a.Org = snap.Org
		if a.Version == 0 {
			a.Version = 1
		}
		s.accounts[orgEntityKey{org: snap.Org, id: a.ID}] = a.Clone()
	}
	for _, o := range snap.Orders {
		o.Org = snap.Org
		if o.Version == 0 {
			o.Version = 1
		}
		s.orders[orgEntityKey{org: snap.Org, id: o.ID}] = o.Clone()
	}
	for _, l := range snap.Lines {
		l.Org = snap.Org
		if l.Version == 0 {
			l.Version = 1
		}
		s.lines[orgEntityKey{org: snap.Org, id: l.ID}] = l.Clone()
	}
	for _, t := range snap.Tickets {
		t.Org = snap.Org
		if t.Version == 0 {
			t.Version = 1
		}
		s.tickets[orgEntityKey{org: snap.Org, id: t.ID}] = t.Clone()
	}
	for _, le := range snap.Ledger {
		le.Org = snap.Org
		if le.Seq > s.ledgerSeq {
			s.ledgerSeq = le.Seq
		}
		s.ledger = append(s.ledger, le)
	}
}

// AdvanceStep increments the logical simulation clock, allowing expired leases
// to become reclaimable deterministically without wall-clock sleep flakiness.
func (s *SharedStateStore) AdvanceStep(delta uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.step += delta
	return s.step
}

// CurrentStep returns the current logical step of the store.
func (s *SharedStateStore) CurrentStep() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.step
}

// SnapshotTenant exports a deterministic copy of all records belonging to org.
func (s *SharedStateStore) SnapshotTenant(org string) TenantSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotTenantLocked(org)
}

func (s *SharedStateStore) snapshotTenantLocked(org string) TenantSnapshot {
	out := TenantSnapshot{Org: org}
	for k, v := range s.accounts {
		if k.org == org {
			out.Accounts = append(out.Accounts, v.Clone())
		}
	}
	for k, v := range s.orders {
		if k.org == org {
			out.Orders = append(out.Orders, v.Clone())
		}
	}
	for k, v := range s.lines {
		if k.org == org {
			out.Lines = append(out.Lines, v.Clone())
		}
	}
	for k, v := range s.tickets {
		if k.org == org {
			out.Tickets = append(out.Tickets, v.Clone())
		}
	}
	for _, le := range s.ledger {
		if le.Org == org {
			out.Ledger = append(out.Ledger, le)
		}
	}
	return out
}

// AuditLog returns a deep defensive copy of the hash-chained interaction log.
func (s *SharedStateStore) AuditLog() []InteractionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]InteractionRecord, len(s.auditLog))
	for i, rec := range s.auditLog {
		rec.Params = rec.Params.Clone()
		cp[i] = rec
	}
	return cp
}

func (s *SharedStateStore) checkCustomerOwnershipLocked(caller Principal, kind EntityKind, id string) error {
	if !s.enforceRBAC() || caller.Role != RoleCustomer {
		return nil
	}
	ownerCustID, ok := s.ownerCustomerIDLocked(caller.Org, kind, id)
	if !ok {
		return nil
	}
	if ownerCustID != caller.ID {
		return &OpError{
			Code:    ErrCodeForbiddenOwner,
			Message: fmt.Sprintf("customer %s cannot access %s %s owned by %s", caller.ID, kind, id, ownerCustID),
		}
	}
	return nil
}

func (s *SharedStateStore) ownerCustomerIDLocked(org string, kind EntityKind, id string) (string, bool) {
	key := orgEntityKey{org: org, id: id}
	switch kind {
	case EntityAccount:
		acc, ok := s.accounts[key]
		return acc.CustomerID, ok
	case EntityOrder:
		ord, ok := s.orders[key]
		if !ok {
			return "", false
		}
		acc, ok := s.accounts[orgEntityKey{org: org, id: ord.AccountID}]
		return acc.CustomerID, ok
	case EntityTelecomLine:
		line, ok := s.lines[key]
		if !ok {
			return "", false
		}
		acc, ok := s.accounts[orgEntityKey{org: org, id: line.AccountID}]
		return acc.CustomerID, ok
	case EntitySupportTicket:
		tkt, ok := s.tickets[key]
		if !ok {
			return "", false
		}
		acc, ok := s.accounts[orgEntityKey{org: org, id: tkt.AccountID}]
		return acc.CustomerID, ok
	default:
		return "", false
	}
}

// GetAccount reads an Account scoped to caller.Org. Attempts to read an ID that
// exists only in another tenant return ErrCodeCrossTenant (org_scope.go discipline).
func (s *SharedStateStore) GetAccount(caller Principal, id string) (Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if caller.Org == "" {
		return Account{}, &OpError{Code: ErrCodeUnauthorizedRole, Message: "empty caller org"}
	}
	if a, ok := s.accounts[orgEntityKey{org: caller.Org, id: id}]; ok {
		if err := s.checkCustomerOwnershipLocked(caller, EntityAccount, id); err != nil {
			return Account{}, err
		}
		return a.Clone(), nil
	}
	if s.existsInForeignOrgLocked(caller.Org, EntityAccount, id) {
		return Account{}, &OpError{Code: ErrCodeCrossTenant, Message: "account belongs to foreign tenant"}
	}
	return Account{}, &OpError{Code: ErrCodeNotFound, Message: "account not found"}
}

// GetOrder reads an Order scoped to caller.Org.
func (s *SharedStateStore) GetOrder(caller Principal, id string) (Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if caller.Org == "" {
		return Order{}, &OpError{Code: ErrCodeUnauthorizedRole, Message: "empty caller org"}
	}
	if o, ok := s.orders[orgEntityKey{org: caller.Org, id: id}]; ok {
		if err := s.checkCustomerOwnershipLocked(caller, EntityOrder, id); err != nil {
			return Order{}, err
		}
		return o.Clone(), nil
	}
	if s.existsInForeignOrgLocked(caller.Org, EntityOrder, id) {
		return Order{}, &OpError{Code: ErrCodeCrossTenant, Message: "order belongs to foreign tenant"}
	}
	return Order{}, &OpError{Code: ErrCodeNotFound, Message: "order not found"}
}

// GetTelecomLine reads a TelecomLine scoped to caller.Org.
func (s *SharedStateStore) GetTelecomLine(caller Principal, id string) (TelecomLine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if caller.Org == "" {
		return TelecomLine{}, &OpError{Code: ErrCodeUnauthorizedRole, Message: "empty caller org"}
	}
	if l, ok := s.lines[orgEntityKey{org: caller.Org, id: id}]; ok {
		if err := s.checkCustomerOwnershipLocked(caller, EntityTelecomLine, id); err != nil {
			return TelecomLine{}, err
		}
		return l.Clone(), nil
	}
	if s.existsInForeignOrgLocked(caller.Org, EntityTelecomLine, id) {
		return TelecomLine{}, &OpError{Code: ErrCodeCrossTenant, Message: "telecom line belongs to foreign tenant"}
	}
	return TelecomLine{}, &OpError{Code: ErrCodeNotFound, Message: "telecom line not found"}
}

// GetSupportTicket reads a SupportTicket scoped to caller.Org.
func (s *SharedStateStore) GetSupportTicket(caller Principal, id string) (SupportTicket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if caller.Org == "" {
		return SupportTicket{}, &OpError{Code: ErrCodeUnauthorizedRole, Message: "empty caller org"}
	}
	if t, ok := s.tickets[orgEntityKey{org: caller.Org, id: id}]; ok {
		if err := s.checkCustomerOwnershipLocked(caller, EntitySupportTicket, id); err != nil {
			return SupportTicket{}, err
		}
		return t.Clone(), nil
	}
	if s.existsInForeignOrgLocked(caller.Org, EntitySupportTicket, id) {
		return SupportTicket{}, &OpError{Code: ErrCodeCrossTenant, Message: "ticket belongs to foreign tenant"}
	}
	return SupportTicket{}, &OpError{Code: ErrCodeNotFound, Message: "ticket not found"}
}

func (s *SharedStateStore) existsInForeignOrgLocked(callerOrg string, kind EntityKind, id string) bool {
	switch kind {
	case EntityAccount:
		for k := range s.accounts {
			if k.id == id && k.org != callerOrg {
				return true
			}
		}
	case EntityOrder:
		for k := range s.orders {
			if k.id == id && k.org != callerOrg {
				return true
			}
		}
	case EntityTelecomLine:
		for k := range s.lines {
			if k.id == id && k.org != callerOrg {
				return true
			}
		}
	case EntitySupportTicket:
		for k := range s.tickets {
			if k.id == id && k.org != callerOrg {
				return true
			}
		}
	}
	return false
}

func (s *SharedStateStore) enforceRBAC() bool {
	return s.mode != ModeDualUncoordinated
}

func (s *SharedStateStore) enforceCASAndLease() bool {
	return s.mode == ModeDualCASLease || s.mode == ModeSingleControl
}

// Apply executes one MutationRequest atomically under the store lock and appends
// a hash-chained InteractionRecord regardless of allow/deny outcome.
func (s *SharedStateStore) Apply(req MutationRequest) (MutationReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	receipt, err := s.applyLocked(req)
	if err != nil {
		code := ErrCodeInvalidArgument
		if opErr, ok := err.(*OpError); ok {
			code = opErr.Code
		}
		s.appendAuditLocked(req, 0, "denied", code)
		return MutationReceipt{}, err
	}
	return receipt, nil
}

func (s *SharedStateStore) appendAuditLocked(req MutationRequest, resultVer uint64, outcome string, errCode ErrorCode) InteractionRecord {
	rec := InteractionRecord{
		Seq:             uint64(len(s.auditLog)),
		Step:            s.step,
		Org:             req.Actor.Org,
		ActorID:         req.Actor.ID,
		ActorRole:       req.Actor.Role,
		CredentialScope: req.Actor.CredentialScope,
		Action:          req.Action,
		TargetKind:      req.TargetKind,
		TargetID:        req.TargetID,
		ExpectedVersion: req.ExpectedVersion,
		ResultVersion:   resultVer,
		IdempotencyKey:  req.IdempotencyKey,
		Outcome:         outcome,
		ErrorCode:       errCode,
		Params:          req.Params.Clone(),
		PrevHash:        s.auditHead,
	}
	rec.Hash = rec.computeHash()
	s.auditHead = rec.Hash
	s.auditLog = append(s.auditLog, rec)
	return rec
}

func (s *SharedStateStore) postLedgerLocked(org, accountID, refKind, refID string, deltaCents int64, actorID, idemKey string) {
	if deltaCents == 0 {
		return
	}
	s.ledgerSeq++
	s.ledger = append(s.ledger, LedgerEntry{
		Seq:            s.ledgerSeq,
		Org:            org,
		AccountID:      accountID,
		ReferenceKind:  refKind,
		ReferenceID:    refID,
		DeltaCents:     deltaCents,
		ActorID:        actorID,
		IdempotencyKey: idemKey,
	})
}

func (s *SharedStateStore) checkForeignLeaseLocked(org, scope, resourceID, actorID string) error {
	if !s.enforceCASAndLease() || scope == "" {
		return nil
	}
	lk := leaseKey{org: org, scope: scope, resourceID: resourceID}
	lease, ok := s.leases[lk]
	if !ok {
		return nil
	}
	if lease.ExpiresAt <= s.step {
		delete(s.leases, lk)
		return nil
	}
	if lease.HolderID != actorID {
		return &OpError{
			Code:        ErrCodeLeaseHeld,
			LeaseHolder: lease.HolderID,
			Message:     fmt.Sprintf("resource %s:%s leased by %s until step %d", scope, resourceID, lease.HolderID, lease.ExpiresAt),
		}
	}
	return nil
}

func (s *SharedStateStore) applyLocked(req MutationRequest) (MutationReceipt, error) {
	// 1. Tenant boundary is mandatory in EVERY mode (LLM.md Architecture Invariant #2).
	if req.Actor.Org == "" || req.Actor.ID == "" {
		return MutationReceipt{}, &OpError{
			Code:    ErrCodeUnauthorizedRole,
			Message: "principal org and id required",
		}
	}

	// 2. State-machine RBAC & disjoint credential verification (HIP-1347 §1.1).
	if s.enforceRBAC() {
		if err := AuthorizeAction(req.Actor, req.Action); err != nil {
			return MutationReceipt{}, err
		}
	}

	// 3. Cross-tenant, existence, and intra-tenant customer ownership checks
	// apply uniformly across ALL operations (including lease acquire/release).
	curVer, exists := s.lookupVersionLocked(req.Actor.Org, req.TargetKind, req.TargetID)
	if !exists {
		if s.existsInForeignOrgLocked(req.Actor.Org, req.TargetKind, req.TargetID) {
			return MutationReceipt{}, &OpError{
				Code:    ErrCodeCrossTenant,
				Message: fmt.Sprintf("%s %s belongs to another org", req.TargetKind, req.TargetID),
			}
		}
		return MutationReceipt{}, &OpError{
			Code:    ErrCodeNotFound,
			Message: fmt.Sprintf("%s %s not found in org %s", req.TargetKind, req.TargetID, req.Actor.Org),
		}
	}
	if err := s.checkCustomerOwnershipLocked(req.Actor, req.TargetKind, req.TargetID); err != nil {
		return MutationReceipt{}, err
	}

	isLeaseOp := req.Action == ActionAcquireLease || req.Action == ActionReleaseLease

	// 4. Per-attempt idempotency deduplication (HIP-1347 §1.4) for state mutations.
	// Ephemeral step-clock lease claims are not cached in idempotency so expired
	// leases transferred to another principal are never replayed as false holds.
	var payloadHash string
	if s.enforceCASAndLease() && !isLeaseOp && req.IdempotencyKey != "" {
		payloadHash = req.Params.PayloadHash(req.Action, req.TargetKind, req.TargetID)
		ik := idempotencyKey{org: req.Actor.Org, actorID: req.Actor.ID, key: req.IdempotencyKey}
		if prev, exists := s.idempotency[ik]; exists {
			if prev.payloadHash != payloadHash {
				return MutationReceipt{}, &OpError{
					Code:    ErrCodeIdempotencyMismatch,
					Message: fmt.Sprintf("idempotency_key %q reused with different mutation parameters", req.IdempotencyKey),
				}
			}
			rec := s.appendAuditLocked(req, prev.receipt.NewVersion, "idempotent_replay", "")
			replayReceipt := prev.receipt
			replayReceipt.Seq = rec.Seq
			replayReceipt.IdempotentReplay = true
			replayReceipt.RecordHash = rec.Hash
			return replayReceipt, nil
		}
	}

	// 5. Handle explicit lease acquisition and release (clients/tasks-style claims).
	if req.Action == ActionAcquireLease {
		if err := ValidateLeaseTarget(req.TargetKind, req.Params.LeaseScope); err != nil {
			return MutationReceipt{}, err
		}
		if s.enforceRBAC() {
			if err := AuthorizeLeaseScope(req.Actor.Role, req.Params.LeaseScope); err != nil {
				return MutationReceipt{}, err
			}
		}
		if err := s.checkForeignLeaseLocked(req.Actor.Org, req.Params.LeaseScope, req.TargetID, req.Actor.ID); err != nil {
			return MutationReceipt{}, err
		}
		ttl := req.Params.LeaseTTLSteps
		if ttl == 0 {
			ttl = DefaultLeaseTTLSteps
		}
		if s.enforceCASAndLease() {
			lk := leaseKey{org: req.Actor.Org, scope: req.Params.LeaseScope, resourceID: req.TargetID}
			s.leases[lk] = AuthorityLease{
				Org:        req.Actor.Org,
				Scope:      req.Params.LeaseScope,
				ResourceID: req.TargetID,
				HolderID:   req.Actor.ID,
				HolderRole: req.Actor.Role,
				ExpiresAt:  s.step + ttl,
			}
		}
		return s.commitReceiptLocked(req, "", curVer), nil
	}

	if req.Action == ActionReleaseLease {
		if err := ValidateLeaseTarget(req.TargetKind, req.Params.LeaseScope); err != nil {
			return MutationReceipt{}, err
		}
		if s.enforceRBAC() {
			if err := AuthorizeLeaseScope(req.Actor.Role, req.Params.LeaseScope); err != nil {
				return MutationReceipt{}, err
			}
		}
		lk := leaseKey{org: req.Actor.Org, scope: req.Params.LeaseScope, resourceID: req.TargetID}
		if s.enforceCASAndLease() {
			if cur, ok := s.leases[lk]; ok && cur.ExpiresAt > s.step {
				if cur.HolderID != req.Actor.ID {
					return MutationReceipt{}, &OpError{
						Code:        ErrCodeLeaseNotOwned,
						LeaseHolder: cur.HolderID,
						Message:     fmt.Sprintf("cannot release lease owned by %s", cur.HolderID),
					}
				}
			}
			delete(s.leases, lk)
		}
		return s.commitReceiptLocked(req, "", curVer), nil
	}

	// 6. Optimistic CAS version check prior to mutating state.
	if s.enforceCASAndLease() && req.ExpectedVersion != curVer {
		return MutationReceipt{}, &OpError{
			Code:           ErrCodeVersionConflict,
			CurrentVersion: curVer,
			Message:        fmt.Sprintf("stale expected_version=%d on %s %s (current=%d)", req.ExpectedVersion, req.TargetKind, req.TargetID, curVer),
		}
	}

	// 7. Execute domain transition.
	newVer, err := s.executeTransitionLocked(req)
	if err != nil {
		return MutationReceipt{}, err
	}
	return s.commitReceiptLocked(req, payloadHash, newVer), nil
}

func (s *SharedStateStore) lookupVersionLocked(org string, kind EntityKind, id string) (uint64, bool) {
	key := orgEntityKey{org: org, id: id}
	switch kind {
	case EntityAccount:
		v, ok := s.accounts[key]
		return v.Version, ok
	case EntityOrder:
		v, ok := s.orders[key]
		return v.Version, ok
	case EntityTelecomLine:
		v, ok := s.lines[key]
		return v.Version, ok
	case EntitySupportTicket:
		v, ok := s.tickets[key]
		return v.Version, ok
	default:
		return 0, false
	}
}

func (s *SharedStateStore) commitReceiptLocked(req MutationRequest, payloadHash string, newVer uint64) MutationReceipt {
	rec := s.appendAuditLocked(req, newVer, "applied", "")
	receipt := MutationReceipt{
		Seq:              rec.Seq,
		NewVersion:       newVer,
		IdempotentReplay: false,
		RecordHash:       rec.Hash,
	}
	if s.enforceCASAndLease() && req.IdempotencyKey != "" {
		ik := idempotencyKey{org: req.Actor.Org, actorID: req.Actor.ID, key: req.IdempotencyKey}
		s.idempotency[ik] = idempotencyEntry{
			payloadHash: payloadHash,
			receipt:     receipt,
		}
	}
	return receipt
}

func (s *SharedStateStore) executeTransitionLocked(req MutationRequest) (uint64, error) {
	org := req.Actor.Org
	key := orgEntityKey{org: org, id: req.TargetID}

	switch req.Action {
	case ActionVerifyPIN:
		if err := s.checkForeignLeaseLocked(org, "account_governance", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		acc := s.accounts[key]
		acc.VerifiedPIN = true
		acc.Version++
		s.accounts[key] = acc
		return acc.Version, nil

	case ActionModifyAccountTierOrLimit:
		if err := s.checkForeignLeaseLocked(org, "account_governance", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		acc := s.accounts[key]
		if s.enforceRBAC() && acc.TenantBudgetCents+req.Params.BudgetDeltaCents < 0 {
			return 0, &OpError{
				Code:    ErrCodeInvalidArgument,
				Message: "resulting tenant_budget_cents cannot be negative",
			}
		}
		if req.Params.NewTier != "" {
			acc.Tier = req.Params.NewTier
		}
		acc.TenantBudgetCents += req.Params.BudgetDeltaCents
		acc.Version++
		s.accounts[key] = acc
		if req.Params.BudgetDeltaCents != 0 {
			s.postLedgerLocked(org, acc.ID, "supervisor_budget_adjustment", acc.ID, req.Params.BudgetDeltaCents, req.Actor.ID, req.IdempotencyKey)
		}
		return acc.Version, nil

	case ActionUpdateShipping:
		live := s.orders[key]
		// Under wholesale LWW (clients/base PUT), writing back a modified stale
		// OrderSnapshot replaces non-shipping fields with stale read-time values.
		work := live
		if !s.enforceCASAndLease() && req.Params.OrderSnapshot != nil {
			work = *req.Params.OrderSnapshot
			work.Org = org
			work.ID = live.ID
		}
		if s.enforceCASAndLease() && (live.Status == "cancelled" || live.Status == "refunded") {
			return 0, &OpError{
				Code:    ErrCodePreconditionFailed,
				Message: fmt.Sprintf("cannot update shipping on %s order", live.Status),
			}
		}
		work.ShippingAddress = req.Params.ShippingAddress
		work.DeliveryNotes = req.Params.DeliveryNotes
		work.Version = live.Version + 1
		s.orders[key] = work
		return work.Version, nil

	case ActionSupervisorApproveHighRefund:
		if err := s.checkForeignLeaseLocked(org, "order_settlement", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		ord := s.orders[key]
		if req.Params.SupervisorApprovalToken == "" {
			return 0, &OpError{Code: ErrCodeInvalidArgument, Message: "supervisor approval token required"}
		}
		ord.SupervisorSignOff = req.Params.SupervisorApprovalToken
		ord.Version++
		s.orders[key] = ord
		return ord.Version, nil

	case ActionCustomerSelfCancelOrder:
		if err := s.checkForeignLeaseLocked(org, "order_settlement", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		live := s.orders[key]
		work := live
		if !s.enforceCASAndLease() && req.Params.OrderSnapshot != nil {
			work = *req.Params.OrderSnapshot
			work.Org = org
			work.ID = live.ID
		}
		accKey := orgEntityKey{org: org, id: live.AccountID}
		acc, ok := s.accounts[accKey]
		if !ok {
			return 0, &OpError{Code: ErrCodeNotFound, Message: "linked account not found"}
		}
		if err := s.checkForeignLeaseLocked(org, "account_governance", acc.ID, req.Actor.ID); err != nil {
			return 0, err
		}

		if s.enforceCASAndLease() {
			if live.Status == "cancelled" || live.Status == "refunded" {
				return 0, &OpError{
					Code:    ErrCodePreconditionFailed,
					Message: fmt.Sprintf("order %s already settled (%s)", live.ID, live.Status),
				}
			}
		}

		refundable := work.PaidCents - work.RefundedCents
		if refundable < 0 {
			refundable = 0
		}
		if s.enforceCASAndLease() && live.RefundedCents+refundable > live.PaidCents {
			return 0, &OpError{
				Code:    ErrCodePreconditionFailed,
				Message: "refund would exceed paid order total",
			}
		}

		work.Status = "cancelled"
		work.RefundedCents += refundable
		work.Version = live.Version + 1
		s.orders[key] = work

		if refundable > 0 {
			acc.CreditBalanceCents += refundable
			acc.Version++
			s.accounts[accKey] = acc
			s.postLedgerLocked(org, acc.ID, "order_self_cancel_refund", live.ID, refundable, req.Actor.ID, req.IdempotencyKey)
		}
		return work.Version, nil

	case ActionAgentIssueRefund:
		if err := s.checkForeignLeaseLocked(org, "order_settlement", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		if req.Params.RefundCents <= 0 {
			return 0, &OpError{Code: ErrCodeInvalidArgument, Message: "refund_cents must be positive"}
		}
		if req.Params.RestockedUnits < 0 {
			return 0, &OpError{Code: ErrCodeInvalidArgument, Message: "restocked_units cannot be negative"}
		}
		live := s.orders[key]
		work := live
		if !s.enforceCASAndLease() && req.Params.OrderSnapshot != nil {
			work = *req.Params.OrderSnapshot
			work.Org = org
			work.ID = live.ID
		}
		accKey := orgEntityKey{org: org, id: live.AccountID}
		acc, ok := s.accounts[accKey]
		if !ok {
			return 0, &OpError{Code: ErrCodeNotFound, Message: "linked account not found"}
		}
		if err := s.checkForeignLeaseLocked(org, "account_governance", acc.ID, req.Actor.ID); err != nil {
			return 0, err
		}

		proposedTotal := work.RefundedCents + req.Params.RefundCents
		if s.enforceRBAC() && proposedTotal > SupervisorThresholdCents && req.Actor.Role != RoleBillingSupervisor {
			signOff := live.SupervisorSignOff
			if !s.enforceCASAndLease() && req.Params.OrderSnapshot != nil && signOff == "" {
				signOff = req.Params.OrderSnapshot.SupervisorSignOff
			}
			if signOff == "" {
				return 0, &OpError{
					Code:    ErrCodeSupervisorRequired,
					Message: fmt.Sprintf("cumulative refund %d cents exceeds %d threshold without supervisor sign-off", proposedTotal, SupervisorThresholdCents),
				}
			}
		}

		if s.enforceCASAndLease() {
			if live.Status == "cancelled" || live.Status == "refunded" {
				return 0, &OpError{
					Code:    ErrCodePreconditionFailed,
					Message: fmt.Sprintf("cannot refund order %s in status %s", live.ID, live.Status),
				}
			}
			if live.RefundedCents+req.Params.RefundCents > live.PaidCents {
				return 0, &OpError{
					Code:    ErrCodePreconditionFailed,
					Message: fmt.Sprintf("refund %d + existing %d exceeds paid %d", req.Params.RefundCents, live.RefundedCents, live.PaidCents),
				}
			}
		}

		work.RefundedCents += req.Params.RefundCents
		work.RestockedUnits += req.Params.RestockedUnits
		if work.RefundedCents >= work.PaidCents {
			work.Status = "refunded"
		} else {
			work.Status = "partially_refunded"
		}
		work.Version = live.Version + 1
		s.orders[key] = work

		acc.CreditBalanceCents += req.Params.RefundCents
		acc.Version++
		s.accounts[accKey] = acc
		s.postLedgerLocked(org, acc.ID, "order_agent_refund", live.ID, req.Params.RefundCents, req.Actor.ID, req.IdempotencyKey)
		return work.Version, nil

	case ActionToggleDeviceRoaming:
		if err := s.checkForeignLeaseLocked(org, "line_provisioning", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		live := s.lines[key]
		work := live
		if !s.enforceCASAndLease() && req.Params.LineSnapshot != nil {
			work = *req.Params.LineSnapshot
			work.Org = org
			work.ID = live.ID
		}
		if req.Params.RoamingOptIn != nil {
			work.CustomerRoamingOptIn = *req.Params.RoamingOptIn
		}
		if req.Params.DeviceAPN != "" {
			work.DeviceAPN = req.Params.DeviceAPN
		}
		if req.Params.RebootDevice {
			work.DeviceRebootCount++
		}
		work.Version = live.Version + 1
		s.lines[key] = work
		return work.Version, nil

	case ActionAgentProvisionRoaming:
		if err := s.checkForeignLeaseLocked(org, "line_provisioning", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		live := s.lines[key]
		work := live
		if !s.enforceCASAndLease() && req.Params.LineSnapshot != nil {
			work = *req.Params.LineSnapshot
			work.Org = org
			work.ID = live.ID
		}
		accKey := orgEntityKey{org: org, id: live.AccountID}
		acc, ok := s.accounts[accKey]
		if !ok {
			return 0, &OpError{Code: ErrCodeNotFound, Message: "linked account not found"}
		}
		if err := s.checkForeignLeaseLocked(org, "account_governance", acc.ID, req.Actor.ID); err != nil {
			return 0, err
		}

		if req.Params.PassCostCents < 0 {
			return 0, &OpError{Code: ErrCodeInvalidArgument, Message: "pass_cost_cents cannot be negative"}
		}

		// HIP-1348 §1.1 & §1.2: Tenant ledger budget MUST be verified before
		// provisioning paid carrier pass resources.
		if s.enforceRBAC() && req.Params.PassCostCents > 0 {
			if acc.TenantBudgetCents < req.Params.PassCostCents {
				return 0, NewBudgetExhaustedError(
					"tenant_ledger",
					fmt.Sprintf("tenant budget %d cents insufficient for pass cost %d cents", acc.TenantBudgetCents, req.Params.PassCostCents),
				)
			}
		}

		// Dual-control coordination invariant: carrier roaming pass activation
		// requires verified customer PIN and explicit handset roaming opt-in.
		if s.enforceCASAndLease() {
			if !acc.VerifiedPIN {
				return 0, &OpError{
					Code:    ErrCodePreconditionFailed,
					Message: "customer PIN must be verified before carrier roaming activation",
				}
			}
			if !live.CustomerRoamingOptIn {
				return 0, &OpError{
					Code:    ErrCodePreconditionFailed,
					Message: "customer device roaming opt-in required before carrier pass activation",
				}
			}
		}

		if req.Params.ClearBar {
			work.CarrierRoamingBarred = false
		}
		if req.Params.ActivatePass != "" {
			work.ActivePass = req.Params.ActivatePass
			work.PassChargeCents += req.Params.PassCostCents
		}
		work.Version = live.Version + 1
		s.lines[key] = work

		if req.Params.PassCostCents > 0 {
			acc.TenantBudgetCents -= req.Params.PassCostCents
			acc.Version++
			s.accounts[accKey] = acc
			s.postLedgerLocked(org, acc.ID, "telecom_pass_debit", live.ID, -req.Params.PassCostCents, req.Actor.ID, req.IdempotencyKey)
		}
		return work.Version, nil

	case ActionAgentEscalateTicket:
		if err := s.checkForeignLeaseLocked(org, "ticket_disposition", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		if req.Params.SLACreditCents < 0 {
			return 0, &OpError{Code: ErrCodeInvalidArgument, Message: "sla_credit_cents cannot be negative"}
		}
		live := s.tickets[key]
		work := live
		if !s.enforceCASAndLease() && req.Params.TicketSnapshot != nil {
			work = *req.Params.TicketSnapshot
			work.Org = org
			work.ID = live.ID
		}
		proposedSLA := work.SLACreditCents + req.Params.SLACreditCents
		if s.enforceCASAndLease() && live.SLACreditCents+req.Params.SLACreditCents > proposedSLA {
			proposedSLA = live.SLACreditCents + req.Params.SLACreditCents
		}
		if s.enforceRBAC() && proposedSLA > MaxSLACreditCents && req.Actor.Role != RoleBillingSupervisor {
			return 0, &OpError{
				Code:    ErrCodeSupervisorRequired,
				Message: fmt.Sprintf("cumulative SLA credit %d exceeds support_agent cap %d", proposedSLA, MaxSLACreditCents),
			}
		}
		accKey := orgEntityKey{org: org, id: live.AccountID}
		acc, ok := s.accounts[accKey]
		if !ok {
			return 0, &OpError{Code: ErrCodeNotFound, Message: "linked account not found"}
		}
		if err := s.checkForeignLeaseLocked(org, "account_governance", acc.ID, req.Actor.ID); err != nil {
			return 0, err
		}

		if req.Params.TicketPriority != "" {
			work.Priority = req.Params.TicketPriority
		}
		if req.Params.TicketStatus != "" {
			work.Status = req.Params.TicketStatus
		}
		if req.Params.ResolutionSummary != "" {
			work.AgentResolutionSummary = req.Params.ResolutionSummary
		}
		if req.Params.SLACreditCents > 0 {
			work.SLACreditCents += req.Params.SLACreditCents
			acc.CreditBalanceCents += req.Params.SLACreditCents
			acc.Version++
			s.accounts[accKey] = acc
			s.postLedgerLocked(org, acc.ID, "ticket_sla_credit", live.ID, req.Params.SLACreditCents, req.Actor.ID, req.IdempotencyKey)
		}
		work.Version = live.Version + 1
		s.tickets[key] = work
		return work.Version, nil

	case ActionCustomerConfirmTicket:
		if err := s.checkForeignLeaseLocked(org, "ticket_disposition", req.TargetID, req.Actor.ID); err != nil {
			return 0, err
		}
		live := s.tickets[key]
		work := live
		if !s.enforceCASAndLease() && req.Params.TicketSnapshot != nil {
			work = *req.Params.TicketSnapshot
			work.Org = org
			work.ID = live.ID
		}
		if s.enforceCASAndLease() && live.AgentResolutionSummary == "" {
			return 0, &OpError{
				Code:    ErrCodePreconditionFailed,
				Message: "customer cannot close ticket before agent records resolution summary",
			}
		}
		work.CustomerConfirmedResolution = req.Params.CustomerConfirmedClose
		if req.Params.CustomerConfirmedClose {
			work.Status = "closed_by_customer"
		}
		work.Version = live.Version + 1
		s.tickets[key] = work
		return work.Version, nil

	default:
		return 0, &OpError{Code: ErrCodeInvalidArgument, Message: fmt.Sprintf("unsupported action %q", req.Action)}
	}
}

// VerifyAuditChain validates contiguous Seq numbering, PrevHash links, and
// per-record canonical SHA-256 digests across an InteractionRecord slice.
func VerifyAuditChain(log []InteractionRecord) error {
	prev := ""
	for i, rec := range log {
		if rec.Seq != uint64(i) {
			return &OpError{
				Code:    ErrCodeTamperedLog,
				Message: fmt.Sprintf("audit sequence gap or reorder at index %d: got seq=%d", i, rec.Seq),
			}
		}
		if rec.PrevHash != prev {
			return &OpError{
				Code:    ErrCodeTamperedLog,
				Message: fmt.Sprintf("broken prev_hash link at seq=%d", rec.Seq),
			}
		}
		expected := rec.computeHash()
		if rec.Hash != expected {
			return &OpError{
				Code:    ErrCodeTamperedLog,
				Message: fmt.Sprintf("hash mismatch at seq=%d: tampered field detected", rec.Seq),
			}
		}
		prev = rec.Hash
	}
	return nil
}

// ReplayAndVerify validates the hash chain of log, deterministically replays
// every logged interaction from initial on a fresh SharedStateStore under mode,
// and verifies that both per-step outcomes and the final tenant state digest
// match expectedFinalDigest bit-for-bit.
func ReplayAndVerify(initial TenantSnapshot, mode Mode, log []InteractionRecord, expectedFinalDigest string) error {
	return ReplayAndVerifyMulti(
		[]TenantSnapshot{initial},
		mode,
		log,
		map[string]string{initial.Org: expectedFinalDigest},
	)
}

// ReplayAndVerifyMulti generalizes ReplayAndVerify to multi-tenant stores where
// interactions across multiple orgs share one hash-chained audit trail.
func ReplayAndVerifyMulti(initials []TenantSnapshot, mode Mode, log []InteractionRecord, expectedFinalDigests map[string]string) error {
	if err := VerifyAuditChain(log); err != nil {
		return err
	}
	replay := NewSharedStateStore(mode)
	for _, snap := range initials {
		replay.LoadTenantSnapshot(snap)
	}

	for _, rec := range log {
		if rec.Step < replay.CurrentStep() {
			return &OpError{
				Code:    ErrCodeTamperedLog,
				Message: fmt.Sprintf("non-monotonic step clock at seq=%d (%d < %d)", rec.Seq, rec.Step, replay.CurrentStep()),
			}
		}
		if delta := rec.Step - replay.CurrentStep(); delta > 0 {
			replay.AdvanceStep(delta)
		}

		receipt, err := replay.Apply(MutationRequest{
			Actor: Principal{
				Org:             rec.Org,
				ID:              rec.ActorID,
				Role:            rec.ActorRole,
				CredentialScope: rec.CredentialScope,
			},
			Action:          rec.Action,
			TargetKind:      rec.TargetKind,
			TargetID:        rec.TargetID,
			ExpectedVersion: rec.ExpectedVersion,
			IdempotencyKey:  rec.IdempotencyKey,
			Params:          rec.Params.Clone(),
		})

		switch rec.Outcome {
		case "denied":
			if err == nil {
				return &OpError{
					Code:    ErrCodeTamperedLog,
					Message: fmt.Sprintf("replay divergence at seq=%d: expected denied(%s), got success", rec.Seq, rec.ErrorCode),
				}
			}
			if !IsErrorCode(err, rec.ErrorCode) {
				return &OpError{
					Code:    ErrCodeTamperedLog,
					Message: fmt.Sprintf("replay error-code divergence at seq=%d: expected %s, got %v", rec.Seq, rec.ErrorCode, err),
				}
			}
		case "applied":
			if err != nil || receipt.IdempotentReplay {
				return &OpError{
					Code:    ErrCodeTamperedLog,
					Message: fmt.Sprintf("replay divergence at seq=%d: expected applied, got replay=%v err=%v", rec.Seq, receipt.IdempotentReplay, err),
				}
			}
			if receipt.NewVersion != rec.ResultVersion || receipt.RecordHash != rec.Hash {
				return &OpError{
					Code:    ErrCodeTamperedLog,
					Message: fmt.Sprintf("replay version/hash divergence at seq=%d: got ver=%d hash=%s, want ver=%d hash=%s", rec.Seq, receipt.NewVersion, receipt.RecordHash, rec.ResultVersion, rec.Hash),
				}
			}
		case "idempotent_replay":
			if err != nil || !receipt.IdempotentReplay {
				return &OpError{
					Code:    ErrCodeTamperedLog,
					Message: fmt.Sprintf("replay divergence at seq=%d: expected idempotent_replay, got err=%v", rec.Seq, err),
				}
			}
			if receipt.NewVersion != rec.ResultVersion || receipt.RecordHash != rec.Hash {
				return &OpError{
					Code:    ErrCodeTamperedLog,
					Message: fmt.Sprintf("replay idempotent version/hash divergence at seq=%d", rec.Seq),
				}
			}
		default:
			return &OpError{
				Code:    ErrCodeTamperedLog,
				Message: fmt.Sprintf("unknown audit outcome %q at seq=%d", rec.Outcome, rec.Seq),
			}
		}
	}

	for org, wantDigest := range expectedFinalDigests {
		gotDigest := replay.SnapshotTenant(org).Digest()
		if gotDigest != wantDigest {
			return &OpError{
				Code:    ErrCodeTamperedLog,
				Message: fmt.Sprintf("final state digest mismatch for org %s after replay: got %s, want %s", org, gotDigest, wantDigest),
			}
		}
	}
	return nil
}
