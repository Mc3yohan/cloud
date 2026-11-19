// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0 OR MIT

package tau2dualcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrCodeLLMUnconfigured is returned (HTTP 503 equivalent) when an optional live
// Enso / OpenAI-compatible model endpoint is invoked without configuration
// (enforcing LLM.md Architecture Invariant #4: fail-closed external integrations).
const ErrCodeLLMUnconfigured ErrorCode = "llm_endpoint_unconfigured"

// OpenAICompatConfig configures the optional Enso / Zen / OpenAI-compatible
// chat-completions tool-calling adapter for live model evaluation.
type OpenAICompatConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	HTTPClient *http.Client
	MaxTurns   int
}

// Validate enforces fail-closed behavior when no model gateway is configured.
func (c OpenAICompatConfig) Validate() error {
	if strings.TrimSpace(c.BaseURL) == "" || strings.TrimSpace(c.Model) == "" {
		return &OpError{
			Code:    ErrCodeLLMUnconfigured,
			Layer:   "ai_gateway",
			Message: "503 Service Unavailable: Enso/OpenAI-compatible BaseURL and Model must be configured (fail-closed per LLM.md Invariant #4)",
		}
	}
	return nil
}

type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
}

type chatToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatFunctionCall `json:"function"`
}

type chatFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type toolPropertySpec struct {
	Type        string                      `json:"type"`
	Description string                      `json:"description,omitempty"`
	Enum        []string                    `json:"enum,omitempty"`
	Properties  map[string]toolPropertySpec `json:"properties,omitempty"`
}

type toolParametersSpec struct {
	Type       string                      `json:"type"`
	Required   []string                    `json:"required,omitempty"`
	Properties map[string]toolPropertySpec `json:"properties"`
}

type toolFunctionSpec struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Parameters  toolParametersSpec `json:"parameters"`
}

type toolSpec struct {
	Type     string           `json:"type"`
	Function toolFunctionSpec `json:"function"`
}

type chatCompletionRequest struct {
	Model       string        `json:"model"`
	Temperature float64       `json:"temperature"`
	Messages    []chatMessage `json:"messages"`
	Tools       []toolSpec    `json:"tools"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message      chatMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
	} `json:"choices"`
}

type toolMutationArgs struct {
	Action          ActionKind     `json:"action"`
	TargetKind      EntityKind     `json:"target_kind"`
	TargetID        string         `json:"target_id"`
	ExpectedVersion uint64         `json:"expected_version"`
	Params          MutationParams `json:"params"`
}

// OpenAIToolAgent drives an external OpenAI/Enso-compatible model over HTTP
// against SharedStateStore using structured tool calls and CAS conflict feedback.
type OpenAIToolAgent struct {
	cfg OpenAICompatConfig
}

// NewOpenAIToolAgent constructs a fail-closed HTTP tool-calling agent adapter.
func NewOpenAIToolAgent(cfg OpenAICompatConfig) (*OpenAIToolAgent, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if cfg.MaxTurns <= 0 {
		cfg.MaxTurns = 6
	}
	return &OpenAIToolAgent{cfg: cfg}, nil
}

// ExecuteTurnLoop runs bounded tool-calling turns against store for actor.
// Every tool mutation receives a deterministic per-turn idempotency_key
// (HIP-1347 §1.4) and structured version_conflict / lease_conflict feedback so
// the model can re-read state and retry safely.
func (a *OpenAIToolAgent) ExecuteTurnLoop(ctx context.Context, store *SharedStateStore, actor Principal, episodeID, taskPrompt string) (int, error) {
	if err := a.cfg.Validate(); err != nil {
		return 0, err
	}

	initObs, err := json.Marshal(store.SnapshotTenant(actor.Org))
	if err != nil {
		return 0, err
	}

	messages := []chatMessage{
		{
			Role:    "system",
			Content: "You are a Hanzo Cloud business operations agent operating in a dual-control shared-state environment. Always supply the latest expected_version before mutating an entity, and re-read state if version_conflict is returned.",
		},
		{
			Role:    "user",
			Content: fmt.Sprintf("Task: %s\nInitial Tenant Snapshot: %s", taskPrompt, string(initObs)),
		},
	}

	toolsSchema := []toolSpec{
		{
			Type: "function",
			Function: toolFunctionSpec{
				Name:        "read_tenant_state",
				Description: "Read fresh versioned entities for the caller's tenant org.",
				Parameters: toolParametersSpec{
					Type:       "object",
					Properties: map[string]toolPropertySpec{},
				},
			},
		},
		{
			Type: "function",
			Function: toolFunctionSpec{
				Name:        "apply_state_mutation",
				Description: "Submit a guarded state mutation with expected_version CAS.",
				Parameters: toolParametersSpec{
					Type:     "object",
					Required: []string{"action", "target_kind", "target_id", "expected_version"},
					Properties: map[string]toolPropertySpec{
						"action": {
							Type: "string",
							Enum: []string{
								string(ActionAgentIssueRefund),
								string(ActionAgentProvisionRoaming),
								string(ActionAgentEscalateTicket),
								string(ActionSupervisorApproveHighRefund),
								string(ActionAcquireLease),
								string(ActionReleaseLease),
							},
						},
						"target_kind": {
							Type: "string",
							Enum: []string{
								string(EntityAccount),
								string(EntityOrder),
								string(EntityTelecomLine),
								string(EntitySupportTicket),
							},
						},
						"target_id":        {Type: "string"},
						"expected_version": {Type: "integer"},
						"params": {
							Type: "object",
							Properties: map[string]toolPropertySpec{
								"refund_cents":              {Type: "integer"},
								"restocked_units":           {Type: "integer"},
								"clear_bar":                 {Type: "boolean"},
								"activate_pass":             {Type: "string"},
								"pass_cost_cents":           {Type: "integer"},
								"ticket_priority":           {Type: "string"},
								"ticket_status":             {Type: "string"},
								"resolution_summary":        {Type: "string"},
								"sla_credit_cents":          {Type: "integer"},
								"supervisor_approval_token": {Type: "string"},
								"lease_scope":               {Type: "string"},
								"lease_ttl_steps":           {Type: "integer"},
							},
						},
					},
				},
			},
		},
	}

	endpoint := strings.TrimRight(a.cfg.BaseURL, "/") + "/chat/completions"
	turns := 0

	for turn := 0; turn < a.cfg.MaxTurns; turn++ {
		turns++
		reqBody, err := json.Marshal(chatCompletionRequest{
			Model:       a.cfg.Model,
			Temperature: 0,
			Messages:    messages,
			Tools:       toolsSchema,
		})
		if err != nil {
			return turns, err
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
		if err != nil {
			return turns, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if a.cfg.APIKey != "" {
			httpReq.Header.Set("Authorization", "Bearer "+a.cfg.APIKey)
		}

		resp, err := a.cfg.HTTPClient.Do(httpReq)
		if err != nil {
			return turns, err
		}
		rawResp, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return turns, readErr
		}
		if resp.StatusCode != http.StatusOK {
			return turns, fmt.Errorf("llm endpoint HTTP %d: %s", resp.StatusCode, string(rawResp))
		}

		var parsed chatCompletionResponse
		if err := json.Unmarshal(rawResp, &parsed); err != nil {
			return turns, fmt.Errorf("decode chat completion: %w", err)
		}
		if len(parsed.Choices) == 0 {
			return turns, fmt.Errorf("llm endpoint returned zero choices")
		}

		msg := parsed.Choices[0].Message
		messages = append(messages, msg)
		if len(msg.ToolCalls) == 0 {
			return turns, nil
		}

		for callIdx, tc := range msg.ToolCalls {
			var toolOutput string
			switch tc.Function.Name {
			case "read_tenant_state":
				snapBytes, mErr := json.Marshal(store.SnapshotTenant(actor.Org))
				if mErr != nil {
					return turns, fmt.Errorf("marshal tenant snapshot: %w", mErr)
				}
				toolOutput = string(snapBytes)

			case "apply_state_mutation":
				var args toolMutationArgs
				if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
					toolOutput = fmt.Sprintf(`{"error":"invalid_json_arguments","detail":%q}`, err.Error())
					break
				}
				idemKey := fmt.Sprintf("%s:llm:%d:%d", episodeID, turn, callIdx)
				receipt, applyErr := store.Apply(MutationRequest{
					Actor:           actor,
					Action:          args.Action,
					TargetKind:      args.TargetKind,
					TargetID:        args.TargetID,
					ExpectedVersion: args.ExpectedVersion,
					IdempotencyKey:  idemKey,
					Params:          args.Params,
				})
				if applyErr != nil {
					errBytes, mErr := json.Marshal(applyErr)
					if mErr != nil {
						toolOutput = fmt.Sprintf(`{"error":%q}`, applyErr.Error())
					} else {
						toolOutput = string(errBytes)
					}
				} else {
					okBytes, mErr := json.Marshal(receipt)
					if mErr != nil {
						return turns, fmt.Errorf("marshal mutation receipt: %w", mErr)
					}
					toolOutput = string(okBytes)
				}

			default:
				toolOutput = fmt.Sprintf(`{"error":"unknown_tool","name":%q}`, tc.Function.Name)
			}

			messages = append(messages, chatMessage{
				Role:       "tool",
				ToolCallID: tc.ID,
				Content:    toolOutput,
			})
		}
		store.AdvanceStep(1)
	}

	return turns, nil
}
