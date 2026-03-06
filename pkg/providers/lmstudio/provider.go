package lmstudio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/providers/openai_compat"
	"github.com/sipeed/picoclaw/pkg/providers/protocoltypes"
)

type (
	ToolCall       = protocoltypes.ToolCall
	FunctionCall   = protocoltypes.FunctionCall
	LLMResponse    = protocoltypes.LLMResponse
	UsageInfo      = protocoltypes.UsageInfo
	Message        = protocoltypes.Message
	ToolDefinition = protocoltypes.ToolDefinition
)

// Provider implements LLMProvider using LM Studio's REST API with stateful
// chats for tool-free calls, falling back to the OpenAI-compatible endpoint
// for calls that include tool definitions.
//
// Stateful chats avoid resending the full conversation history by chaining
// requests via response_id / previous_response_id. The KV cache is kept
// warm server-side between calls in the same chain.
type Provider struct {
	apiBase    string
	httpClient *http.Client

	// Fallback for tool-using calls (OpenAI-compatible /v1/chat/completions).
	compat *openai_compat.Provider

	mu       sync.Mutex
	sessions map[string]*session
}

type session struct {
	responseID   string
	messagesSent int // number of picoclaw messages covered by the server-side state
}

const defaultTimeout = 120 * time.Second

func NewProvider(apiKey, apiBase, proxy string) *Provider {
	base := strings.TrimRight(apiBase, "/")
	if base == "" {
		base = "http://localhost:1234"
	}

	// The compat delegate needs the /v1 suffix for /v1/chat/completions.
	compatBase := base + "/v1"
	if strings.HasSuffix(base, "/v1") {
		compatBase = base
		base = strings.TrimSuffix(base, "/v1")
	}

	return &Provider{
		apiBase: base,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		compat:   openai_compat.NewProvider(apiKey, compatBase, proxy),
		sessions: make(map[string]*session),
	}
}

func (p *Provider) GetDefaultModel() string { return "" }

func (p *Provider) Chat(
	ctx context.Context,
	messages []Message,
	tools []ToolDefinition,
	model string,
	options map[string]any,
) (*LLMResponse, error) {
	// Tool-using calls must go through OpenAI-compat because the LM Studio
	// REST API doesn't accept client-side tool definitions.
	if len(tools) > 0 {
		return p.compat.Chat(ctx, sanitizeToolMessages(messages), tools, model, options)
	}

	cacheKey, _ := options["prompt_cache_key"].(string)
	if cacheKey == "" {
		return p.compat.Chat(ctx, sanitizeToolMessages(messages), tools, model, options)
	}

	p.mu.Lock()
	sess := p.sessions[cacheKey]
	p.mu.Unlock()

	if sess != nil && sess.responseID != "" {
		return p.tryContinueStateful(ctx, sess, messages, model, cacheKey, options)
	}
	return p.tryNewStateful(ctx, messages, model, cacheKey, options)
}

// tryContinueStateful attempts to continue an existing stateful session by
// sending only the new user message. Falls back to OpenAI-compat on failure.
func (p *Provider) tryContinueStateful(
	ctx context.Context,
	sess *session,
	messages []Message,
	model, cacheKey string,
	options map[string]any,
) (*LLMResponse, error) {
	if len(messages) <= sess.messagesSent {
		return p.chatCompat(ctx, messages, model, cacheKey, options)
	}

	newMsgs := messages[sess.messagesSent:]
	if len(newMsgs) != 1 || newMsgs[0].Role != "user" {
		// Not a simple user-message continuation; reset and fallback.
		p.invalidateSession(cacheKey)
		return p.chatCompat(ctx, messages, model, cacheKey, options)
	}

	resp, respID, err := p.doStatefulChat(ctx, newMsgs[0].Content, model, sess.responseID, "", options)
	if err != nil {
		log.Printf("lmstudio: stateful continue failed, falling back: %v", err)
		p.invalidateSession(cacheKey)
		return p.chatCompat(ctx, messages, model, cacheKey, options)
	}

	p.mu.Lock()
	sess.responseID = respID
	sess.messagesSent = len(messages) + 1 // +1 accounts for the assistant response
	p.mu.Unlock()

	return resp, nil
}

// tryNewStateful starts a fresh stateful session. Only possible when the first
// message is system and the last is user, with no history in between (brand new
// conversation).
func (p *Provider) tryNewStateful(
	ctx context.Context,
	messages []Message,
	model, cacheKey string,
	options map[string]any,
) (*LLMResponse, error) {
	if len(messages) < 1 {
		return p.chatCompat(ctx, messages, model, cacheKey, options)
	}

	last := messages[len(messages)-1]
	if last.Role != "user" {
		return p.chatCompat(ctx, messages, model, cacheKey, options)
	}

	// Only start stateful for short conversations (system + user).
	// Longer histories mean we'd lose context the server doesn't have.
	if len(messages) > 2 {
		return p.chatCompat(ctx, messages, model, cacheKey, options)
	}

	var systemPrompt string
	if messages[0].Role == "system" {
		systemPrompt = messages[0].Content
	}

	resp, respID, err := p.doStatefulChat(ctx, last.Content, model, "", systemPrompt, options)
	if err != nil {
		log.Printf("lmstudio: stateful new session failed, falling back: %v", err)
		return p.chatCompat(ctx, messages, model, cacheKey, options)
	}

	p.mu.Lock()
	p.sessions[cacheKey] = &session{
		responseID:   respID,
		messagesSent: len(messages) + 1,
	}
	p.mu.Unlock()

	return resp, nil
}

// chatCompat delegates to the OpenAI-compatible endpoint.
func (p *Provider) chatCompat(
	ctx context.Context,
	messages []Message,
	model, cacheKey string,
	options map[string]any,
) (*LLMResponse, error) {
	p.invalidateSession(cacheKey)
	return p.compat.Chat(ctx, sanitizeToolMessages(messages), nil, model, options)
}

// sanitizeToolMessages rewrites tool-related messages so that models whose Jinja
// chat templates don't understand role="tool" or assistant tool_calls still get
// a coherent conversation. Each assistant message with tool_calls becomes a plain
// assistant message describing the calls, and each tool result becomes a user
// message with the output.
func sanitizeToolMessages(messages []Message) []Message {
	hasToolRole := false
	for _, m := range messages {
		if m.Role == "tool" {
			hasToolRole = true
			break
		}
	}
	if !hasToolRole {
		return messages
	}

	out := make([]Message, 0, len(messages))
	for _, m := range messages {
		switch {
		case m.Role == "assistant" && len(m.ToolCalls) > 0:
			var sb strings.Builder
			if m.Content != "" {
				sb.WriteString(m.Content)
				sb.WriteString("\n\n")
			}
			for _, tc := range m.ToolCalls {
				name := tc.Name
				if name == "" && tc.Function != nil {
					name = tc.Function.Name
				}
				argsJSON, _ := json.Marshal(tc.Arguments)
				fmt.Fprintf(&sb, "[Calling tool: %s(%s)]\n", name, string(argsJSON))
			}
			out = append(out, Message{
				Role:    "assistant",
				Content: sb.String(),
			})

		case m.Role == "tool":
			content := m.Content
			if m.ToolCallID != "" {
				content = fmt.Sprintf("[Tool result for %s]:\n%s", m.ToolCallID, m.Content)
			}
			out = append(out, Message{
				Role:    "user",
				Content: content,
			})

		default:
			out = append(out, m)
		}
	}
	return out
}

func (p *Provider) invalidateSession(cacheKey string) {
	if cacheKey == "" {
		return
	}
	p.mu.Lock()
	delete(p.sessions, cacheKey)
	p.mu.Unlock()
}

// --- LM Studio REST API wire types ---

type restRequest struct {
	Model              string  `json:"model"`
	Input              string  `json:"input"`
	SystemPrompt       string  `json:"system_prompt,omitempty"`
	PreviousResponseID string  `json:"previous_response_id,omitempty"`
	MaxOutputTokens    int     `json:"max_output_tokens,omitempty"`
	Temperature        float64 `json:"temperature,omitempty"`
	Store              bool    `json:"store"`
}

type restResponse struct {
	ModelInstanceID string       `json:"model_instance_id"`
	Output          []restOutput `json:"output"`
	Stats           restStats    `json:"stats"`
	ResponseID      string       `json:"response_id"`
	Error           *restError   `json:"error,omitempty"`
}

type restOutput struct {
	Type    string `json:"type"` // "message", "reasoning", "tool_call"
	Content string `json:"content,omitempty"`
}

type restStats struct {
	InputTokens            int     `json:"input_tokens"`
	TotalOutputTokens      int     `json:"total_output_tokens"`
	ReasoningOutputTokens  int     `json:"reasoning_output_tokens"`
	TokensPerSecond        float64 `json:"tokens_per_second"`
	TimeToFirstTokenSecs   float64 `json:"time_to_first_token_seconds"`
}

type restError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

func (p *Provider) doStatefulChat(
	ctx context.Context,
	input, model, prevResponseID, systemPrompt string,
	options map[string]any,
) (*LLMResponse, string, error) {
	req := restRequest{
		Model: model,
		Input: input,
		Store: true,
	}
	if systemPrompt != "" {
		req.SystemPrompt = systemPrompt
	}
	if prevResponseID != "" {
		req.PreviousResponseID = prevResponseID
	}
	if maxTokens, ok := asInt(options["max_tokens"]); ok {
		req.MaxOutputTokens = maxTokens
	}
	if temp, ok := asFloat(options["temperature"]); ok {
		req.Temperature = temp
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, "", fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", p.apiBase+"/api/v1/chat", bytes.NewReader(body))
	if err != nil {
		return nil, "", fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, "", fmt.Errorf("send request: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read response: %w", err)
	}

	if httpResp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("API %d: %s", httpResp.StatusCode, string(respBody))
	}

	var resp restResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, "", fmt.Errorf("unmarshal response: %w", err)
	}
	if resp.Error != nil {
		return nil, "", fmt.Errorf("API error: %s", resp.Error.Message)
	}

	llmResp := mapRestResponse(&resp)
	return llmResp, resp.ResponseID, nil
}

func mapRestResponse(r *restResponse) *LLMResponse {
	var content strings.Builder
	var reasoning strings.Builder

	for _, out := range r.Output {
		switch out.Type {
		case "message":
			content.WriteString(out.Content)
		case "reasoning":
			reasoning.WriteString(out.Content)
		}
	}

	contentStr := content.String()
	reasoningStr := reasoning.String()

	// Strip <think> tags from content when the REST API doesn't separate them.
	if reasoningStr == "" {
		var extracted string
		contentStr, extracted = protocoltypes.ExtractThinkContent(contentStr)
		reasoningStr = extracted
	}

	return &LLMResponse{
		Content:          contentStr,
		ReasoningContent: reasoningStr,
		FinishReason:     "stop",
		Usage: &UsageInfo{
			PromptTokens:     r.Stats.InputTokens,
			CompletionTokens: r.Stats.TotalOutputTokens,
			TotalTokens:      r.Stats.InputTokens + r.Stats.TotalOutputTokens,
		},
	}
}

func asInt(v any) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	default:
		return 0, false
	}
}

func asFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	default:
		return 0, false
	}
}
