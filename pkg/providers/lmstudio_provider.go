package providers

import (
	"context"

	"github.com/sipeed/picoclaw/pkg/providers/lmstudio"
)

type LMStudioProvider struct {
	delegate *lmstudio.Provider
}

func NewLMStudioProvider(apiKey, apiBase, proxy string) *LMStudioProvider {
	return &LMStudioProvider{
		delegate: lmstudio.NewProvider(apiKey, apiBase, proxy),
	}
}

func (p *LMStudioProvider) Chat(
	ctx context.Context,
	messages []Message,
	tools []ToolDefinition,
	model string,
	options map[string]any,
) (*LLMResponse, error) {
	return p.delegate.Chat(ctx, messages, tools, model, options)
}

func (p *LMStudioProvider) GetDefaultModel() string {
	return ""
}
