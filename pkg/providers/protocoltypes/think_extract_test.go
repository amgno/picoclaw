package protocoltypes

import "testing"

func TestExtractThinkContent_NoThinkTags(t *testing.T) {
	content := "Hello, how can I help you?"
	cleaned, reasoning := ExtractThinkContent(content)
	if cleaned != content {
		t.Errorf("cleaned = %q, want %q", cleaned, content)
	}
	if reasoning != "" {
		t.Errorf("reasoning = %q, want empty", reasoning)
	}
}

func TestExtractThinkContent_SingleBlock(t *testing.T) {
	content := "<think>Let me reason about this...</think>\n\nThe answer is 42."
	cleaned, reasoning := ExtractThinkContent(content)
	if cleaned != "The answer is 42." {
		t.Errorf("cleaned = %q, want %q", cleaned, "The answer is 42.")
	}
	if reasoning != "Let me reason about this..." {
		t.Errorf("reasoning = %q, want %q", reasoning, "Let me reason about this...")
	}
}

func TestExtractThinkContent_MultipleBlocks(t *testing.T) {
	content := "<think>First thought</think>\nSome text\n<think>Second thought</think>\nFinal answer."
	cleaned, reasoning := ExtractThinkContent(content)
	if cleaned != "Some text\n\nFinal answer." {
		t.Errorf("cleaned = %q, want %q", cleaned, "Some text\n\nFinal answer.")
	}
	if reasoning != "First thought\n\nSecond thought" {
		t.Errorf("reasoning = %q, want %q", reasoning, "First thought\n\nSecond thought")
	}
}

func TestExtractThinkContent_MultilineThinkBlock(t *testing.T) {
	content := "<think>\nLine 1\nLine 2\nLine 3\n</think>\n\nResult here."
	cleaned, reasoning := ExtractThinkContent(content)
	if cleaned != "Result here." {
		t.Errorf("cleaned = %q, want %q", cleaned, "Result here.")
	}
	if reasoning != "Line 1\nLine 2\nLine 3" {
		t.Errorf("reasoning = %q, want %q", reasoning, "Line 1\nLine 2\nLine 3")
	}
}

func TestExtractThinkContent_EmptyThinkBlock(t *testing.T) {
	content := "<think></think>Hello"
	cleaned, reasoning := ExtractThinkContent(content)
	if cleaned != "Hello" {
		t.Errorf("cleaned = %q, want %q", cleaned, "Hello")
	}
	if reasoning != "" {
		t.Errorf("reasoning = %q, want empty", reasoning)
	}
}

func TestExtractThinkContent_OnlyThinkBlock(t *testing.T) {
	content := "<think>Just reasoning, no answer</think>"
	cleaned, reasoning := ExtractThinkContent(content)
	if cleaned != "" {
		t.Errorf("cleaned = %q, want empty", cleaned)
	}
	if reasoning != "Just reasoning, no answer" {
		t.Errorf("reasoning = %q, want %q", reasoning, "Just reasoning, no answer")
	}
}

func TestExtractThinkContent_PreservesExistingReasoning(t *testing.T) {
	// No <think> tags at all
	content := "Regular content without think tags"
	cleaned, reasoning := ExtractThinkContent(content)
	if cleaned != content {
		t.Errorf("cleaned = %q, want %q", cleaned, content)
	}
	if reasoning != "" {
		t.Errorf("reasoning should be empty, got %q", reasoning)
	}
}
