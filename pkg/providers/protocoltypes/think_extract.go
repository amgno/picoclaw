package protocoltypes

import (
	"regexp"
	"strings"
)

var thinkTagRegex = regexp.MustCompile(`(?s)<think>(.*?)</think>`)

// ExtractThinkContent strips <think>...</think> blocks from content,
// returning the cleaned content and the extracted reasoning text.
// Many local/open-source models (Qwen 3, DeepSeek R1, etc.) embed
// chain-of-thought reasoning inside <think> tags in the content field
// rather than using a dedicated reasoning_content API field.
func ExtractThinkContent(content string) (cleaned, reasoning string) {
	if !strings.Contains(content, "<think>") {
		return content, ""
	}

	matches := thinkTagRegex.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 {
		return content, ""
	}

	var parts []string
	for _, m := range matches {
		parts = append(parts, strings.TrimSpace(m[1]))
	}

	cleaned = strings.TrimSpace(thinkTagRegex.ReplaceAllString(content, ""))
	reasoning = strings.Join(parts, "\n\n")
	return cleaned, reasoning
}
