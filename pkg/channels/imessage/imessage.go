// PicoClaw - Ultra-lightweight personal AI agent
// iMessage channel implementation (macOS only)
//
// Receives messages by polling ~/Library/Messages/chat.db (SQLite).
// Sends messages via AppleScript through osascript.
// Requires macOS with Messages.app configured and Full Disk Access for the process.

package imessage

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/identity"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const (
	defaultPollInterval = 3
	defaultDBPath       = "~/Library/Messages/chat.db"
	maxMessageLength    = 10000
)

type IMessageChannel struct {
	*channels.BaseChannel
	config       config.IMessageConfig
	db           *sql.DB
	lastRowID    int64
	pollInterval time.Duration
	dbPath       string
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func NewIMessageChannel(cfg config.IMessageConfig, messageBus *bus.MessageBus) (*IMessageChannel, error) {
	base := channels.NewBaseChannel("imessage", cfg, messageBus, cfg.AllowFrom,
		channels.WithMaxMessageLength(maxMessageLength),
		channels.WithReasoningChannelID(cfg.ReasoningChannelID),
	)

	dbPath := cfg.DBPath
	if dbPath == "" {
		dbPath = defaultDBPath
	}
	dbPath = expandHome(dbPath)

	pollSec := cfg.PollInterval
	if pollSec <= 0 {
		pollSec = defaultPollInterval
	}

	return &IMessageChannel{
		BaseChannel:  base,
		config:       cfg,
		pollInterval: time.Duration(pollSec) * time.Second,
		dbPath:       dbPath,
	}, nil
}

func (c *IMessageChannel) Start(ctx context.Context) error {
	logger.InfoC("imessage", "Starting iMessage channel...")

	if _, err := os.Stat(c.dbPath); os.IsNotExist(err) {
		return fmt.Errorf("iMessage database not found at %s — is Messages.app configured?", c.dbPath)
	}

	dsn := fmt.Sprintf("file:%s?mode=ro&_journal_mode=WAL", c.dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("failed to open iMessage database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping iMessage database (Full Disk Access required): %w", err)
	}

	c.db = db

	row := c.db.QueryRow("SELECT COALESCE(MAX(ROWID), 0) FROM message")
	if err := row.Scan(&c.lastRowID); err != nil {
		c.db.Close()
		return fmt.Errorf("failed to read last message ROWID: %w", err)
	}

	c.logDiagnostics()

	logger.InfoCF("imessage", "Starting poll from message ROWID", map[string]any{
		"last_rowid": c.lastRowID,
		"db_path":    c.dbPath,
		"poll_sec":   c.pollInterval.Seconds(),
		"allow_from": c.config.AllowFrom,
	})

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.SetRunning(true)

	c.wg.Add(1)
	go c.pollLoop()

	logger.InfoC("imessage", "iMessage channel started")
	return nil
}

func (c *IMessageChannel) Stop(ctx context.Context) error {
	logger.InfoC("imessage", "Stopping iMessage channel...")

	c.SetRunning(false)
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()

	if c.db != nil {
		c.db.Close()
	}

	logger.InfoC("imessage", "iMessage channel stopped")
	return nil
}

func (c *IMessageChannel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if !c.IsRunning() {
		return channels.ErrNotRunning
	}

	logger.DebugCF("imessage", "Sending message", map[string]any{
		"chat_id": msg.ChatID,
		"preview": utils.Truncate(msg.Content, 100),
	})

	return c.sendViaAppleScript(ctx, msg.ChatID, msg.Content)
}

// SendMedia implements the channels.MediaSender interface.
func (c *IMessageChannel) SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) error {
	if !c.IsRunning() {
		return channels.ErrNotRunning
	}

	store := c.GetMediaStore()
	if store == nil {
		return fmt.Errorf("no media store available: %w", channels.ErrSendFailed)
	}

	for _, part := range msg.Parts {
		localPath, err := store.Resolve(part.Ref)
		if err != nil {
			logger.ErrorCF("imessage", "Failed to resolve media ref", map[string]any{
				"ref":   part.Ref,
				"error": err.Error(),
			})
			continue
		}

		logger.InfoCF("imessage", "Sending file", map[string]any{
			"chat_id":  msg.ChatID,
			"type":     part.Type,
			"filename": part.Filename,
			"path":     localPath,
		})

		if err := c.sendFileViaAppleScript(ctx, msg.ChatID, localPath); err != nil {
			logger.ErrorCF("imessage", "Failed to send file", map[string]any{
				"chat_id": msg.ChatID,
				"path":    localPath,
				"error":   err.Error(),
			})
			continue
		}

		if part.Caption != "" {
			_ = c.sendViaAppleScript(ctx, msg.ChatID, part.Caption)
		}
	}

	return nil
}

func (c *IMessageChannel) pollLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.pollNewMessages()
		}
	}
}

func (c *IMessageChannel) pollNewMessages() {
	// Query includes attributedBody for macOS Ventura+ where text may be NULL
	query := `
		SELECT
			m.ROWID,
			m.text,
			m.attributedBody,
			COALESCE(h.id, '') as sender_id,
			COALESCE(c.chat_identifier, '') as chat_identifier,
			COALESCE(c.display_name, '') as display_name
		FROM message m
		LEFT JOIN handle h ON m.handle_id = h.ROWID
		LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
		LEFT JOIN chat c ON cmj.chat_id = c.ROWID
		WHERE m.ROWID > ? AND m.is_from_me = 0
		ORDER BY m.ROWID ASC
		LIMIT 50
	`

	rows, err := c.db.QueryContext(c.ctx, query, c.lastRowID)
	if err != nil {
		if c.ctx.Err() != nil {
			return
		}
		logger.ErrorCF("imessage", "Failed to poll messages", map[string]any{
			"error": err.Error(),
		})
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var rowID int64
		var textRaw sql.NullString
		var attributedBody []byte
		var senderID, chatIdentifier, displayName string

		if err := rows.Scan(&rowID, &textRaw, &attributedBody, &senderID, &chatIdentifier, &displayName); err != nil {
			logger.ErrorCF("imessage", "Failed to scan message row", map[string]any{
				"error": err.Error(),
			})
			continue
		}

		c.lastRowID = rowID
		count++

		// Try text field first, fall back to extracting from attributedBody
		text := ""
		if textRaw.Valid && textRaw.String != "" {
			text = textRaw.String
		} else if len(attributedBody) > 0 {
			text = extractTextFromAttributedBody(attributedBody)
		}

		if text == "" {
			logger.InfoCF("imessage", "Skipping message with no text content", map[string]any{
				"rowid":          rowID,
				"sender":         senderID,
				"has_text":       textRaw.Valid,
				"has_attr_body":  len(attributedBody) > 0,
			})
			continue
		}

		if senderID == "" {
			logger.InfoCF("imessage", "Skipping message with no sender", map[string]any{
				"rowid": rowID,
			})
			continue
		}

		chatID := chatIdentifier
		if chatID == "" {
			chatID = senderID
		}

		isGroup := strings.HasPrefix(chatIdentifier, "chat")
		var peer bus.Peer
		if isGroup {
			peer = bus.Peer{Kind: "group", ID: chatIdentifier}
		} else {
			peer = bus.Peer{Kind: "direct", ID: senderID}
		}

		senderName := displayName
		if senderName == "" {
			senderName = senderID
		}

		metadata := map[string]string{
			"sender_name":     senderName,
			"chat_identifier": chatIdentifier,
			"platform":        "imessage",
		}

		sender := bus.SenderInfo{
			Platform:    "imessage",
			PlatformID:  senderID,
			CanonicalID: identity.BuildCanonicalID("imessage", senderID),
			DisplayName: senderName,
		}

		logger.InfoCF("imessage", "Received message", map[string]any{
			"rowid":   rowID,
			"sender":  senderID,
			"chat_id": chatID,
			"preview": utils.Truncate(text, 80),
		})

		c.HandleMessage(c.ctx, peer, fmt.Sprintf("%d", rowID), senderID, chatID, text, nil, metadata, sender)
	}

	if count > 0 {
		logger.InfoCF("imessage", "Poll cycle completed", map[string]any{
			"new_messages": count,
			"last_rowid":   c.lastRowID,
		})
	}
}

// sendViaAppleScript sends a message using osascript and AppleScript.
// chatID is expected to be a phone number or email address (the chat_identifier from the DB).
func (c *IMessageChannel) sendViaAppleScript(ctx context.Context, chatID, content string) error {
	escaped := strings.ReplaceAll(content, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)

	script := fmt.Sprintf(`
tell application "Messages"
	set targetService to 1st account whose service type = iMessage
	set targetBuddy to participant "%s" of targetService
	send "%s" to targetBuddy
end tell`, chatID, escaped)

	cmd := exec.CommandContext(ctx, "osascript", "-e", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Fallback: try sending via chat reference for group chats
		if strings.HasPrefix(chatID, "chat") {
			return c.sendViaChatRef(ctx, chatID, content)
		}
		return fmt.Errorf("osascript failed: %s — %w", strings.TrimSpace(string(output)), channels.ErrTemporary)
	}

	return nil
}

// sendFileViaAppleScript sends a file using osascript and POSIX file.
// It copies the file to a staging directory first so Messages.app can
// access it reliably (media store temp paths may be inaccessible).
func (c *IMessageChannel) sendFileViaAppleScript(ctx context.Context, chatID, filePath string) error {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path: %w", err)
	}

	if _, err := os.Stat(absPath); err != nil {
		return fmt.Errorf("file not found: %s — %w", absPath, channels.ErrSendFailed)
	}

	staged, err := c.stageFile(absPath)
	if err != nil {
		return fmt.Errorf("failed to stage file: %w", err)
	}
	defer func() {
		go func() {
			time.Sleep(30 * time.Second)
			os.Remove(staged)
		}()
	}()

	script := fmt.Sprintf(`
tell application "Messages"
	set theFile to POSIX file "%s"
	set targetService to 1st account whose service type = iMessage
	set targetBuddy to participant "%s" of targetService
	send theFile to targetBuddy
end tell`, staged, chatID)

	cmd := exec.CommandContext(ctx, "osascript", "-e", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.HasPrefix(chatID, "chat") {
			return c.sendFileToChatRef(ctx, chatID, staged)
		}
		return fmt.Errorf("osascript send file failed: %s — %w", strings.TrimSpace(string(output)), channels.ErrTemporary)
	}

	return nil
}

// sendFileToChatRef sends a file to a group chat reference.
func (c *IMessageChannel) sendFileToChatRef(ctx context.Context, chatID, stagedPath string) error {
	script := fmt.Sprintf(`
tell application "Messages"
	set theFile to POSIX file "%s"
	send theFile to chat "%s"
end tell`, stagedPath, chatID)

	cmd := exec.CommandContext(ctx, "osascript", "-e", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("osascript send file to chat failed: %s — %w", strings.TrimSpace(string(output)), channels.ErrTemporary)
	}

	return nil
}

// stageFile copies a file to /tmp/picoclaw-media/ preserving the original
// filename and extension, so Messages.app can properly detect the file type.
func (c *IMessageChannel) stageFile(srcPath string) (string, error) {
	stageDir := filepath.Join(os.TempDir(), "picoclaw-media")
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return "", err
	}

	filename := filepath.Base(srcPath)
	dstPath := filepath.Join(stageDir, filename)

	// Avoid collisions
	if _, err := os.Stat(dstPath); err == nil {
		ext := filepath.Ext(filename)
		name := strings.TrimSuffix(filename, ext)
		dstPath = filepath.Join(stageDir, fmt.Sprintf("%s_%d%s", name, time.Now().UnixMilli(), ext))
	}

	src, err := os.Open(srcPath)
	if err != nil {
		return "", err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return "", err
	}
	defer dst.Close()

	if _, err := dst.ReadFrom(src); err != nil {
		os.Remove(dstPath)
		return "", err
	}

	return dstPath, nil
}

// sendViaChatRef tries to send via chat reference (for group chats).
func (c *IMessageChannel) sendViaChatRef(ctx context.Context, chatID, content string) error {
	escaped := strings.ReplaceAll(content, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)

	script := fmt.Sprintf(`
tell application "Messages"
	set targetChat to chat "%s"
	send "%s" to targetChat
end tell`, chatID, escaped)

	cmd := exec.CommandContext(ctx, "osascript", "-e", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("osascript chat-ref failed: %s — %w", strings.TrimSpace(string(output)), channels.ErrTemporary)
	}

	return nil
}

// extractTextFromAttributedBody extracts plain text from the NSAttributedString
// binary plist stored in attributedBody (macOS Ventura+ may use this instead of text).
func extractTextFromAttributedBody(blob []byte) string {
	// The NSAttributedString blob contains the plain text as a UTF-8 run.
	// A common pattern: the text appears after "NSString" marker bytes followed
	// by a length prefix, or as a contiguous valid UTF-8 substring.
	// We look for the longest valid UTF-8 run that looks like user-typed text.

	// Strategy: find the substring between known markers.
	// In NSKeyedArchiver format, text typically follows a specific byte pattern.
	// Look for "NS.string" or the text payload after type indicators.
	markers := [][]byte{
		[]byte("NS.string"),
		[]byte("NSString"),
		[]byte("NS.bytes"),
	}

	for _, marker := range markers {
		idx := bytes.Index(blob, marker)
		if idx < 0 {
			continue
		}
		// Skip past marker and any length/type bytes
		start := idx + len(marker)
		// Scan forward past non-printable bytes to find the text start
		for start < len(blob) && (blob[start] < 0x20 || blob[start] > 0x7E) && blob[start] < 0x80 {
			start++
		}
		if start >= len(blob) {
			continue
		}
		// Extract valid UTF-8 text until we hit control/null bytes
		end := start
		for end < len(blob) {
			r, size := utf8.DecodeRune(blob[end:])
			if r == utf8.RuneError && size <= 1 {
				break
			}
			if r < 0x20 && r != '\n' && r != '\r' && r != '\t' {
				break
			}
			end += size
		}
		if text := strings.TrimSpace(string(blob[start:end])); text != "" {
			return text
		}
	}

	return ""
}

// logDiagnostics prints database info at startup to help troubleshoot issues.
func (c *IMessageChannel) logDiagnostics() {
	var totalMessages, recentIncoming int64

	c.db.QueryRow("SELECT COUNT(*) FROM message").Scan(&totalMessages)
	c.db.QueryRow("SELECT COUNT(*) FROM message WHERE is_from_me = 0 AND ROWID > ?", c.lastRowID-100).Scan(&recentIncoming)

	// Show the 3 most recent incoming messages for debugging
	rows, err := c.db.Query(`
		SELECT m.ROWID, COALESCE(m.text, '[no text]'), COALESCE(h.id, '[no handle]'),
		       m.text IS NOT NULL as has_text, LENGTH(m.attributedBody) as attr_len
		FROM message m
		LEFT JOIN handle h ON m.handle_id = h.ROWID
		WHERE m.is_from_me = 0
		ORDER BY m.ROWID DESC LIMIT 3
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var rowID int64
			var text, handle string
			var hasText bool
			var attrLen sql.NullInt64
			if rows.Scan(&rowID, &text, &handle, &hasText, &attrLen) == nil {
				logger.InfoCF("imessage", "Recent incoming message", map[string]any{
					"rowid":         rowID,
					"handle":        handle,
					"has_text":      hasText,
					"attr_body_len": attrLen.Int64,
					"preview":       utils.Truncate(text, 60),
				})
			}
		}
	}

	logger.InfoCF("imessage", "Database diagnostics", map[string]any{
		"total_messages":         totalMessages,
		"recent_incoming_around": recentIncoming,
		"max_rowid":             c.lastRowID,
		"db_path":               c.dbPath,
	})
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		return filepath.Join(home, path[2:])
	}
	return path
}
