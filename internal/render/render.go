// Package render writes the Claude Code CLI's configuration into the
// memory-backed run directory at start: the MCP file (with the core bearer
// token), a settings file whose apiKeyHelper prints the Anthropic key file,
// the helper script and the system prompt. Nothing here is passed through an
// environment variable or an argument.
package render

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ServerName is the MCP server name the CLI sees; its tools are
// mcp__core__<tool>.
const ServerName = "core"

// Input is what Render needs.
type Input struct {
	RunDir           string
	StateDir         string
	CoreMCPURL       string
	CoreTokenFile    string
	AnthropicKeyFile string
	HookBin          string // absolute path of the tria-spot binary
	KubectlWritesHr  int
	Instruction      string // the rendered system prompt
}

// Paths are the files Render wrote.
type Paths struct {
	MCPConfig   string
	Settings    string
	KeyHelper   string
	Instruction string
}

// Render writes the files with mode 0400 (the helper 0500) in a 0700 run
// directory, replacing earlier ones atomically.
func Render(in Input) (Paths, error) {
	token, err := readSecretFile(in.CoreTokenFile)
	if err != nil {
		return Paths{}, fmt.Errorf("core token: %w", err)
	}
	if _, err := readSecretFile(in.AnthropicKeyFile); err != nil {
		return Paths{}, fmt.Errorf("anthropic key: %w", err)
	}
	if err := os.MkdirAll(in.RunDir, 0o700); err != nil {
		return Paths{}, err
	}
	if err := os.Chmod(in.RunDir, 0o700); err != nil {
		return Paths{}, err
	}
	p := Paths{
		MCPConfig:   filepath.Join(in.RunDir, "mcp.json"),
		Settings:    filepath.Join(in.RunDir, "settings.json"),
		KeyHelper:   filepath.Join(in.RunDir, "api-key-helper.sh"),
		Instruction: filepath.Join(in.RunDir, "instruction.md"),
	}

	mcp := map[string]any{"mcpServers": map[string]any{ServerName: map[string]any{
		"type":    "http",
		"url":     in.CoreMCPURL,
		"headers": map[string]string{"Authorization": "Bearer " + token},
	}}}
	if err := writeJSON(p.MCPConfig, mcp, 0o400); err != nil {
		return Paths{}, err
	}

	helper := "#!/bin/sh\n# Prints the Anthropic key so it is never an environment variable or an argument.\nexec tr -d '\\r\\n' < " + shQuote(in.AnthropicKeyFile) + "\n"
	if err := writeFile(p.KeyHelper, []byte(helper), 0o500); err != nil {
		return Paths{}, err
	}

	if err := writeJSON(p.Settings, Settings(in, p.KeyHelper), 0o400); err != nil {
		return Paths{}, err
	}
	if err := writeFile(p.Instruction, []byte(in.Instruction), 0o400); err != nil {
		return Paths{}, err
	}
	return p, nil
}

// Settings builds the settings JSON. Exposed for tests.
func Settings(in Input, helperPath string) map[string]any {
	hook := map[string]any{
		"type":    "command",
		"command": in.HookBin,
		"args": []string{
			"hook", "pretool",
			"--state-dir", in.StateDir,
			"--kubectl-writes-per-hour", fmt.Sprint(in.KubectlWritesHr),
		},
		"timeout": 20,
	}
	return map[string]any{
		"apiKeyHelper": helperPath,
		"hooks": map[string]any{
			"PreToolUse": []any{map[string]any{
				"matcher": "Bash",
				"hooks":   []any{hook},
			}},
		},
		// Best effort only: the shell runs as the same user and can cat these.
		// The key's spend limit and the ServiceAccount's RBAC are the boundary.
		"permissions": map[string]any{
			"deny": []string{
				"Read(" + absRule(in.RunDir) + "/**)",
				"Read(" + absRule(in.CoreTokenFile) + ")",
				"Read(" + absRule(in.AnthropicKeyFile) + ")",
			},
		},
	}
}

// absRule writes an absolute path in Claude Code's permission rule syntax,
// where a leading double slash means the filesystem root.
func absRule(p string) string { return "/" + filepath.ToSlash(p) }

func readSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return s, nil
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

func writeJSON(path string, v any, mode os.FileMode) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(path, append(b, '\n'), mode)
}

// writeFile writes to a sibling temp file with the final mode and renames it
// into place, so a reader never sees a partial or world-readable file.
func writeFile(path string, b []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name) // no-op after a successful rename
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
