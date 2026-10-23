// Package render writes the Claude Code CLI's configuration into the
// memory-backed run directory at start: the MCP file (with the core bearer
// token), a settings file whose apiKeyHelper prints the Anthropic key file,
// the helper script and the system prompt. Nothing here is passed through an
// environment variable or an argument.
package render

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ServerName is the MCP server name the CLI sees; its tools are
// mcp__core__<tool>.
const ServerName = "core"

// ProfileName is the name of the federation profile Render writes.
const ProfileName = "spot"

// Input is what Render needs.
type Input struct {
	RunDir           string
	StateDir         string
	CoreMCPURL       string
	CoreTokenFile    string
	AnthropicKeyFile string // key mode
	// Federation mode: set Federation and leave AnthropicKeyFile empty.
	Federation  *Federation
	HookBin     string // absolute path of the tria-spot binary
	Instruction string // the rendered system prompt
	// OtelTokenFile, when set, makes the CLI send its traces with a bearer
	// token read from this file by a rendered helper (otelHeadersHelper).
	OtelTokenFile string
}

// Federation is the non-secret part of Workload Identity Federation: the ids
// and the path of the projected identity token.
type Federation struct {
	IdentityTokenFile string
	RuleID            string
	OrganizationID    string
	ServiceAccountID  string
	WorkspaceID       string // optional
}

// Paths are the files Render wrote.
type Paths struct {
	MCPConfig   string
	Settings    string
	KeyHelper   string // key mode only
	OtelHelper  string // when OtelTokenFile is set
	Instruction string
	// AnthropicConfigDir and Profile are set in federation mode: the CLI is
	// started with ANTHROPIC_CONFIG_DIR and ANTHROPIC_PROFILE pointing here.
	AnthropicConfigDir string
	Profile            string
}

// Render writes the files with mode 0400 (the helper 0500) in a 0700 run
// directory, replacing earlier ones atomically.
func Render(in Input) (Paths, error) {
	if _, err := readSecretFile(in.CoreTokenFile); err != nil {
		return Paths{}, fmt.Errorf("core token: %w", err)
	}
	if in.Federation == nil {
		if _, err := readSecretFile(in.AnthropicKeyFile); err != nil {
			return Paths{}, fmt.Errorf("anthropic key: %w", err)
		}
	} else if in.AnthropicKeyFile != "" {
		return Paths{}, errors.New("federation mode takes no API key file: it would shadow federation")
	}
	if err := os.MkdirAll(in.RunDir, 0o700); err != nil {
		return Paths{}, err
	}
	// A mounted emptyDir belongs to root with the pod's fsGroup, so the
	// supervisor cannot chmod its root. Every file below carries its own mode.
	if err := os.Chmod(in.RunDir, 0o700); err != nil && !errors.Is(err, fs.ErrPermission) {
		return Paths{}, err
	}
	p := Paths{
		MCPConfig:   filepath.Join(in.RunDir, "mcp.json"),
		Settings:    filepath.Join(in.RunDir, "settings.json"),
		Instruction: filepath.Join(in.RunDir, "instruction.md"),
	}

	if in.Federation == nil {
		p.KeyHelper = filepath.Join(in.RunDir, "api-key-helper.sh")
	}

	if err := WriteMCP(p.MCPConfig, in.CoreMCPURL, in.CoreTokenFile); err != nil {
		return Paths{}, err
	}

	if in.Federation != nil {
		_ = os.Remove(filepath.Join(in.RunDir, "api-key-helper.sh")) // never leave a helper that would shadow federation
		if err := renderProfile(in, &p); err != nil {
			return Paths{}, err
		}
	} else {
		helper := "#!/bin/sh\n# Prints the Anthropic key so it is never an environment variable or an argument.\nexec tr -d '\\r\\n' < " + shQuote(in.AnthropicKeyFile) + "\n"
		if err := writeFile(p.KeyHelper, []byte(helper), 0o500); err != nil {
			return Paths{}, err
		}
	}

	if in.OtelTokenFile != "" {
		p.OtelHelper = filepath.Join(in.RunDir, "otel-headers.sh")
		helper := "#!/bin/sh\n# Prints the OTLP sender's header so the token is never an environment variable or an argument.\nprintf '{\"Authorization\":\"Bearer %s\"}' \"$(tr -d '\\r\\n' < " + shQuote(in.OtelTokenFile) + ")\"\n"
		if err := writeFile(p.OtelHelper, []byte(helper), 0o500); err != nil {
			return Paths{}, err
		}
	}
	if err := writeJSON(p.Settings, Settings(in, p.KeyHelper), 0o400); err != nil {
		return Paths{}, err
	}
	if err := writeFile(p.Instruction, []byte(in.Instruction), 0o400); err != nil {
		return Paths{}, err
	}
	return p, nil
}

// WriteMCP writes the CLI's MCP file for the core with the bearer token read
// from tokenFile now. The runner calls it before every turn, so a rotated
// token (a projected ServiceAccount token the kubelet rewrites, or a replaced
// Secret) reaches the CLI without a restart.
func WriteMCP(path, url, tokenFile string, extra ...Server) error {
	token, err := readSecretFile(tokenFile)
	if err != nil {
		return fmt.Errorf("core token: %w", err)
	}
	servers := map[string]any{ServerName: map[string]any{
		"type":    "http",
		"url":     url,
		"headers": map[string]string{"Authorization": "Bearer " + token},
	}}
	for _, s := range extra {
		if s.TokenFile == "" {
			// A connector holds its own credential; the spot sends none.
			servers[s.Name] = map[string]any{"type": "http", "url": s.URL}
			continue
		}
		t, err := readSecretFile(s.TokenFile)
		if err != nil {
			return fmt.Errorf("%s token: %w", s.Name, err)
		}
		servers[s.Name] = map[string]any{
			"type":    "http",
			"url":     s.URL,
			"headers": map[string]string{"Authorization": "Bearer " + t},
		}
	}
	return writeJSON(path, map[string]any{"mcpServers": servers}, 0o400)
}

// EgoServerName is the MCP server name of ego; its tools are mcp__ego__<tool>.
const EgoServerName = "ego"

// Server is one more MCP server beside the core: its name, streamable-HTTP
// URL and the file holding its bearer token (read at every render); no token
// file means the spot sends no credential (a connector).
type Server struct {
	Name      string
	URL       string
	TokenFile string
}

// Settings builds the settings JSON. Exposed for tests.
func Settings(in Input, helperPath string) map[string]any {
	hook := map[string]any{
		"type":    "command",
		"command": in.HookBin,
		"args": []string{
			"hook", "pretool",
			"--state-dir", in.StateDir,
		},
		"timeout": 20,
	}
	deny := []string{"Read(" + absRule(in.RunDir) + "/**)"}
	st := map[string]any{
		"hooks": map[string]any{
			"PreToolUse": []any{map[string]any{
				"matcher": "Bash",
				"hooks":   []any{hook},
			}},
		},
		// Best effort only: the shell runs as the same user and can cat these.
		// The key's spend limit and the ServiceAccount's RBAC are the boundary.
	}
	deny = append(deny, "Read("+absRule(in.CoreTokenFile)+")")
	if in.OtelTokenFile != "" {
		deny = append(deny, "Read("+absRule(in.OtelTokenFile)+")")
		st["otelHeadersHelper"] = filepath.Join(in.RunDir, "otel-headers.sh")
	}
	if in.Federation != nil {
		// No apiKeyHelper: it outranks federation and would shadow it. The
		// profile and the cached access token live under the run dir (already
		// denied); the identity token file is denied explicitly.
		deny = append(deny, "Read("+absRule(in.Federation.IdentityTokenFile)+")")
	} else {
		st["apiKeyHelper"] = helperPath
		deny = append(deny, "Read("+absRule(in.AnthropicKeyFile)+")")
	}
	st["permissions"] = map[string]any{"deny": deny}
	return st
}

// profileFile is the federation profile, https://platform.claude.com/docs/en/
// manage-claude/wif-reference#profile-configuration-file. It holds no secret.
func profileFile(f *Federation) map[string]any {
	prof := map[string]any{
		"version": "1.0",
		"authentication": map[string]any{
			"type":               "oidc_federation",
			"federation_rule_id": f.RuleID,
			"service_account_id": f.ServiceAccountID,
			"identity_token":     map[string]any{"source": "file", "path": f.IdentityTokenFile},
		},
		"organization_id": f.OrganizationID,
		"base_url":        "https://api.anthropic.com",
	}
	if f.WorkspaceID != "" {
		prof["workspace_id"] = f.WorkspaceID
	}
	return prof
}

// renderProfile writes <run>/anthropic/configs/<ProfileName>.json and makes
// the credentials directory the SDK caches the access token in. Both are on
// the memory-backed run dir, never on the state volume.
func renderProfile(in Input, p *Paths) error {
	dir := filepath.Join(in.RunDir, "anthropic")
	for _, d := range []string{dir, filepath.Join(dir, "configs"), filepath.Join(dir, "credentials")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(d, 0o700); err != nil {
			return err
		}
	}
	if err := writeJSON(filepath.Join(dir, "configs", ProfileName+".json"), profileFile(in.Federation), 0o400); err != nil {
		return err
	}
	p.AnthropicConfigDir, p.Profile = dir, ProfileName
	return nil
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
