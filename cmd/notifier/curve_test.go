package main

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

func TestUsageTextMentionsCurveGen(t *testing.T) {
	t.Parallel()

	if !strings.Contains(usageText(), "notifier curve gen") {
		t.Fatalf("expected usage text to mention curve gen, got %q", usageText())
	}
}

func TestCurveHelpPrintsUsage(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"curve", "help"}, &stdout); err != nil {
		t.Fatalf("run curve help: %v", err)
	}
	if !strings.Contains(stdout.String(), "notifier curve gen") {
		t.Fatalf("expected curve help to mention curve gen, got %q", stdout.String())
	}
}

func TestCurveGenPrintsTOMLConfig(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"curve", "gen"}, &stdout); err != nil {
		t.Fatalf("run curve gen: %v", err)
	}

	output := stdout.String()
	for _, snippet := range []string{
		"[cluster.zeromq]",
		`security = "curve"`,
		"[cluster.zeromq.curve]",
		"server_public_key",
		"server_secret_key",
		"client_public_key",
		"client_secret_key",
		"allowed_client_public_keys",
	} {
		if !strings.Contains(output, snippet) {
			t.Fatalf("expected output to contain %q, got %q", snippet, output)
		}
	}

	serverPublicKey := extractCurveOutputValue(t, output, "server_public_key")
	serverSecretKey := extractCurveOutputValue(t, output, "server_secret_key")
	clientPublicKey := extractCurveOutputValue(t, output, "client_public_key")
	clientSecretKey := extractCurveOutputValue(t, output, "client_secret_key")

	for name, key := range map[string]string{
		"server_public_key": serverPublicKey,
		"server_secret_key": serverSecretKey,
		"client_public_key": clientPublicKey,
		"client_secret_key": clientSecretKey,
	} {
		if len(key) != 40 {
			t.Fatalf("expected %s to be 40 chars, got %d (%q)", name, len(key), key)
		}
	}

	if serverPublicKey == clientPublicKey {
		t.Fatalf("expected server and client public keys to differ")
	}
	if serverSecretKey == clientSecretKey {
		t.Fatalf("expected server and client secret keys to differ")
	}

	allowedKeys := extractCurveAllowedKeys(t, output)
	if len(allowedKeys) != 1 || allowedKeys[0] != clientPublicKey {
		t.Fatalf("unexpected allowed client public keys: %+v", allowedKeys)
	}
}

func TestCurveRejectsUnknownSubcommand(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"curve", "bogus"}, &stdout)
	if err == nil || !strings.Contains(err.Error(), `unknown curve command "bogus"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCurveGenRejectsPositionalArguments(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"curve", "gen", "extra"}, &stdout)
	if err == nil || !strings.Contains(err.Error(), "curve gen does not accept positional arguments") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func extractCurveOutputValue(t *testing.T, output, key string) string {
	t.Helper()

	pattern := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(key) + ` = "([^"]+)"$`)
	matches := pattern.FindStringSubmatch(output)
	if len(matches) != 2 {
		t.Fatalf("missing %s in output %q", key, output)
	}
	return matches[1]
}

func extractCurveAllowedKeys(t *testing.T, output string) []string {
	t.Helper()

	pattern := regexp.MustCompile(`(?m)^allowed_client_public_keys = \["([^"]+)"\]$`)
	matches := pattern.FindStringSubmatch(output)
	if len(matches) != 2 {
		t.Fatalf("missing allowed_client_public_keys in output %q", output)
	}
	return []string{matches[1]}
}
