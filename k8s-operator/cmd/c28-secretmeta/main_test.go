package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestBadArgumentsAreRefusedWithAFixedPhraseAndNoOutput(t *testing.T) {
	cases := [][]string{
		{},
		{"--no-such-flag"},
		{"--kubeconfig", "/dev/null", "--context", "c", "--namespace", "ns", "--name", "n", "extra"},
		{"--kubeconfig", "/dev/null", "--context", "c", "--namespace", "ns", "--name", "n", "--require-label", "novalue"},
		{"--kubeconfig", "/dev/null", "--context", "c", "--namespace", "ns", "--name", "n", "--require-label", "=v"},
		{"--kubeconfig", "/dev/null", "--context", "c", "--namespace", "NS", "--name", "n"},
		{"--kubeconfig", "/dev/null", "--context", "c", "--namespace", "ns", "--name", "n", "--timeout", "10m"},
		{"--kubeconfig", "/dev/null", "--context", "c", "--namespace", "ns", "--name", "n"},
	}
	for i, args := range cases {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 3 {
			t.Fatalf("case %d: exit %d", i, code)
		}
		if out.Len() != 0 || !strings.HasPrefix(errOut.String(), "c28-secretmeta: refused: ") || strings.Count(errOut.String(), "\n") != 1 {
			t.Fatalf("case %d: stdout %q stderr %q", i, out.String(), errOut.String())
		}
	}
}
