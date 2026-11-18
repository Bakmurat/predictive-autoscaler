// Command c28-secretmeta prints the metadata of one named Secret (never its contents) as JSON.
//
//	c28-secretmeta --kubeconfig F --context C --namespace NS --name NAME
//	               [--label-key K]... [--require-label K=V]... [--timeout 15s]
//
// Exit status: 0 with {"found": true|false, ...} on a proven answer; 3 with one fixed phrase on stderr otherwise.
// See package secretmeta for what is requested, accepted and returned.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"predictive-autoscaler/internal/secretmeta"
)

type list []string

func (l *list) String() string     { return strings.Join(*l, ",") }
func (l *list) Set(v string) error { *l = append(*l, v); return nil }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("c28-secretmeta", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var keys, require list
	kubeconfig := fs.String("kubeconfig", "", "explicit kubeconfig path")
	kubecontext := fs.String("context", "", "explicit context")
	namespace := fs.String("namespace", "", "the Secret's namespace")
	name := fs.String("name", "", "the Secret's name")
	timeout := fs.Duration("timeout", 15*time.Second, "the whole request")
	fs.Var(&keys, "label-key", "a label that may be returned (repeatable)")
	fs.Var(&require, "require-label", "a label that must be present, KEY=VALUE (repeatable)")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "c28-secretmeta: refused: bad arguments")
		return 3
	}
	req := secretmeta.Request{Kubeconfig: *kubeconfig, Context: *kubecontext, Namespace: *namespace, Name: *name,
		LabelKeys: keys, Require: map[string]string{}, Timeout: *timeout}
	for _, kv := range require {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			fmt.Fprintln(stderr, "c28-secretmeta: refused: --require-label needs KEY=VALUE")
			return 3
		}
		req.Require[k] = v
	}
	meta, err := secretmeta.Get(context.Background(), req)
	if err != nil {
		fmt.Fprintln(stderr, "c28-secretmeta: refused: "+err.Error())
		return 3
	}
	if json.NewEncoder(stdout).Encode(meta) != nil {
		fmt.Fprintln(stderr, "c28-secretmeta: refused: the result could not be written")
		return 3
	}
	return 0
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
