package controllers

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"text/template/parse"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Metric source (plan item #4). Each PredictiveAutoscaler names the request-rate signal of its target as a PromQL
// template; the operator compiles it once per reconcile and publishes the exact query, its hash and the generation it
// was compiled from in status.metricSource. The forecasting service and the trainer read the query from there (never
// from a request body), and every model, cached forecast and accuracy record is bound to the hash, so a changed signal
// never reuses state built on another one.
//
// Contract (requests-per-second/v1): an instant PromQL expression that returns exactly one series, the target's
// requests per second. Converting to requests per minute happens once, where the operator reads the value.

const (
	metricContract   = "requests-per-second/v1"
	maxRenderedQuery = 2048

	istioQueryTemplate = `sum(rate(istio_requests_total{reporter="destination",destination_workload="{{ .Name }}",destination_workload_namespace="{{ .Namespace }}"}[1m]))`
)

var (
	errInvalidMetricQuery = stderrors.New("invalid metric query")
	templateDefinition    = regexp.MustCompile(`\{\{-?\s*(define|block|template)\b`)
)

type compiledMetricQuery struct {
	Query  string
	SHA256 string
}

// compileMetricQuery renders the request-rate query for the target namespace/name from the PA's source (nil or an
// empty preset = the istio preset, today's signal).
func compileMetricQuery(src *autoscalerv1alpha1.MetricSource, namespace, name string) (compiledMetricQuery, error) {
	preset, text := autoscalerv1alpha1.PresetIstio, istioQueryTemplate
	if src != nil && src.Preset != "" {
		preset = src.Preset
	}
	switch preset {
	case autoscalerv1alpha1.PresetIstio:
		if src != nil && src.Query != "" {
			return compiledMetricQuery{}, fmt.Errorf("%w: query is only allowed with preset %q", errInvalidMetricQuery, autoscalerv1alpha1.PresetPrometheus)
		}
	case autoscalerv1alpha1.PresetPrometheus:
		if src == nil || strings.TrimSpace(src.Query) == "" {
			return compiledMetricQuery{}, fmt.Errorf("%w: preset %q needs a query", errInvalidMetricQuery, preset)
		}
		text = src.Query
	default:
		return compiledMetricQuery{}, fmt.Errorf("%w: unknown preset %q", errInvalidMetricQuery, preset)
	}
	q, err := renderQueryTemplate(text, namespace, name)
	if err != nil {
		return compiledMetricQuery{}, err
	}
	sum := sha256.Sum256([]byte(q)) // the exact rendered bytes: no normalization (whitespace may sit inside literals)
	return compiledMetricQuery{Query: q, SHA256: hex.EncodeToString(sum[:])}, nil
}

// renderQueryTemplate renders text with only {{ .Namespace }} and {{ .Name }} substitutions. The parsed tree is
// allowlisted (text, comments and those two fields; text/template's predefined functions exist even with an empty
// FuncMap, so they are rejected structurally), the values are escaped as PromQL string-literal content, and the
// output is capped while rendering.
func renderQueryTemplate(text, namespace, name string) (string, error) {
	if templateDefinition.MatchString(text) { // also a definition named "query", which would replace the body
		return "", fmt.Errorf("%w: define, block and template actions are not allowed", errInvalidMetricQuery)
	}
	t, err := template.New("query").Option("missingkey=error").Parse(text)
	if err != nil {
		return "", fmt.Errorf("%w: %v", errInvalidMetricQuery, err)
	}
	for _, other := range t.Templates() {
		if other.Name() != "query" {
			return "", fmt.Errorf("%w: template definitions are not allowed", errInvalidMetricQuery)
		}
	}
	if t.Tree == nil || t.Tree.Root == nil {
		return "", fmt.Errorf("%w: empty query", errInvalidMetricQuery)
	}
	if err := allowOnlySubstitutions(t.Tree.Root); err != nil {
		return "", err
	}
	out := &cappedBuffer{limit: maxRenderedQuery}
	values := struct{ Namespace, Name string }{promQLStringContent(namespace), promQLStringContent(name)}
	if err := t.Execute(out, values); err != nil {
		return "", fmt.Errorf("%w: %v", errInvalidMetricQuery, err)
	}
	q := out.String()
	if strings.TrimSpace(q) == "" {
		return "", fmt.Errorf("%w: empty query", errInvalidMetricQuery)
	}
	return q, nil
}

func allowOnlySubstitutions(list *parse.ListNode) error {
	for _, node := range list.Nodes {
		switch n := node.(type) {
		case *parse.TextNode, *parse.CommentNode:
		case *parse.ActionNode:
			p := n.Pipe
			if p == nil || len(p.Decl) != 0 || p.IsAssign || len(p.Cmds) != 1 || len(p.Cmds[0].Args) != 1 {
				return fmt.Errorf("%w: only {{ .Namespace }} and {{ .Name }} are allowed, got %s", errInvalidMetricQuery, n)
			}
			f, ok := p.Cmds[0].Args[0].(*parse.FieldNode)
			if !ok || len(f.Ident) != 1 || (f.Ident[0] != "Namespace" && f.Ident[0] != "Name") {
				return fmt.Errorf("%w: only {{ .Namespace }} and {{ .Name }} are allowed, got %s", errInvalidMetricQuery, n)
			}
		default:
			return fmt.Errorf("%w: only {{ .Namespace }} and {{ .Name }} are allowed, got %s", errInvalidMetricQuery, node)
		}
	}
	return nil
}

// promQLStringContent escapes a value for use inside a double-quoted PromQL string literal.
func promQLStringContent(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`).Replace(v)
}

type cappedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.limit {
		return 0, fmt.Errorf("%w: the rendered query exceeds %d bytes", errInvalidMetricQuery, b.limit)
	}
	return b.Buffer.Write(p)
}
