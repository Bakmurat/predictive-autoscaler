package controllers

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The write allowlist: with it, a replica count is written only to a listed Deployment at its reviewed UID. RBAC
// resourceNames cannot do this (they bind a name), and the reactive path needs no forecast qualification.

func TestWriteAllowlistParsing(t *testing.T) {
	w, err := ParseWriteAllowlist(" demo/web=11111111-2222-3333-4444-555555555555 , shop/api.v2=abc ")
	if err != nil || w.Len() != 2 {
		t.Fatalf("a well-formed list must parse: %v, %d entries", err, w.Len())
	}
	if why := w.refusal("demo", "web", "11111111-2222-3333-4444-555555555555"); why != "" {
		t.Fatalf("the listed object must be allowed: %s", why)
	}
	for _, c := range []struct{ namespace, name, uid, want string }{
		{"demo", "other", "abc", "not in the operator's write allowlist"},
		{"shop", "web", "11111111-2222-3333-4444-555555555555", "not in the operator's write allowlist"}, // the name in another namespace
		{"demo", "web", "99999999-2222-3333-4444-555555555555", "was replaced after it was reviewed"},
		{"demo", "web", "", "was replaced after it was reviewed"},
	} {
		if why := w.refusal(c.namespace, c.name, types.UID(c.uid)); !strings.Contains(why, c.want) {
			t.Errorf("%s/%s uid %q: got %q, want %q", c.namespace, c.name, c.uid, why, c.want)
		}
	}
	var none *WriteAllowlist
	if none.Len() != 0 || none.refusal("any", "thing", "") != "" {
		t.Fatal("no allowlist must restrict nothing")
	}
	long := make([]string, maxWriteAllowlist+1)
	for i := range long {
		long[i] = fmt.Sprintf("demo/web-%d=uid-%d", i, i)
	}
	for raw, want := range map[string]string{
		"":                            "the list is empty",
		"  ":                          "the list is empty",
		"demo/web":                    "is not namespace/name=uid",
		"web=abc":                     "is not namespace/name=uid",
		"Demo/web=abc":                "is not a namespace name",
		"demo/Web=abc":                "is not a Deployment name",
		"demo/web=":                   "is not a UID",
		"demo/web=a b":                "is not a UID",
		"demo/web=abc=def":            "is not a UID",
		"demo/web=abc,":               "is not namespace/name=uid",
		"demo/web=abc,demo/web=abc":   "listed more than once",
		"demo/web=abc,demo/web=other": "listed more than once",
		strings.Join(long, ","):       "at most 64 are allowed",
	} {
		if w, err := ParseWriteAllowlist(raw); err == nil || w != nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%.40q: got (%v, %v), want an error containing %q", raw, w, err, want)
		}
	}
}

func TestWriteAllowlistFromEnvIsUnrestrictedOnlyWhenUnset(t *testing.T) {
	if w, err := WriteAllowlistFromEnv(); w != nil || err != nil {
		t.Fatalf("unset must mean no restriction: %v, %v", w, err)
	}
	t.Setenv(WriteAllowlistEnv, "")
	if w, err := WriteAllowlistFromEnv(); w != nil || err == nil || !strings.Contains(err.Error(), WriteAllowlistEnv) {
		t.Fatalf("a set but empty list must be an error naming the variable: %v, %v", w, err)
	}
	t.Setenv(WriteAllowlistEnv, "demo/web=abc")
	if w, err := WriteAllowlistFromEnv(); err != nil || w.Len() != 1 {
		t.Fatalf("a set list must be read: %v, %v", w, err)
	}
}

// allowlistHarness is an Active autoscaler that needs 5 replicas (1 now) on the reactive path alone: the forecasting
// service answers 503. It returns the target and a counter of /scale updates that reached the API.
func allowlistHarness(t *testing.T) (*PredictiveAutoscalerReconciler, ctrl.Request, string, *appsv1.Deployment, *int) {
	t.Helper()
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	if dep.UID == "" {
		t.Fatal("the harness target needs a UID")
	}
	writes := new(int)
	wrapClient(r, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				*writes++
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	return r, req, path, &dep, writes
}

func allowlistFor(t *testing.T, namespace, name string, uid types.UID) *WriteAllowlist {
	t.Helper()
	w, err := ParseWriteAllowlist(fmt.Sprintf("%s/%s=%s", namespace, name, uid))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func scalingActive(t *testing.T, r *PredictiveAutoscalerReconciler, req ctrl.Request) (string, string) {
	t.Helper()
	for _, c := range getPA(t, r, req).Status.Conditions {
		if c.Type == "ScalingActive" {
			return c.Reason, c.Message
		}
	}
	t.Fatal("no ScalingActive condition")
	return "", ""
}

func TestAListedTargetIsScaled(t *testing.T) {
	r, req, path, dep, writes := allowlistHarness(t)
	r.WriteAllowlist = allowlistFor(t, dep.Namespace, dep.Name, dep.UID)
	if d := runRPMReconcile(t, r, req, path); d.Action != "scale_up" || d.AppliedReplicas != 5 || *writes != 1 {
		t.Fatalf("the reviewed target must be scaled to 5 with one write: %+v, %d writes", d, *writes)
	}
}

func TestAnUnlistedTargetIsNeverWritten(t *testing.T) {
	r, req, path, dep, writes := allowlistHarness(t)
	r.WriteAllowlist = allowlistFor(t, dep.Namespace, "another-deployment", dep.UID)
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "target_not_allowed" || d.AppliedReplicas != 1 || *writes != 0 {
		t.Fatalf("an unlisted target must not be written: %+v, %d writes", d, *writes)
	}
	reason, message := scalingActive(t, r, req)
	if reason != "TargetNotAllowed" || !strings.Contains(message, "not in the operator's write allowlist") {
		t.Fatalf("the status must say why nothing is written: %s: %s", reason, message)
	}
	if s := getPA(t, r, req).Status; s.CalculatedReplicas != 5 || s.AppliedReplicas != 0 {
		t.Fatalf("the decision is still computed and published, and nothing is applied: %+v", s)
	}
}

// The case RBAC cannot stop: the Deployment is deleted and created again under the listed name. The operator adopts
// the new object on its next reconcile, and without the allowlist it would scale it reactively.
func TestAReplacedTargetIsNotWrittenUntilItIsReviewed(t *testing.T) {
	r, req, path, dep, writes := allowlistHarness(t)
	r.WriteAllowlist = allowlistFor(t, dep.Namespace, dep.Name, dep.UID)
	dep.UID = types.UID("replacement")
	if err := r.Update(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // the reconcile that adopts the new UID and the one after it
		if d := runRPMReconcile(t, r, req, path); d.Action != "target_not_allowed" || *writes != 0 {
			t.Fatalf("reconcile %d: a replaced target must not be written: %+v, %d writes", i+1, d, *writes)
		}
	}
	if reason, message := scalingActive(t, r, req); reason != "TargetNotAllowed" || !strings.Contains(message, "was replaced after it was reviewed") {
		t.Fatalf("the status must name the replacement: %s: %s", reason, message)
	}
	r.WriteAllowlist = allowlistFor(t, dep.Namespace, dep.Name, "replacement") // reviewed: the operator restarts with the new list
	if d := runRPMReconcile(t, r, req, path); d.Action != "scale_up" || *writes != 1 {
		t.Fatalf("the reviewed replacement must be scaled: %+v, %d writes", d, *writes)
	}
}

func TestRecommendModeIsUnchangedByTheAllowlist(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	r.WriteAllowlist = allowlistFor(t, req.Namespace, "another-deployment", "abc")
	if d := runRPMReconcile(t, r, req, path); d.Action != "recommend" {
		t.Fatalf("Recommend mode writes nothing and keeps its own record: %+v", d)
	}
	if reason, _ := scalingActive(t, r, req); reason != "RecommendMode" {
		t.Fatalf("Recommend mode keeps its own reason: %s", reason)
	}
}

// The check sits at the write itself as well: a Scale whose UID is not the reviewed one is not sent, whatever the
// caller decided before.
func TestTheWriteItselfRefusesAnUnreviewedUID(t *testing.T) {
	r, _, _, dep, writes := allowlistHarness(t)
	r.WriteAllowlist = allowlistFor(t, dep.Namespace, dep.Name, dep.UID)
	scale := &autoscalingv1.Scale{}
	if err := r.SubResource("scale").Get(context.Background(), dep.DeepCopy(), scale); err != nil {
		t.Fatal(err)
	}
	scale.UID = types.UID("replacement")
	err := r.writeScale(context.Background(), dep, scale, 5)
	if !stderrors.Is(err, errWriteNotAllowed) || *writes != 0 {
		t.Fatalf("an unreviewed UID must be refused before the request: %v, %d writes", err, *writes)
	}
	scale.UID = dep.UID
	if err := r.writeScale(context.Background(), dep, scale, 5); err != nil || *writes != 1 {
		t.Fatalf("the reviewed UID must be written: %v, %d writes", err, *writes)
	}
}
