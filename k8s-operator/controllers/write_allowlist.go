package controllers

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

// WriteAllowlistEnv names the reviewed Deployments the operator may scale, as comma-separated
// "namespace/name=uid" entries. RBAC resourceNames bind a name, not an object: a Deployment deleted and created again
// under a listed name is a different object that nobody reviewed. With the allowlist, every replica write, reactive
// ones included, also requires the target's UID to be the listed one.
const WriteAllowlistEnv = "SCALE_WRITE_ALLOWLIST"

// maxWriteAllowlist matches the chart's limit on operator.rbac.scaleTargets.
const maxWriteAllowlist = 64

var (
	allowlistNamespace = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	allowlistName      = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
	// A UID is opaque to clients; the API server issues UUIDs. Only the separators of the list format are excluded.
	allowlistUID = regexp.MustCompile(`^[0-9A-Za-z-]{1,64}$`)
)

// WriteAllowlist is the set of Deployments, each with the UID it had when it was reviewed, that may be scaled.
// A nil allowlist restricts nothing (the default: writes are bounded by RBAC only).
type WriteAllowlist struct {
	uids map[types.NamespacedName]types.UID
}

// ParseWriteAllowlist reads "namespace/name=uid,namespace/name=uid". Every entry must be well formed and each
// Deployment may be listed once: a list that cannot be read exactly is refused, never partly applied.
func ParseWriteAllowlist(raw string) (*WriteAllowlist, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("the list is empty: name at least one Deployment, or unset %s to leave writes unrestricted", WriteAllowlistEnv)
	}
	parts := strings.Split(raw, ",")
	if len(parts) > maxWriteAllowlist {
		return nil, fmt.Errorf("%d entries, at most %d are allowed", len(parts), maxWriteAllowlist)
	}
	w := &WriteAllowlist{uids: make(map[types.NamespacedName]types.UID, len(parts))}
	for i, part := range parts {
		entry := strings.TrimSpace(part)
		target, uid, ok := strings.Cut(entry, "=")
		namespace, name, slash := strings.Cut(target, "/")
		switch {
		case !ok || !slash:
			return nil, fmt.Errorf("entry %d (%q) is not namespace/name=uid", i+1, entry)
		case !allowlistNamespace.MatchString(namespace):
			return nil, fmt.Errorf("entry %d: %q is not a namespace name", i+1, namespace)
		case len(name) > 253 || !allowlistName.MatchString(name):
			return nil, fmt.Errorf("entry %d: %q is not a Deployment name", i+1, name)
		case !allowlistUID.MatchString(uid):
			return nil, fmt.Errorf("entry %d: %q is not a UID", i+1, uid)
		}
		key := types.NamespacedName{Namespace: namespace, Name: name}
		if _, dup := w.uids[key]; dup {
			return nil, fmt.Errorf("entry %d: %s is listed more than once", i+1, key)
		}
		w.uids[key] = types.UID(uid)
	}
	return w, nil
}

// WriteAllowlistFromEnv returns nil (no restriction) when the variable is unset and an error when it is set but
// cannot be read, so a damaged list stops the operator instead of widening or silently removing its writes.
func WriteAllowlistFromEnv() (*WriteAllowlist, error) {
	raw, set := os.LookupEnv(WriteAllowlistEnv)
	if !set {
		return nil, nil
	}
	w, err := ParseWriteAllowlist(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", WriteAllowlistEnv, err)
	}
	return w, nil
}

// Len is the number of listed Deployments (0 for no restriction).
func (w *WriteAllowlist) Len() int {
	if w == nil {
		return 0
	}
	return len(w.uids)
}

// refusal returns why a write to this Deployment object is not allowed, or "" when it is. An empty UID is never
// allowed: the write could not be bound to the reviewed object.
func (w *WriteAllowlist) refusal(namespace, name string, uid types.UID) string {
	if w == nil {
		return ""
	}
	listed, ok := w.uids[types.NamespacedName{Namespace: namespace, Name: name}]
	switch {
	case !ok:
		return fmt.Sprintf("Deployment %s/%s is not in the operator's write allowlist", namespace, name)
	case uid == "" || uid != listed:
		return fmt.Sprintf("Deployment %s/%s has UID %q, the operator's write allowlist names UID %q: the Deployment "+
			"was replaced after it was reviewed", namespace, name, uid, listed)
	}
	return ""
}
