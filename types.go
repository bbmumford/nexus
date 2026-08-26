// Package nexus is a scope-agnostic relationship + authorization graph. It stores opaque entities and
// opaque typed relationships and resolves questions like "who owns this / what role does X have"
// over the graph by a consumer-supplied precedence ladder — WITHOUT interpreting any axis. Entity and
// relationship types are opaque strings the consumer assigns ("user", "org", "team", "project";
// "member", "owner", "admin", "parent"); nexus knows graph mechanics, never what an "org" or "tenant"
// means. "users belong to orgs" and "users -> teams -> projects -> org/tenant" (arbitrary depth,
// overlapping paths) are the SAME graph, distinguished only by those opaque type strings. This is the
// object->relation->object model: no schema baked in, arbitrary relations the consumer defines.
//
// nexus is the agnostic core of an accounts/identity system: users/orgs/memberships/roles are one
// consumer schema over the generic graph. The concrete account features (email verify, invites) are a
// thin service layer over it; the core here stays axis-blind.
package nexus

// Entity is an opaque node. Type is a consumer-assigned string nexus never interprets; Attrs are
// opaque key/values nexus stores but never acts on.
type Entity struct {
	ID    string
	Type  string
	Attrs map[string]string
}

// Clone returns a deep copy so a caller mutation cannot alias stored state.
func (e *Entity) Clone() *Entity {
	if e == nil {
		return nil
	}
	cp := *e
	cp.Attrs = cloneAttrs(e.Attrs)
	return &cp
}

// Relationship is an opaque directed typed edge From one entity To another. Type is a consumer-assigned
// string nexus resolves over but never interprets. Many edges (any types) may connect the same pair;
// arbitrary depth and overlapping paths are normal, so (From, To, Type) is the edge identity.
type Relationship struct {
	From  string // entity id
	To    string // entity id
	Type  string
	Attrs map[string]string
}

// Clone returns a deep copy.
func (r *Relationship) Clone() *Relationship {
	if r == nil {
		return nil
	}
	cp := *r
	cp.Attrs = cloneAttrs(r.Attrs)
	return &cp
}

func cloneAttrs(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}
	cp := make(map[string]string, len(a))
	for k, v := range a {
		cp[k] = v
	}
	return cp
}
