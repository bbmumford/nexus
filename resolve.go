package nexus

import "context"

// Resolution is the structured result of a precedence resolution: the winning value with its
// provenance, every candidate seen, and whether the outcome was ambiguous or unresolved. Callers get
// a definite auditable answer — never a silent "pick first" guess.
type Resolution struct {
	Value      string      // resolved entity id (the owner/role target); "" when unresolved or ambiguous
	Via        string      // provenance: which rung produced Value
	Resolved   bool        // true when exactly one value won
	Ambiguous  bool        // true when the first rung that matched yielded >1 distinct value
	Candidates []Candidate // every value any rung produced, in order (for a human to disambiguate)
}

// Candidate is one produced value + the rung it came from.
type Candidate struct {
	Value string
	Via   string
}

// Rule is one rung of a precedence ladder: given the subject entity id and the graph, it yields the
// candidate values (entity ids) at that rung and a provenance label, or no values. Consumer-defined so
// the graph stays axis-blind — the consumer expresses "owner = the entity at the end of an 'owner'
// edge, else via the subject's 'team' then that team's 'owner', else via 'org'". An errored rule stops
// resolution (fail-closed) — never a silent skip.
type Rule func(ctx context.Context, g GraphStore, subject string) (values []string, via string, err error)

// Resolve runs the rules in precedence order over the graph. The FIRST rung that yields any values
// decides the outcome: exactly one distinct value -> Resolved with that Value + Via; more than one ->
// Ambiguous (a human disambiguates from Candidates). Later rungs never override a decided outcome
// (first-hit-wins), but every rung's values are retained as Candidates for transparency. No rung
// yielding a value -> unresolved (Resolved=false, Ambiguous=false). Any rule error fails the whole
// resolution closed.
func Resolve(ctx context.Context, g GraphStore, subject string, rules ...Rule) (Resolution, error) {
	read := make([]ReadRule, len(rules))
	for i, rule := range rules {
		read[i] = func(ctx context.Context, _ GraphReader, subject string) ([]string, string, error) {
			return rule(ctx, g, subject)
		}
	}
	return ResolveIn(ctx, g, subject, read...)
}

// OutTargets is the simplest rung: the To-entity-ids of the subject's out-edges of edgeType
// ("owner = the entity at the end of an 'owner' edge"). Provenance is "out:<edgeType>".
func OutTargets(edgeType string) Rule {
	return func(ctx context.Context, g GraphStore, subject string) ([]string, string, error) {
		edges, err := g.OutEdges(ctx, subject, edgeType)
		if err != nil {
			return nil, "", err
		}
		out := make([]string, 0, len(edges))
		for _, e := range edges {
			out = append(out, e.To)
		}
		return out, "out:" + edgeType, nil
	}
}

// Via applies inner AFTER hopping the subject's out-edges of hopType — a transitive rung
// ("via the subject's 'team', then that team's 'owner'"). Arbitrary depth composes: Via("org",
// Via("team", OutTargets("owner"))). Provenance is "via:<hopType>|<inner-via>".
func Via(hopType string, inner Rule) Rule {
	return func(ctx context.Context, g GraphStore, subject string) ([]string, string, error) {
		hops, err := g.OutEdges(ctx, subject, hopType)
		if err != nil {
			return nil, "", err
		}
		var out []string
		innerVia := ""
		for _, h := range hops {
			vals, iv, err := inner(ctx, g, h.To)
			if err != nil {
				return nil, "", err
			}
			innerVia = iv
			out = append(out, vals...)
		}
		return out, "via:" + hopType + "|" + innerVia, nil
	}
}

func dedupe(in []string) []string {
	if len(in) < 2 {
		return in
	}
	seen := make(map[string]struct{}, len(in))
	out := in[:0:0]
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
