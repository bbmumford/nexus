package nexus

import "context"

// ReadRule is one rung of a precedence ladder over any GraphReader: a store, the
// reader a View hands its function, or a snapshot. A resolver that must read
// inside one transaction (the reader View passes) runs the same ladder a store
// runs, which a Rule cannot, since a Rule asks for the whole GraphStore.
type ReadRule func(ctx context.Context, g GraphReader, subject string) (values []string, via string, err error)

// ResolveIn is Resolve over a GraphReader: the first rung that yields any values
// decides, one distinct value resolving and several being ambiguous; every rung's
// values stay as Candidates; any rung error fails the whole resolution closed.
func ResolveIn(ctx context.Context, g GraphReader, subject string, rules ...ReadRule) (Resolution, error) {
	var res Resolution
	decided := false
	for _, rule := range rules {
		vals, via, err := rule(ctx, g, subject)
		if err != nil {
			return Resolution{}, err
		}
		vals = dedupe(vals)
		for _, v := range vals {
			res.Candidates = append(res.Candidates, Candidate{Value: v, Via: via})
		}
		if !decided && len(vals) > 0 {
			decided = true
			if len(vals) == 1 {
				res.Value, res.Via, res.Resolved = vals[0], via, true
			} else {
				res.Ambiguous = true
			}
		}
	}
	return res, nil
}

// SetResolution is a set-valued precedence resolution: the deciding rung's whole
// set of values, for a question whose answer is several entities (every approver a
// step selects) rather than one.
type SetResolution struct {
	Values     []string    // the deciding rung's distinct values; nil when unresolved
	Via        string      // provenance: which rung decided
	Resolved   bool        // true when some rung yielded a value
	Candidates []Candidate // every value any rung produced, in order
}

// ResolveSet runs the rules in precedence order: the first rung that yields any
// values decides, and all of its distinct values are the answer. Later rungs never
// override a decided outcome, but their values are kept as Candidates. Any rung
// error fails the whole resolution closed.
func ResolveSet(ctx context.Context, g GraphReader, subject string, rules ...ReadRule) (SetResolution, error) {
	var res SetResolution
	for _, rule := range rules {
		vals, via, err := rule(ctx, g, subject)
		if err != nil {
			return SetResolution{}, err
		}
		vals = dedupe(vals)
		for _, v := range vals {
			res.Candidates = append(res.Candidates, Candidate{Value: v, Via: via})
		}
		if !res.Resolved && len(vals) > 0 {
			res.Values, res.Via, res.Resolved = vals, via, true
		}
	}
	return res, nil
}

// OutTargetsIn is OutTargets over a GraphReader, keeping only the edges every keep
// predicate accepts (an edge attribute such as the source that wrote it). Its
// provenance is "out:<edgeType>", with label appended after "|" when set, so two
// rungs over one edge type that filter differently read apart.
func OutTargetsIn(edgeType, label string, keep ...func(Relationship) bool) ReadRule {
	via := "out:" + edgeType
	if label != "" {
		via += "|" + label
	}
	return func(ctx context.Context, g GraphReader, subject string) ([]string, string, error) {
		edges, err := g.OutEdges(ctx, subject, edgeType)
		if err != nil {
			return nil, "", err
		}
		out := make([]string, 0, len(edges))
	next:
		for _, e := range edges {
			for _, k := range keep {
				if !k(e) {
					continue next
				}
			}
			out = append(out, e.To)
		}
		return out, via, nil
	}
}

// InSourcesIn is the reverse rung over a GraphReader: the From-entity-ids of the
// subject's in-edges of edgeType that every keep predicate accepts ("the members
// of a group" when membership edges point at the group). Provenance is
// "in:<edgeType>", with label appended as OutTargetsIn does.
func InSourcesIn(edgeType, label string, keep ...func(Relationship) bool) ReadRule {
	via := "in:" + edgeType
	if label != "" {
		via += "|" + label
	}
	return func(ctx context.Context, g GraphReader, subject string) ([]string, string, error) {
		edges, err := g.InEdges(ctx, subject, edgeType)
		if err != nil {
			return nil, "", err
		}
		out := make([]string, 0, len(edges))
	next:
		for _, e := range edges {
			for _, k := range keep {
				if !k(e) {
					continue next
				}
			}
			out = append(out, e.From)
		}
		return out, via, nil
	}
}

// ViaIn is Via over a GraphReader: inner runs from every entity at the end of the
// subject's out-edges of hopType. Provenance is "via:<hopType>|<inner-via>".
func ViaIn(hopType string, inner ReadRule) ReadRule {
	return func(ctx context.Context, g GraphReader, subject string) ([]string, string, error) {
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
