package nexus

import (
	"context"
	"errors"
	"testing"
)

// managers puts alice's manager edges: one set by hand, two reported by two
// directories, each edge naming its source.
func managers(t *testing.T) *MemStore {
	t.Helper()
	m := sampleGraph(t)
	for _, r := range []Relationship{
		{From: "alice", To: "bob", Type: "manager", Attrs: map[string]string{"source": "hand"}},
		{From: "alice", To: "carol", Type: "manager", Attrs: map[string]string{"source": "directory:entra"}},
		{From: "alice", To: "dave", Type: "manager", Attrs: map[string]string{"source": "directory:google"}},
	} {
		r := r
		if err := m.PutRelationship(ctx, &r); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

func source(s string) func(Relationship) bool {
	return func(r Relationship) bool { return r.Attrs["source"] == s }
}

// The rungs read through any GraphReader, the reader a View hands included, and a
// filtered rung decides by the edge's attribute: hand first, then the chosen
// directory, with the other source kept as a candidate.
func TestResolveInRunsTheLadderOverAReader(t *testing.T) {
	m := managers(t)
	ladder := []ReadRule{
		OutTargetsIn("manager", "hand", source("hand")),
		OutTargetsIn("manager", "directory:entra", source("directory:entra")),
		OutTargetsIn("manager", "directory:google", source("directory:google")),
	}
	var res Resolution
	err := m.View(ctx, func(r GraphReader) error {
		var err error
		res, err = ResolveIn(ctx, r, "alice", ladder...)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Resolved || res.Value != "bob" || res.Via != "out:manager|hand" || len(res.Candidates) != 3 {
		t.Fatalf("hand-set manager must decide with every source kept: %+v", res)
	}
	tie, err := ResolveIn(ctx, m, "alice", OutTargetsIn("manager", "directories", func(r Relationship) bool {
		return r.Attrs["source"] != "hand"
	}))
	if err != nil {
		t.Fatal(err)
	}
	if tie.Resolved || !tie.Ambiguous || tie.Value != "" {
		t.Fatalf("two answers at the deciding rung must be ambiguous: %+v", tie)
	}
}

// A set-valued resolution answers the deciding rung's whole set, never a guess
// and never Ambiguous, and a later rung only adds candidates.
func TestResolveSetAnswersTheDecidingRungsSet(t *testing.T) {
	m := managers(t)
	res, err := ResolveSet(ctx, m, "team-x",
		InSourcesIn("member", "members"),
		OutTargetsIn("in", ""))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Resolved || len(res.Values) != 1 || res.Values[0] != "alice" || res.Via != "in:member|members" || len(res.Candidates) != 2 {
		t.Fatalf("members of team-x: %+v", res)
	}
	all, err := ResolveSet(ctx, m, "alice", OutTargetsIn("owner", ""), OutTargetsIn("manager", ""))
	if err != nil {
		t.Fatal(err)
	}
	if !all.Resolved || len(all.Values) != 3 || all.Via != "out:manager" {
		t.Fatalf("a set rung after an empty one: %+v", all)
	}
	none, err := ResolveSet(ctx, m, "bob", OutTargetsIn("manager", ""), ViaIn("member", OutTargetsIn("owner", "")))
	if err != nil {
		t.Fatal(err)
	}
	if none.Resolved || none.Values != nil {
		t.Fatalf("nothing found must be unresolved: %+v", none)
	}
}

func TestReaderRungsFailClosed(t *testing.T) {
	m := sampleGraph(t)
	boom := errors.New("boom")
	failing := func(context.Context, GraphReader, string) ([]string, string, error) { return nil, "", boom }
	if _, err := ResolveSet(ctx, m, "alice", OutTargetsIn("owner", ""), failing); !errors.Is(err, boom) {
		t.Fatalf("a failing rung after an empty one: %v", err)
	}
	if _, err := ResolveIn(ctx, m, "alice", ViaIn("member", failing)); !errors.Is(err, boom) {
		t.Fatalf("a failing inner rung: %v", err)
	}
	res, err := ResolveIn(ctx, m, "alice", ViaIn("member", ViaIn("in", OutTargetsIn("owner", ""))))
	if err != nil || !res.Resolved || res.Value != "bob" || res.Via != "via:member|via:in|out:owner" {
		t.Fatalf("a transitive reader rung: %+v, %v", res, err)
	}
}
