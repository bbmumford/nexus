package nexus

import (
	"context"
	"errors"
	"testing"
)

var ctx = context.Background()

// build a graph: alice(user) is on team-x; team-x is in project-p; project-p is in org-o.
// direct owner edges: project-p owner = bob; org-o owner = carol.
func sampleGraph(t *testing.T) *MemStore {
	t.Helper()
	m := NewMemStore()
	for _, e := range []Entity{
		{ID: "alice", Type: "user"}, {ID: "bob", Type: "user"}, {ID: "carol", Type: "user"},
		{ID: "team-x", Type: "team"}, {ID: "project-p", Type: "project"}, {ID: "org-o", Type: "org"},
	} {
		e := e
		m.PutEntity(ctx, &e)
	}
	edges := []Relationship{
		{From: "alice", To: "team-x", Type: "member"},
		{From: "team-x", To: "project-p", Type: "in"},
		{From: "project-p", To: "org-o", Type: "in"},
		{From: "project-p", To: "bob", Type: "owner"},
		{From: "org-o", To: "carol", Type: "owner"},
	}
	for _, r := range edges {
		r := r
		m.PutRelationship(ctx, &r)
	}
	return m
}

func TestEntityAndEdgeRoundTrip(t *testing.T) {
	m := NewMemStore()
	m.PutEntity(ctx, &Entity{ID: "u1", Type: "user", Attrs: map[string]string{"email": "a@x"}})
	got, err := m.GetEntity(ctx, "u1")
	if err != nil || got.Type != "user" || got.Attrs["email"] != "a@x" {
		t.Fatalf("entity round-trip: %+v %v", got, err)
	}
	if _, err := m.GetEntity(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown entity must be ErrNotFound")
	}
	m.PutRelationship(ctx, &Relationship{From: "u1", To: "o1", Type: "member"})
	if ok, _ := m.HasRelationship(ctx, "u1", "o1", "member"); !ok {
		t.Fatal("edge must exist")
	}
	// deleting an entity removes incident edges
	m.DeleteEntity(ctx, "u1")
	if ok, _ := m.HasRelationship(ctx, "u1", "o1", "member"); ok {
		t.Fatal("deleting an entity must remove its edges")
	}
}

func TestResolve_DirectEdgeWins(t *testing.T) {
	m := sampleGraph(t)
	// "who owns project-p?" direct owner edge → bob (resolved, first rung).
	res, err := Resolve(ctx, m, "project-p",
		OutTargets("owner"),
		Via("in", OutTargets("owner")), // else via the org it is in
	)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Resolved || res.Value != "bob" || res.Via != "out:owner" {
		t.Fatalf("direct owner must win: %+v", res)
	}
}

func TestResolve_FallbackThroughArbitraryPath(t *testing.T) {
	m := sampleGraph(t)
	// "who owns alice?" no direct owner; fall back: via team→project→owner, else via team→project→org→owner.
	res, err := Resolve(ctx, m, "alice",
		OutTargets("owner"),                                      // none
		Via("member", Via("in", OutTargets("owner"))),            // alice→team→project→owner = bob
		Via("member", Via("in", Via("in", OutTargets("owner")))), // alice→team→project→org→owner = carol
	)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Resolved || res.Value != "bob" || res.Via == "" {
		t.Fatalf("fallback must resolve to bob via the team→project→owner path; got %+v", res)
	}
	// carol is retained as a candidate (the org-owner path), for transparency.
	foundCarol := false
	for _, c := range res.Candidates {
		if c.Value == "carol" {
			foundCarol = true
		}
	}
	if !foundCarol {
		t.Fatalf("the org-owner path must be retained as a candidate; got %+v", res.Candidates)
	}
}

func TestResolve_AmbiguousFirstRung(t *testing.T) {
	m := NewMemStore()
	m.PutEntity(ctx, &Entity{ID: "p", Type: "project"})
	// two direct owners → ambiguous, not "pick first".
	m.PutRelationship(ctx, &Relationship{From: "p", To: "bob", Type: "owner"})
	m.PutRelationship(ctx, &Relationship{From: "p", To: "carol", Type: "owner"})
	res, err := Resolve(ctx, m, "p", OutTargets("owner"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Resolved || !res.Ambiguous || res.Value != "" {
		t.Fatalf("two owners must be ambiguous (not pick-first): %+v", res)
	}
	if len(res.Candidates) != 2 {
		t.Fatalf("both owners must be candidates; got %+v", res.Candidates)
	}
}

func TestResolve_UnresolvedAndFailClosed(t *testing.T) {
	m := sampleGraph(t)
	// no "manager" edge anywhere → unresolved (not ambiguous, not resolved).
	res, _ := Resolve(ctx, m, "alice", OutTargets("manager"))
	if res.Resolved || res.Ambiguous || res.Value != "" {
		t.Fatalf("no match must be unresolved; got %+v", res)
	}
	// a rule error fails the whole resolution closed.
	boom := errors.New("boom")
	failing := func(context.Context, GraphStore, string) ([]string, string, error) { return nil, "", boom }
	if _, err := Resolve(ctx, m, "alice", OutTargets("owner"), failing); !errors.Is(err, boom) {
		t.Fatal("a rule error must fail the resolution closed")
	}
}
