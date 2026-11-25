package nexus

import (
	"database/sql"
	"errors"
	"testing"

	_ "modernc.org/sqlite"
)

func newSQLStore(t *testing.T) *SQLStore {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1) // keep the :memory: DB on one connection
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close: %v", err)
		}
	})
	s := NewSQLStore(db)
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return s
}

// seed the same graph the MemStore test uses, over any GraphStore.
func seedSample(t *testing.T, g GraphStore) {
	t.Helper()
	for _, e := range []Entity{
		{ID: "alice", Type: "user"}, {ID: "bob", Type: "user"}, {ID: "carol", Type: "user"},
		{ID: "team-x", Type: "team"}, {ID: "project-p", Type: "project"}, {ID: "org-o", Type: "org"},
	} {
		e := e
		if err := g.PutEntity(ctx, &e); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range []Relationship{
		{From: "alice", To: "team-x", Type: "member"},
		{From: "team-x", To: "project-p", Type: "in"},
		{From: "project-p", To: "org-o", Type: "in"},
		{From: "project-p", To: "bob", Type: "owner"},
		{From: "org-o", To: "carol", Type: "owner"},
	} {
		r := r
		if err := g.PutRelationship(ctx, &r); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLStore_EntityEdgeRoundTripAndFilter(t *testing.T) {
	s := newSQLStore(t)
	if err := s.PutEntity(ctx, &Entity{ID: "u1", Type: "user", Attrs: map[string]string{"email": "a@x"}}); err != nil {
		t.Fatalf("s.PutEntity: %v", err)
	}
	got, err := s.GetEntity(ctx, "u1")
	if err != nil || got.Type != "user" || got.Attrs["email"] != "a@x" {
		t.Fatalf("entity round-trip incl attrs: %+v %v", got, err)
	}
	if _, err := s.GetEntity(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown entity must be ErrNotFound")
	}
	if err := s.PutRelationship(ctx, &Relationship{From: "u1", To: "o1", Type: "member"}); err != nil {
		t.Fatalf("s.PutRelationship: %v", err)
	}
	if err := s.PutRelationship(ctx, &Relationship{From: "u1", To: "t1", Type: "lead"}); err != nil {
		t.Fatalf("s.PutRelationship: %v", err)
	}
	// OutEdges type filter
	if got, _ := s.OutEdges(ctx, "u1", "member"); len(got) != 1 || got[0].To != "o1" {
		t.Fatalf("OutEdges type filter: %+v", got)
	}
	if got, _ := s.OutEdges(ctx, "u1"); len(got) != 2 {
		t.Fatalf("OutEdges no filter must return all; got %d", len(got))
	}
	if got, _ := s.InEdges(ctx, "o1", "member"); len(got) != 1 {
		t.Fatalf("InEdges: %+v", got)
	}
	// delete-cascade
	if err := s.DeleteEntity(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.HasRelationship(ctx, "u1", "o1", "member"); ok {
		t.Fatal("deleting an entity must remove its edges")
	}
	if err := s.DeleteEntity(ctx, "u1"); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleting an unknown entity must be ErrNotFound")
	}
}

func TestSQLStore_ResolveMatchesReference(t *testing.T) {
	s := newSQLStore(t)
	seedSample(t, s)
	// the SAME store-agnostic Resolve runs over the *sql.DB store: arbitrary-path fallback resolves
	// alice's owner to bob via team→project→owner, with carol (org-owner path) retained as candidate.
	res, err := Resolve(ctx, s, "alice",
		OutTargets("owner"),
		Via("member", Via("in", OutTargets("owner"))),
		Via("member", Via("in", Via("in", OutTargets("owner")))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Resolved || res.Value != "bob" {
		t.Fatalf("resolver over the SQL store must resolve to bob; got %+v", res)
	}
	foundCarol := false
	for _, c := range res.Candidates {
		if c.Value == "carol" {
			foundCarol = true
		}
	}
	if !foundCarol {
		t.Fatalf("org-owner path must be a candidate; got %+v", res.Candidates)
	}
	// ListEntitiesByType
	if users, _ := s.ListEntitiesByType(ctx, "user"); len(users) != 3 {
		t.Fatalf("ListEntitiesByType(user) = %d, want 3", len(users))
	}
}
