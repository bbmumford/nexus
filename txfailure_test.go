package nexus

import (
	"errors"
	"testing"
)

// A write that fails inside Update fails the whole transaction even when the
// function drops its error and returns nil: deleteEntity's entity delete ran
// before its edge delete was refused, and committing would leave the entity gone
// and its edges in place.
func TestUpdate_ADroppedWriteErrorStillRollsBack(t *testing.T) {
	s := fileSQLStore(t)
	_ = s.PutEntity(ctx, &Entity{ID: "r1", Type: "role"})
	mustPut(t, s, Relationship{From: "s1", To: "r1", Type: "assigned"})
	if _, err := s.DB().ExecContext(ctx, `CREATE TRIGGER refuseEdgeDelete BEFORE DELETE ON nexusEdges
		BEGIN SELECT RAISE(ABORT, 'edge delete refused'); END`); err != nil {
		t.Fatal(err)
	}
	var dropped, later error
	err := s.Update(ctx, func(w TxWriter) error {
		dropped = w.DeleteEntity(ctx, "r1")
		later = w.PutEntity(ctx, &Entity{ID: "r2", Type: "role"})
		return nil
	})
	if dropped == nil {
		t.Fatal("the refused edge delete reported no error")
	}
	if err == nil || err.Error() != dropped.Error() {
		t.Fatalf("Update returned %v, want the dropped write's error %v", err, dropped)
	}
	if later == nil || later.Error() != dropped.Error() {
		t.Fatalf("a write after the failure answered %v, want %v", later, dropped)
	}
	if _, err := s.GetEntity(ctx, "r1"); err != nil {
		t.Fatalf("the entity delete committed although its edge delete failed: %v", err)
	}
	if _, err := s.GetEntity(ctx, "r2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a write after the failure committed: %v", err)
	}
}

// A refusal decided before anything is written does not fail the transaction: a
// function may catch the floor or a missing entity and commit its other writes.
func TestUpdate_ACaughtRefusalStillCommits(t *testing.T) {
	eachTxStore(t, func(t *testing.T, s TxStore) {
		mustPut(t, s, Relationship{From: "s1", To: "p", Type: "member"})
		err := s.Update(ctx, func(w TxWriter) error {
			if err := w.DeleteRelationshipKeepingMin(ctx, "s1", "p", "member", 1); !errors.Is(err, ErrCardinalityFloor) {
				t.Errorf("floor: %v", err)
			}
			if err := w.DeleteEntity(ctx, "absent"); !errors.Is(err, ErrNotFound) {
				t.Errorf("absent entity: %v", err)
			}
			if _, err := w.DeleteEdgesWhere(ctx, EdgeSelector{Type: "member"}); !errors.Is(err, ErrInvalidSelector) {
				t.Errorf("selector: %v", err)
			}
			return w.PutEntity(ctx, &Entity{ID: "r1", Type: "role"})
		})
		if err != nil {
			t.Fatalf("Update after caught refusals: %v", err)
		}
		if _, err := s.GetEntity(ctx, "r1"); err != nil {
			t.Fatalf("the write after caught refusals did not commit: %v", err)
		}
	})
}

// A selector attribute the SQL store would match differently from MemStore is
// refused by both: a backslash in a key is a JSON path escape there, and bytes that
// are not UTF-8 are replaced when attributes are stored as JSON.
func TestTxWriter_DeleteEdgesWhereRefusesAnAttributeTheStoresMatchDifferently(t *testing.T) {
	escaped := "a" + `\` + "u0062"
	eachTxStore(t, func(t *testing.T, s TxStore) {
		mustPut(t, s,
			Relationship{From: "s1", To: "t1", Type: "e", Attrs: map[string]string{"ab": "v"}},
			Relationship{From: "s1", To: "t2", Type: "e", Attrs: map[string]string{escaped: "v", "k": "\xff"}},
		)
		for name, attrs := range map[string]map[string]string{
			"escaped key":     {escaped: "v"},
			"trailing escape": {"a" + `\`: "v"},
			"non-UTF-8 key":   {"k\xff": "v"},
			"non-UTF-8 value": {"k": "\xff"},
		} {
			var keys []EdgeKey
			err := s.Update(ctx, func(w TxWriter) error {
				var err error
				keys, err = w.DeleteEdgesWhere(ctx, EdgeSelector{From: "s1", Type: "e", Attrs: attrs})
				return err
			})
			if !errors.Is(err, ErrInvalidSelector) {
				t.Errorf("%s: err %v, removed %v, want ErrInvalidSelector", name, err, keys)
			}
		}
		if n := outCount(t, s, "s1", "e"); n != 2 {
			t.Fatalf("a refused selector removed edges: %d left, want 2", n)
		}
	})
}
