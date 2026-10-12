package nexus

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

// fileSQLStore opens a SQLStore on a file database in WAL mode, with every writing
// transaction opened BEGIN IMMEDIATE, which is how the libSQL replicas this store
// serves in production open them. A file, not ":memory:", because these tests hold
// a reader and a writer on two connections at once.
func fileSQLStore(t *testing.T) *SQLStore {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "nexus.db") +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(20000)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := NewSQLStore(db)
	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return s
}

// eachTxStore runs one contract over the reference store and the SQL store, since
// the reference is what certifies the durable store's behaviour.
func eachTxStore(t *testing.T, run func(t *testing.T, s TxStore)) {
	t.Run("mem", func(t *testing.T) { run(t, NewMemStore()) })
	t.Run("sql", func(t *testing.T) { run(t, fileSQLStore(t)) })
}

func mustPut(t *testing.T, s GraphStore, rels ...Relationship) {
	t.Helper()
	for _, r := range rels {
		r := r
		if err := s.PutRelationship(ctx, &r); err != nil {
			t.Fatal(err)
		}
	}
}

func outCount(t *testing.T, r GraphReader, from string, types ...string) int {
	t.Helper()
	got, err := r.OutEdges(ctx, from, types...)
	if err != nil {
		t.Fatal(err)
	}
	return len(got)
}

var errAbort = errors.New("abort")

func TestUpdate_CommitsEveryWriteAndReadsItsOwn(t *testing.T) {
	eachTxStore(t, func(t *testing.T, s TxStore) {
		err := s.Update(ctx, func(w TxWriter) error {
			if err := w.PutEntity(ctx, &Entity{ID: "r1", Type: "role"}); err != nil {
				return err
			}
			if err := w.PutRelationship(ctx, &Relationship{From: "s1", To: "r1", Type: "assigned"}); err != nil {
				return err
			}
			if n := outCount(t, w, "s1", "assigned"); n != 1 {
				return fmt.Errorf("a read inside Update sees %d of its own edges, want 1", n)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetEntity(ctx, "r1"); err != nil {
			t.Fatalf("the committed entity: %v", err)
		}
		if n := outCount(t, s, "s1", "assigned"); n != 1 {
			t.Fatalf("committed edges: %d, want 1", n)
		}
	})
}

func TestUpdate_AnErrorRollsEveryWriteBack(t *testing.T) {
	eachTxStore(t, func(t *testing.T, s TxStore) {
		mustPut(t, s, Relationship{From: "s1", To: "p", Type: "member"})
		err := s.Update(ctx, func(w TxWriter) error {
			_ = w.PutEntity(ctx, &Entity{ID: "r1", Type: "role"})
			_ = w.PutRelationship(ctx, &Relationship{From: "s1", To: "r1", Type: "assigned"})
			_ = w.DeleteRelationship(ctx, "s1", "p", "member")
			return errAbort
		})
		if !errors.Is(err, errAbort) {
			t.Fatalf("Update returned %v, want the function's error", err)
		}
		if _, err := s.GetEntity(ctx, "r1"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("a rolled-back entity is still stored: %v", err)
		}
		if n := outCount(t, s, "s1", "assigned"); n != 0 {
			t.Fatalf("a rolled-back edge is still stored: %d", n)
		}
		if ok, _ := s.HasRelationship(ctx, "s1", "p", "member"); !ok {
			t.Fatal("a rolled-back delete removed the edge")
		}
	})
}

// A View reads one snapshot: a commit landing between two of its reads is seen by
// neither, and is seen by the next read outside it.
func TestView_DoesNotSeeACommitMadeDuringIt(t *testing.T) {
	eachTxStore(t, func(t *testing.T, s TxStore) {
		mustPut(t, s, Relationship{From: "s1", To: "p1", Type: "member"})
		err := s.View(ctx, func(r GraphReader) error {
			if n := outCount(t, r, "s1"); n != 1 {
				return fmt.Errorf("first read: %d edges, want 1", n)
			}
			if err := s.Update(ctx, func(w TxWriter) error {
				return w.PutRelationship(ctx, &Relationship{From: "s1", To: "p2", Type: "member"})
			}); err != nil {
				return fmt.Errorf("the concurrent commit: %w", err)
			}
			if n := outCount(t, r, "s1"); n != 1 {
				return fmt.Errorf("a read after a concurrent commit sees %d edges; the snapshot held 1", n)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if n := outCount(t, s, "s1"); n != 2 {
			t.Fatalf("after the View: %d edges, want 2", n)
		}
	})
}

// Every Update reads a count and writes the edge that count names. Run at once,
// they commit one after another only if each reads after the previous one
// committed: a lost update writes one key twice and leaves fewer edges.
func TestUpdate_ReadThenWriteSerialises(t *testing.T) {
	const n = 24
	eachTxStore(t, func(t *testing.T, s TxStore) {
		var wg sync.WaitGroup
		errs := make(chan error, n)
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs <- s.Update(ctx, func(w TxWriter) error {
					in, err := w.InEdges(ctx, "ctr", "tick")
					if err != nil {
						return err
					}
					return w.PutRelationship(ctx, &Relationship{From: fmt.Sprintf("t%03d", len(in)), To: "ctr", Type: "tick"})
				})
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("an Update failed: %v", err)
			}
		}
		in, err := s.InEdges(ctx, "ctr", "tick")
		if err != nil {
			t.Fatal(err)
		}
		if len(in) != n {
			t.Fatalf("%d edges after %d serialised read-then-write Updates; an update was lost", len(in), n)
		}
	})
}

func TestTxWriter_DeleteEntityTakesItsEdgesInTheSameTransaction(t *testing.T) {
	eachTxStore(t, func(t *testing.T, s TxStore) {
		_ = s.PutEntity(ctx, &Entity{ID: "r1", Type: "role"})
		mustPut(t, s,
			Relationship{From: "s1", To: "r1", Type: "assigned"},
			Relationship{From: "r1", To: "p", Type: "in"},
			Relationship{From: "s1", To: "p", Type: "member"})
		// Rolled back: the entity and both incident edges stay.
		_ = s.Update(ctx, func(w TxWriter) error {
			if err := w.DeleteEntity(ctx, "r1"); err != nil {
				return err
			}
			if n := outCount(t, w, "s1", "assigned"); n != 0 {
				return fmt.Errorf("inside the transaction the deleted entity still has %d incoming edges", n)
			}
			return errAbort
		})
		if _, err := s.GetEntity(ctx, "r1"); err != nil {
			t.Fatalf("a rolled-back DeleteEntity removed the entity: %v", err)
		}
		if outCount(t, s, "s1", "assigned") != 1 || outCount(t, s, "r1", "in") != 1 {
			t.Fatal("a rolled-back DeleteEntity removed an incident edge")
		}
		// Committed: both go, the unrelated edge stays.
		if err := s.Update(ctx, func(w TxWriter) error { return w.DeleteEntity(ctx, "r1") }); err != nil {
			t.Fatal(err)
		}
		if outCount(t, s, "s1", "assigned") != 0 || outCount(t, s, "r1", "in") != 0 {
			t.Fatal("a committed DeleteEntity left an incident edge")
		}
		if outCount(t, s, "s1", "member") != 1 {
			t.Fatal("DeleteEntity removed an edge not incident to it")
		}
		if err := s.Update(ctx, func(w TxWriter) error { return w.DeleteEntity(ctx, "r1") }); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleting an unknown entity: %v, want ErrNotFound", err)
		}
	})
}

func TestTxWriter_DeleteEdgesWhereRemovesExactlyTheSelectedSet(t *testing.T) {
	eachTxStore(t, func(t *testing.T, s TxStore) {
		inst := map[string]string{"partition": "P", "membership_id": "m1"}
		mustPut(t, s,
			Relationship{From: "s1", To: "rb", Type: "assigned", Attrs: inst},
			Relationship{From: "s1", To: "ra", Type: "assigned", Attrs: map[string]string{"partition": "P", "membership_id": "m1", "extra": "x"}},
			Relationship{From: "s1", To: "rc", Type: "assigned", Attrs: map[string]string{"partition": "P", "membership_id": "m0"}},
			Relationship{From: "s1", To: "rd", Type: "assigned", Attrs: map[string]string{"partition": "Q", "membership_id": "m1"}},
			Relationship{From: "s1", To: "re", Type: "assigned", Attrs: map[string]string{"partition": "P"}},
			Relationship{From: "s1", To: "rf", Type: "other", Attrs: inst},
			Relationship{From: "s2", To: "rb", Type: "assigned", Attrs: inst},
		)
		var keys []EdgeKey
		err := s.Update(ctx, func(w TxWriter) error {
			var err error
			keys, err = w.DeleteEdgesWhere(ctx, EdgeSelector{From: "s1", Type: "assigned", Attrs: inst})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		want := []EdgeKey{{"s1", "ra", "assigned"}, {"s1", "rb", "assigned"}}
		if fmt.Sprint(keys) != fmt.Sprint(want) {
			t.Fatalf("removed keys %v, want %v", keys, want)
		}
		left, _ := s.OutEdges(ctx, "s1")
		if len(left) != 4 {
			t.Fatalf("%d edges left from s1, want 4 (other instance, other partition, absent attribute, other type): %v", len(left), left)
		}
		if ok, _ := s.HasRelationship(ctx, "s2", "rb", "assigned"); !ok {
			t.Fatal("an edge from another subject was removed")
		}

		// By target: every edge of the type entering rb.
		mustPut(t, s, Relationship{From: "s3", To: "rb", Type: "assigned"})
		err = s.Update(ctx, func(w TxWriter) error {
			var err error
			keys, err = w.DeleteEdgesWhere(ctx, EdgeSelector{To: "rb", Type: "assigned"})
			return err
		})
		if err != nil || fmt.Sprint(keys) != fmt.Sprint([]EdgeKey{{"s2", "rb", "assigned"}, {"s3", "rb", "assigned"}}) {
			t.Fatalf("by target: keys %v err %v", keys, err)
		}

		// An absent attribute matches no value, the empty one included.
		err = s.Update(ctx, func(w TxWriter) error {
			var err error
			keys, err = w.DeleteEdgesWhere(ctx, EdgeSelector{From: "s1", Type: "assigned", Attrs: map[string]string{"membership_id": ""}})
			return err
		})
		if err != nil || len(keys) != 0 {
			t.Fatalf("an empty value matched an absent attribute: keys %v err %v", keys, err)
		}
	})
}

func TestTxWriter_DeleteEdgesWhereRefusesAnUnboundedSelector(t *testing.T) {
	eachTxStore(t, func(t *testing.T, s TxStore) {
		mustPut(t, s, Relationship{From: "s1", To: "r1", Type: "assigned"})
		for name, sel := range map[string]EdgeSelector{
			"no endpoint":     {Type: "assigned"},
			"both endpoints":  {From: "s1", To: "r1", Type: "assigned"},
			"no type":         {From: "s1"},
			"empty key":       {From: "s1", Type: "assigned", Attrs: map[string]string{"": "x"}},
			"key with quote":  {From: "s1", Type: "assigned", Attrs: map[string]string{`a"b`: "x"}},
			"empty selector":  {},
			"type and target": {To: "r1"},
		} {
			err := s.Update(ctx, func(w TxWriter) error {
				_, err := w.DeleteEdgesWhere(ctx, sel)
				return err
			})
			if !errors.Is(err, ErrInvalidSelector) {
				t.Errorf("%s: err %v, want ErrInvalidSelector", name, err)
			}
		}
		if ok, _ := s.HasRelationship(ctx, "s1", "r1", "assigned"); !ok {
			t.Fatal("a refused selector removed an edge")
		}
	})
}

// The store's own DeleteEntity is one transaction too: when the edge delete fails,
// the entity delete before it is rolled back rather than left committed.
func TestSQLStore_DeleteEntityIsOneTransaction(t *testing.T) {
	s := fileSQLStore(t)
	_ = s.PutEntity(ctx, &Entity{ID: "r1", Type: "role"})
	mustPut(t, s, Relationship{From: "s1", To: "r1", Type: "assigned"})
	if _, err := s.DB().ExecContext(ctx, `CREATE TRIGGER refuse_edge_delete BEFORE DELETE ON nexus_edges
		BEGIN SELECT RAISE(ABORT, 'edge delete refused'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteEntity(ctx, "r1"); err == nil {
		t.Fatal("DeleteEntity succeeded although its edge delete was refused")
	}
	if _, err := s.GetEntity(ctx, "r1"); err != nil {
		t.Fatalf("the entity was deleted although its edges were not: %v", err)
	}
}
