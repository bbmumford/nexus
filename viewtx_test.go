package nexus

import (
	"fmt"
	"testing"
)

// The SQL store's View hands a *SQLTxReader whose transaction reads a table on the
// same database at the snapshot the edges are read at: a commit landing during the
// View changes neither the edges nor that table's row as the View reads them, and
// both are seen after it.
func TestSQLStoreView_AnswersTheTransactionItReadsIn(t *testing.T) {
	s := fileSQLStore(t)
	if _, err := s.DB().ExecContext(ctx, `CREATE TABLE side (k TEXT PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create side table: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO side (k, v) VALUES ('a', 'before')`); err != nil {
		t.Fatalf("seed side table: %v", err)
	}
	mustPut(t, s, Relationship{From: "s1", To: "p1", Type: "member"})

	side := func(r *SQLTxReader) (string, error) {
		var v string
		err := r.Tx().QueryRowContext(ctx, `SELECT v FROM side WHERE k = 'a'`).Scan(&v)
		return v, err
	}
	err := s.View(ctx, func(r GraphReader) error {
		sr, ok := r.(*SQLTxReader)
		if !ok {
			return fmt.Errorf("View handed %T, want *SQLTxReader", r)
		}
		if n := outCount(t, sr, "s1"); n != 1 {
			return fmt.Errorf("first edge read: %d, want 1", n)
		}
		if v, err := side(sr); err != nil || v != "before" {
			return fmt.Errorf("first side read: %q (err %v), want before", v, err)
		}
		if _, err := s.DB().ExecContext(ctx, `UPDATE side SET v = 'after' WHERE k = 'a'`); err != nil {
			return fmt.Errorf("the concurrent side write: %w", err)
		}
		if err := s.Update(ctx, func(w TxWriter) error {
			return w.PutRelationship(ctx, &Relationship{From: "s1", To: "p2", Type: "member"})
		}); err != nil {
			return fmt.Errorf("the concurrent edge commit: %w", err)
		}
		if n := outCount(t, sr, "s1"); n != 1 {
			return fmt.Errorf("an edge read after a concurrent commit sees %d edges; the snapshot held 1", n)
		}
		if v, err := side(sr); err != nil || v != "before" {
			return fmt.Errorf("a side read after a concurrent commit sees %q (err %v); the snapshot held before", v, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var v string
	if err := s.DB().QueryRowContext(ctx, `SELECT v FROM side WHERE k = 'a'`).Scan(&v); err != nil || v != "after" {
		t.Fatalf("after the View the side row reads %q (err %v), want after", v, err)
	}
	if n := outCount(t, s, "s1"); n != 2 {
		t.Fatalf("after the View: %d edges, want 2", n)
	}
}

// The in-memory store has no SQL transaction, so its View hands a reader that is
// not a *SQLTxReader.
func TestMemStoreView_HandsNoSQLTxReader(t *testing.T) {
	err := NewMemStore().View(ctx, func(r GraphReader) error {
		if _, ok := r.(*SQLTxReader); ok {
			return fmt.Errorf("the in-memory View handed a SQL reader")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
