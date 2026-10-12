package nexus

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
)

// fakeConnector hands database/sql one connection that never runs a statement.
// The tests below need only what reaches BeginTx: which options a store asks for,
// and what it answers when the connector refuses them.
type fakeConnector struct{ conn driver.Conn }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake: open by connector")
}

var errUnused = errors.New("fake: not used by these tests")

// plainConn has no BeginTx, so database/sql itself refuses a read-only or
// serializable request before the driver sees it.
type plainConn struct{}

func (plainConn) Prepare(string) (driver.Stmt, error) { return nil, errUnused }
func (plainConn) Close() error                        { return nil }
func (plainConn) Begin() (driver.Tx, error)           { return nil, errUnused }

// optsConn takes BeginTx options and answers with begin's error.
type optsConn struct {
	plainConn
	begin func(driver.TxOptions) error
}

func (c optsConn) BeginTx(_ context.Context, o driver.TxOptions) (driver.Tx, error) {
	return nil, c.begin(o)
}

func fakeStore(t *testing.T, c driver.Conn) *SQLStore {
	t.Helper()
	db := sql.OpenDB(fakeConnector{conn: c})
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLStore(db)
}

// callsNeedingATransaction are the store calls that must not run over separate
// statements: View, Update, and the floor-keeping delete.
func callsNeedingATransaction(s *SQLStore) map[string]func() error {
	return map[string]func() error{
		"View":   func() error { return s.View(ctx, func(GraphReader) error { return nil }) },
		"Update": func() error { return s.Update(ctx, func(TxWriter) error { return nil }) },
		"DeleteRelationshipKeepingMin": func() error {
			return s.DeleteRelationshipKeepingMin(ctx, "a", "t", "member", 1)
		},
	}
}

// A connector that refuses the options answers ErrNoTransaction, whichever layer
// refuses: database/sql for a driver without BeginTx options, or a driver that
// takes options and refuses them as the libSQL connector does when it was opened
// without immediate transactions.
func TestSQLStore_ARefusingConnectorAnswersErrNoTransaction(t *testing.T) {
	libsqlRefusal := func(o driver.TxOptions) error {
		if o.ReadOnly {
			return errors.New("read only transactions are not supported")
		}
		return errors.New("isolation level 6 is not supported")
	}
	for name, conn := range map[string]driver.Conn{
		"database/sql": plainConn{},
		"driver":       optsConn{begin: libsqlRefusal},
	} {
		for call, run := range callsNeedingATransaction(fakeStore(t, conn)) {
			if err := run(); !errors.Is(err, ErrNoTransaction) {
				t.Errorf("%s refusal, %s: err %v, want ErrNoTransaction", name, call, err)
			}
		}
	}
}

// Any other begin failure is returned as it is: a busy primary is not a connector
// that can never transact.
func TestSQLStore_ABusyBeginIsNotErrNoTransaction(t *testing.T) {
	busy := errors.New("database is locked")
	s := fakeStore(t, optsConn{begin: func(driver.TxOptions) error { return busy }})
	for call, run := range callsNeedingATransaction(s) {
		err := run()
		if !errors.Is(err, busy) || errors.Is(err, ErrNoTransaction) {
			t.Errorf("%s: err %v, want the driver's busy error and not ErrNoTransaction", call, err)
		}
	}
}

// View asks for a read-only transaction; Update and the floor-keeping delete ask
// for a serializable one by name, so a connector that would default to a deferred
// transaction refuses rather than open one.
func TestSQLStore_TransactionsNameTheirOptions(t *testing.T) {
	var mu sync.Mutex
	var got driver.TxOptions
	recorded := errors.New("fake: recorded")
	s := fakeStore(t, optsConn{begin: func(o driver.TxOptions) error {
		mu.Lock()
		got = o
		mu.Unlock()
		return recorded
	}})
	want := map[string]driver.TxOptions{
		"View":                         {ReadOnly: true},
		"Update":                       {Isolation: driver.IsolationLevel(sql.LevelSerializable)},
		"DeleteRelationshipKeepingMin": {Isolation: driver.IsolationLevel(sql.LevelSerializable)},
	}
	for call, run := range callsNeedingATransaction(s) {
		mu.Lock()
		got = driver.TxOptions{Isolation: -1}
		mu.Unlock()
		if err := run(); !errors.Is(err, recorded) {
			t.Fatalf("%s: err %v, want the recording fake's", call, err)
		}
		mu.Lock()
		if got != want[call] {
			t.Errorf("%s opened %+v, want %+v", call, got, want[call])
		}
		mu.Unlock()
	}
}
