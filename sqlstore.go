package nexus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// SQLStore is the durable default GraphStore over any database/sql driver — the shipped standalone
// store. The package imports only database/sql (stdlib); the APP supplies the opened *sql.DB and
// registers the driver (go-libsql/Turso, modernc, Postgres), so nexus pulls no driver dependency yet
// works out of the box. It interprets nothing: entity id/type/attrs and edge from/to/type/attrs are
// opaque columns (attrs as JSON). The GraphStore interface remains the escape-hatch for a mesh-gateway
// or in-memory (MemStore) backend.
type SQLStore struct{ db *sql.DB }

// NewSQLStore builds a store over an opened *sql.DB. Call InitSchema once before use.
func NewSQLStore(db *sql.DB) *SQLStore { return &SQLStore{db: db} }

var (
	_ GraphStore = (*SQLStore)(nil)
	_ TxStore    = (*SQLStore)(nil)
)

// DB returns the database the store reads and writes. census compares it with its
// transfer log's database before it records an event in a transaction Update
// opened, because an event can share that transaction only on the same database.
func (s *SQLStore) DB() *sql.DB { return s.db }

// querier is what every statement below runs on: the *sql.DB for a store method,
// or the *sql.Tx View or Update opened.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// InitSchema creates the entity/edge tables + indexes (idempotent); run once at boot.
func (s *SQLStore) InitSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS nexusEntities (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL DEFAULT '',
			attrs TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE INDEX IF NOT EXISTS idxNexusEntitiesType ON nexusEntities(type)`,
		`CREATE TABLE IF NOT EXISTS nexusEdges (
			fromId TEXT NOT NULL,
			toId TEXT NOT NULL,
			type TEXT NOT NULL,
			attrs TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (fromId, toId, type)
		)`,
		`CREATE INDEX IF NOT EXISTS idxNexusEdgesFrom ON nexusEdges(fromId, type)`,
		`CREATE INDEX IF NOT EXISTS idxNexusEdgesTo ON nexusEdges(toId, type)`,
	}
	for _, q := range stmts {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func marshalAttrs(a map[string]string) string {
	if a == nil {
		return "{}"
	}
	b, _ := json.Marshal(a)
	return string(b)
}

func unmarshalAttrs(s string) map[string]string {
	if s == "" || s == "{}" {
		return nil
	}
	var out map[string]string
	if json.Unmarshal([]byte(s), &out) != nil {
		return nil
	}
	return out
}

// ── Transactions ──

// begin opens a transaction with opts and answers ErrNoTransaction, wrapping the
// driver's error, when the connector refuses those options.
//
// The refusal is recognised by its text because neither refusing layer gives
// anything else to match: database/sql refuses a read-only or non-default isolation
// request to a driver without BeginTx options ("does not support"), and the libSQL
// connector refuses both unless it was opened with immediate transactions ("not
// supported"). Any other failure, a busy primary or a cancelled context, is returned
// as it is, so it is not mistaken for a connector that can never transact.
func (s *SQLStore) begin(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, opts)
	if err == nil {
		return tx, nil
	}
	if msg := err.Error(); strings.Contains(msg, "not supported") || strings.Contains(msg, "does not support") {
		return nil, fmt.Errorf("%w: %w", ErrNoTransaction, err)
	}
	return nil, err
}

// writeOptions are the options every write transaction opens with. Serializable
// is named, rather than left to the driver's default, so a connector that would
// otherwise open a deferred transaction refuses instead: a deferred transaction
// takes no write lock before its first read, and a decision made on that read can
// be overtaken by another writer before the write lands.
var writeOptions = &sql.TxOptions{Isolation: sql.LevelSerializable}

// View runs fn over one read-only transaction and always rolls it back, so nothing
// fn reaches can be written through it. The reader fn receives is a *SQLTxReader.
func (s *SQLStore) View(ctx context.Context, fn func(r GraphReader) error) error {
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return fn(&SQLTxReader{sqlReader: sqlReader{q: tx}, tx: tx})
}

// SQLTxReader is the GraphReader SQLStore.View hands its function.
type SQLTxReader struct {
	sqlReader
	tx *sql.Tx
}

var _ GraphReader = (*SQLTxReader)(nil)

// Tx returns the read-only transaction the reader runs in, so a table on the same
// database (census.ViewSQLTx's consumers) is read at the snapshot the edges are
// read at. View owns the transaction and rolls it back when fn returns, so nothing
// written through it is kept: a caller never commits or rolls it back.
func (r *SQLTxReader) Tx() *sql.Tx { return r.tx }

// Update runs fn in one serializable transaction and commits when fn returns nil
// and the writer holds no failed write (SQLTxWriter.do).
func (s *SQLStore) Update(ctx context.Context, fn func(w TxWriter) error) error {
	tx, err := s.begin(ctx, writeOptions)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	w := &SQLTxWriter{sqlReader: sqlReader{q: tx}, tx: tx}
	if err := fn(w); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	return tx.Commit()
}

// sqlReader answers GraphReader over one querier.
type sqlReader struct{ q querier }

func (r sqlReader) GetEntity(ctx context.Context, id string) (*Entity, error) {
	return getEntity(ctx, r.q, id)
}

func (r sqlReader) ListEntitiesByType(ctx context.Context, typ string) ([]Entity, error) {
	return listEntitiesByType(ctx, r.q, typ)
}

func (r sqlReader) HasRelationship(ctx context.Context, from, to, typ string) (bool, error) {
	return hasRelationship(ctx, r.q, from, to, typ)
}

func (r sqlReader) OutEdges(ctx context.Context, from string, types ...string) ([]Relationship, error) {
	q, args := edgeQuery("fromId", from, types)
	return scanEdges(ctx, r.q, q, args)
}

func (r sqlReader) InEdges(ctx context.Context, to string, types ...string) ([]Relationship, error) {
	q, args := edgeQuery("toId", to, types)
	return scanEdges(ctx, r.q, q, args)
}

// SQLTxWriter is the TxWriter SQLStore.Update hands its function.
//
// err holds the first write that failed after it may have changed the
// transaction: deleteEntity runs two statements, and SQLite may roll a whole
// transaction back on a statement error (a full disk, an I/O error, a busy lock),
// after which further statements on the connection commit one at a time. Once err is set every later write answers it,
// and Update returns it instead of committing even when fn returns nil, so a
// function that drops a write's error cannot commit part of its writes.
type SQLTxWriter struct {
	sqlReader
	tx  *sql.Tx
	err error
}

// do runs one write unless an earlier one failed, and keeps its error unless it is
// a refusal decided before anything was written: ErrNotFound (no entity row was
// deleted), ErrCardinalityFloor (counted before the delete) or ErrInvalidSelector
// (refused before any statement). A function may catch those and go on.
func (w *SQLTxWriter) do(write func() error) error {
	if w.err != nil {
		return w.err
	}
	err := write()
	if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrCardinalityFloor) &&
		!errors.Is(err, ErrInvalidSelector) {
		w.err = err
	}
	return err
}

var _ TxWriter = (*SQLTxWriter)(nil)

// Tx returns the transaction the writer runs in, so a table on the same database
// (census's transfer log) is written in it and commits or rolls back with the
// edges. Update owns the transaction: a caller never commits or rolls it back.
func (w *SQLTxWriter) Tx() *sql.Tx { return w.tx }

func (w *SQLTxWriter) PutEntity(ctx context.Context, e *Entity) error {
	return w.do(func() error { return putEntity(ctx, w.tx, e) })
}

func (w *SQLTxWriter) DeleteEntity(ctx context.Context, id string) error {
	return w.do(func() error { return deleteEntity(ctx, w.tx, id) })
}

func (w *SQLTxWriter) PutRelationship(ctx context.Context, r *Relationship) error {
	return w.do(func() error { return putRelationship(ctx, w.tx, r) })
}

func (w *SQLTxWriter) DeleteRelationship(ctx context.Context, from, to, typ string) error {
	return w.do(func() error { return deleteRelationship(ctx, w.tx, from, to, typ) })
}

func (w *SQLTxWriter) DeleteRelationshipKeepingMin(ctx context.Context, from, to, typ string, min int) error {
	return w.do(func() error { return deleteKeepingMin(ctx, w.tx, from, to, typ, min) })
}

func (w *SQLTxWriter) DeleteEdgesWhere(ctx context.Context, sel EdgeSelector) ([]EdgeKey, error) {
	var keys []EdgeKey
	err := w.do(func() error {
		var err error
		keys, err = deleteEdgesWhere(ctx, w.tx, sel)
		return err
	})
	return keys, err
}

// ── Entities ──

func (s *SQLStore) PutEntity(ctx context.Context, e *Entity) error { return putEntity(ctx, s.db, e) }

func putEntity(ctx context.Context, q querier, e *Entity) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO nexusEntities (id, type, attrs) VALUES (?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET type = excluded.type, attrs = excluded.attrs`,
		e.ID, e.Type, marshalAttrs(e.Attrs))
	return err
}

func (s *SQLStore) GetEntity(ctx context.Context, id string) (*Entity, error) {
	return getEntity(ctx, s.db, id)
}

func getEntity(ctx context.Context, q querier, id string) (*Entity, error) {
	var e Entity
	var attrs string
	err := q.QueryRowContext(ctx, `SELECT id, type, attrs FROM nexusEntities WHERE id = ?`, id).Scan(&e.ID, &e.Type, &attrs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	e.Attrs = unmarshalAttrs(attrs)
	return &e, nil
}

// DeleteEntity removes the node and every edge incident to it (no dangling edges),
// both in one transaction, so no reader sees the edges of an entity already gone.
func (s *SQLStore) DeleteEntity(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := deleteEntity(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit()
}

func deleteEntity(ctx context.Context, q querier, id string) error {
	res, err := q.ExecContext(ctx, `DELETE FROM nexusEntities WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	_, err = q.ExecContext(ctx, `DELETE FROM nexusEdges WHERE fromId = ? OR toId = ?`, id, id)
	return err
}

func (s *SQLStore) ListEntitiesByType(ctx context.Context, typ string) ([]Entity, error) {
	return listEntitiesByType(ctx, s.db, typ)
}

func listEntitiesByType(ctx context.Context, q querier, typ string) ([]Entity, error) {
	rows, err := q.QueryContext(ctx, `SELECT id, type, attrs FROM nexusEntities WHERE type = ? ORDER BY id ASC`, typ)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var out []Entity
	for rows.Next() {
		var e Entity
		var attrs string
		if err := rows.Scan(&e.ID, &e.Type, &attrs); err != nil {
			return nil, err
		}
		e.Attrs = unmarshalAttrs(attrs)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ── Relationships ──

func (s *SQLStore) PutRelationship(ctx context.Context, r *Relationship) error {
	return putRelationship(ctx, s.db, r)
}

func putRelationship(ctx context.Context, q querier, r *Relationship) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO nexusEdges (fromId, toId, type, attrs) VALUES (?, ?, ?, ?)
		ON CONFLICT(fromId, toId, type) DO UPDATE SET attrs = excluded.attrs`,
		r.From, r.To, r.Type, marshalAttrs(r.Attrs))
	return err
}

func (s *SQLStore) DeleteRelationship(ctx context.Context, from, to, typ string) error {
	return deleteRelationship(ctx, s.db, from, to, typ)
}

func deleteRelationship(ctx context.Context, q querier, from, to, typ string) error {
	_, err := q.ExecContext(ctx, `DELETE FROM nexusEdges WHERE fromId = ? AND toId = ? AND type = ?`, from, to, typ)
	return err
}

// DeleteRelationshipKeepingMin counts and deletes inside one serializable
// transaction, so a concurrent caller is serialised behind it rather than reading a
// total the commit is about to invalidate. A writer that cannot take the lock fails
// with the driver's busy error, which refuses the delete — the floor holds either way.
func (s *SQLStore) DeleteRelationshipKeepingMin(ctx context.Context, from, to, typ string, min int) error {
	tx, err := s.begin(ctx, writeOptions)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := deleteKeepingMin(ctx, tx, from, to, typ, min); err != nil {
		return err
	}
	return tx.Commit()
}

func deleteKeepingMin(ctx context.Context, q querier, from, to, typ string, min int) error {
	var incoming int
	if err := q.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM nexusEdges WHERE toId = ? AND type = ?`, to, typ).Scan(&incoming); err != nil {
		return err
	}
	if incoming <= min {
		return ErrCardinalityFloor
	}
	return deleteRelationship(ctx, q, from, to, typ)
}

// deleteEdgesWhere reads the keys sel names and deletes the same set, both under
// one predicate in the caller's transaction, so the keys returned are exactly the
// edges removed. An attribute is compared through json_extract on the stored JSON;
// a row whose JSON does not parse fails the statement rather than being skipped.
func deleteEdgesWhere(ctx context.Context, q querier, sel EdgeSelector) ([]EdgeKey, error) {
	if err := sel.validate(); err != nil {
		return nil, err
	}
	where, args := selectorWhere(sel)
	rows, err := q.QueryContext(ctx,
		`SELECT fromId, toId, type FROM nexusEdges WHERE `+where+` ORDER BY fromId, toId, type`, args...)
	if err != nil {
		return nil, err
	}
	var keys []EdgeKey
	for rows.Next() {
		var k EdgeKey
		if err := rows.Scan(&k.From, &k.To, &k.Type); err != nil {
			closeRows(rows)
			return nil, err
		}
		keys = append(keys, k)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM nexusEdges WHERE `+where, args...); err != nil {
		return nil, err
	}
	return keys, nil
}

// selectorWhere renders sel as a WHERE clause and its arguments. Attribute keys go
// in as bound JSON paths, in sorted order so the statement text is stable.
func selectorWhere(sel EdgeSelector) (string, []any) {
	col, id := "fromId", sel.From
	if sel.To != "" {
		col, id = "toId", sel.To
	}
	where := col + ` = ? AND type = ?`
	args := []any{id, sel.Type}
	for _, k := range sortedKeys(sel.Attrs) {
		where += ` AND json_extract(attrs, ?) = ?`
		args = append(args, `$."`+k+`"`, sel.Attrs[k])
	}
	return where, args
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *SQLStore) HasRelationship(ctx context.Context, from, to, typ string) (bool, error) {
	return hasRelationship(ctx, s.db, from, to, typ)
}

func hasRelationship(ctx context.Context, q querier, from, to, typ string) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM nexusEdges WHERE fromId = ? AND toId = ? AND type = ?`, from, to, typ).Scan(&n)
	return n > 0, err
}

// edgeQuery builds a `col = ?` [+ AND type IN (?,...)] filter and its args.
func edgeQuery(col, id string, types []string) (string, []any) {
	q := `SELECT fromId, toId, type, attrs FROM nexusEdges WHERE ` + col + ` = ?`
	args := []any{id}
	if len(types) > 0 {
		q += ` AND type IN (` + strings.TrimSuffix(strings.Repeat("?,", len(types)), ",") + `)`
		for _, t := range types {
			args = append(args, t)
		}
	}
	return q, args
}

func scanEdges(ctx context.Context, qr querier, q string, args []any) ([]Relationship, error) {
	rows, err := qr.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var out []Relationship
	for rows.Next() {
		var r Relationship
		var attrs string
		if err := rows.Scan(&r.From, &r.To, &r.Type, &attrs); err != nil {
			return nil, err
		}
		r.Attrs = unmarshalAttrs(attrs)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SQLStore) OutEdges(ctx context.Context, from string, types ...string) ([]Relationship, error) {
	return sqlReader{q: s.db}.OutEdges(ctx, from, types...)
}

func (s *SQLStore) InEdges(ctx context.Context, to string, types ...string) ([]Relationship, error) {
	return sqlReader{q: s.db}.InEdges(ctx, to, types...)
}

// closeRows is the deferred Close of every query's rows in this package.
// database/sql closes rows itself once Next reports the end, so a normal return
// leaves nothing to close, and an early return already carries the error that
// stopped the read; its result is dropped.
func closeRows(rows *sql.Rows) { _ = rows.Close() }
