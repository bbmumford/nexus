package nexus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

var _ GraphStore = (*SQLStore)(nil)

// InitSchema creates the entity/edge tables + indexes (idempotent); run once at boot.
func (s *SQLStore) InitSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS nexus_entities (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL DEFAULT '',
			attrs TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE INDEX IF NOT EXISTS idx_nexus_entities_type ON nexus_entities(type)`,
		`CREATE TABLE IF NOT EXISTS nexus_edges (
			from_id TEXT NOT NULL,
			to_id TEXT NOT NULL,
			type TEXT NOT NULL,
			attrs TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (from_id, to_id, type)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_nexus_edges_from ON nexus_edges(from_id, type)`,
		`CREATE INDEX IF NOT EXISTS idx_nexus_edges_to ON nexus_edges(to_id, type)`,
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

// ── Entities ──

func (s *SQLStore) PutEntity(ctx context.Context, e *Entity) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO nexus_entities (id, type, attrs) VALUES (?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET type = excluded.type, attrs = excluded.attrs`,
		e.ID, e.Type, marshalAttrs(e.Attrs))
	return err
}

func (s *SQLStore) GetEntity(ctx context.Context, id string) (*Entity, error) {
	var e Entity
	var attrs string
	err := s.db.QueryRowContext(ctx, `SELECT id, type, attrs FROM nexus_entities WHERE id = ?`, id).Scan(&e.ID, &e.Type, &attrs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	e.Attrs = unmarshalAttrs(attrs)
	return &e, nil
}

// DeleteEntity removes the node and every edge incident to it (no dangling edges).
func (s *SQLStore) DeleteEntity(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM nexus_entities WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM nexus_edges WHERE from_id = ? OR to_id = ?`, id, id)
	return err
}

func (s *SQLStore) ListEntitiesByType(ctx context.Context, typ string) ([]Entity, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, type, attrs FROM nexus_entities WHERE type = ? ORDER BY id ASC`, typ)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO nexus_edges (from_id, to_id, type, attrs) VALUES (?, ?, ?, ?)
		ON CONFLICT(from_id, to_id, type) DO UPDATE SET attrs = excluded.attrs`,
		r.From, r.To, r.Type, marshalAttrs(r.Attrs))
	return err
}

func (s *SQLStore) DeleteRelationship(ctx context.Context, from, to, typ string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM nexus_edges WHERE from_id = ? AND to_id = ? AND type = ?`, from, to, typ)
	return err
}

// DeleteRelationshipKeepingMin counts and deletes inside one immediate transaction, so
// a concurrent caller is serialised behind it rather than reading a total the commit is
// about to invalidate. A writer that cannot take the lock fails with the driver's busy
// error, which refuses the delete — the floor holds either way.
func (s *SQLStore) DeleteRelationshipKeepingMin(ctx context.Context, from, to, typ string, min int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var incoming int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM nexus_edges WHERE to_id = ? AND type = ?`, to, typ).Scan(&incoming); err != nil {
		return err
	}
	if incoming <= min {
		return ErrCardinalityFloor
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM nexus_edges WHERE from_id = ? AND to_id = ? AND type = ?`, from, to, typ); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLStore) HasRelationship(ctx context.Context, from, to, typ string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM nexus_edges WHERE from_id = ? AND to_id = ? AND type = ?`, from, to, typ).Scan(&n)
	return n > 0, err
}

// edgeQuery builds a `col = ?` [+ AND type IN (?,...)] filter and its args.
func edgeQuery(col, id string, types []string) (string, []any) {
	q := `SELECT from_id, to_id, type, attrs FROM nexus_edges WHERE ` + col + ` = ?`
	args := []any{id}
	if len(types) > 0 {
		q += ` AND type IN (` + strings.TrimSuffix(strings.Repeat("?,", len(types)), ",") + `)`
		for _, t := range types {
			args = append(args, t)
		}
	}
	return q, args
}

func (s *SQLStore) scanEdges(ctx context.Context, q string, args []any) ([]Relationship, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
	q, args := edgeQuery("from_id", from, types)
	return s.scanEdges(ctx, q, args)
}

func (s *SQLStore) InEdges(ctx context.Context, to string, types ...string) ([]Relationship, error) {
	q, args := edgeQuery("to_id", to, types)
	return s.scanEdges(ctx, q, args)
}
