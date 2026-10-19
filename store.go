package nexus

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"
)

// ErrNotFound is returned by GetEntity for an unknown id.
var ErrNotFound = errors.New("nexus: not found")

// ErrCardinalityFloor is returned by DeleteRelationshipKeepingMin when removing the
// edge would leave fewer than the required number pointing at the target.
var ErrCardinalityFloor = errors.New("nexus: cardinality floor")

// ErrNoTransaction is returned by View and Update when the store cannot give the
// transaction they promise: its connector refuses a read-only transaction or a
// serializable one. A caller whose decision depends on reading and writing in one
// transaction refuses on it rather than proceeding over separate statements.
var ErrNoTransaction = errors.New("nexus: no transaction")

// ErrInvalidSelector is returned by DeleteEdgesWhere for a selector that does not
// name exactly one endpoint and a type, or names an attribute key it cannot match.
var ErrInvalidSelector = errors.New("nexus: invalid edge selector")

// GraphReader is the read half of a graph: the lookups and the edge walks the
// resolver runs. View hands one to its function, so a reader there cannot write.
type GraphReader interface {
	GetEntity(ctx context.Context, id string) (*Entity, error)
	ListEntitiesByType(ctx context.Context, typ string) ([]Entity, error)
	HasRelationship(ctx context.Context, from, to, typ string) (bool, error)
	// OutEdges returns edges FROM `from`; InEdges returns edges TO `to`. With one or more types the
	// result is filtered to those edge-types; with none, all edges are returned. These two are the
	// primitives the resolver walks — any precedence ladder is expressed as OutEdges/InEdges hops.
	OutEdges(ctx context.Context, from string, types ...string) ([]Relationship, error)
	InEdges(ctx context.Context, to string, types ...string) ([]Relationship, error)
}

// GraphStore persists opaque entities and typed relationships. A consumer supplies a *sql.DB store
// (the shipped standalone default), a mesh-RPC gateway, or the reference MemStore. It interprets
// NOTHING — opaque ids/types/attrs move in and out; the edge walks below are what the resolver runs.
//
// Deleting an entity also removes every edge incident to it (no dangling edges); this is a store
// responsibility so a consumer never has to sweep edges by hand.
type GraphStore interface {
	GraphReader

	// ── Entities ──
	PutEntity(ctx context.Context, e *Entity) error
	DeleteEntity(ctx context.Context, id string) error

	// ── Relationships ──
	PutRelationship(ctx context.Context, r *Relationship) error
	DeleteRelationship(ctx context.Context, from, to, typ string) error
	// DeleteRelationshipKeepingMin deletes (from,to,typ) only while more than `min`
	// edges of typ point at `to`, and returns ErrCardinalityFloor otherwise. The count
	// and the delete are ONE operation.
	//
	// It exists as a store method, rather than a count followed by DeleteRelationship,
	// because that pair is not atomic: two callers each observing a safe count both
	// proceed, and the floor they were both honouring is breached. An implementation
	// that cannot make the pair atomic must not implement this interface.
	DeleteRelationshipKeepingMin(ctx context.Context, from, to, typ string, min int) error
}

// TxWriter reads and writes inside one Update. Every read sees the writes made
// before it in the same function, and nothing it writes is visible to anyone else
// until the function returns nil and the transaction commits.
type TxWriter interface {
	GraphReader

	PutEntity(ctx context.Context, e *Entity) error
	// DeleteEntity removes the entity and every edge incident to it in this
	// transaction, and returns ErrNotFound for an unknown id.
	DeleteEntity(ctx context.Context, id string) error
	PutRelationship(ctx context.Context, r *Relationship) error
	DeleteRelationship(ctx context.Context, from, to, typ string) error
	// DeleteRelationshipKeepingMin is the store method of the same name, counted and
	// deleted in this transaction.
	DeleteRelationshipKeepingMin(ctx context.Context, from, to, typ string, min int) error
	// DeleteEdgesWhere deletes every edge sel names with one statement and returns
	// their keys ordered by (From, To, Type). A removal of any size is therefore one
	// short transaction rather than one statement per edge.
	DeleteEdgesWhere(ctx context.Context, sel EdgeSelector) ([]EdgeKey, error)
}

// TxStore is a GraphStore whose reads share one snapshot and whose writes commit as one.
//
// View runs fn over one read-only snapshot: a write committed by anyone after the
// snapshot is taken is not seen by any read fn makes. Update runs fn in one
// serializable transaction and commits only when fn returns nil and no write in it
// failed other than by a refusal decided before writing (ErrNotFound,
// ErrCardinalityFloor, ErrInvalidSelector); any other error, returned or dropped by
// fn, or a panic, rolls every write back. Both answer ErrNoTransaction when the
// connector cannot give that transaction.
//
// A write on the store itself from inside fn is a second writer: it waits for the
// transaction fn runs in, as a second connection does.
type TxStore interface {
	GraphStore
	View(ctx context.Context, fn func(r GraphReader) error) error
	Update(ctx context.Context, fn func(w TxWriter) error) error
}

// EdgeKey is an edge's identity, (From, To, Type).
type EdgeKey struct {
	From string
	To   string
	Type string
}

// EdgeSelector names a set of edges for DeleteEdgesWhere: the edges of Type leaving
// From, or the edges of Type entering To, whose attributes hold every pair in
// Attrs. Exactly one of From and To is set and Type is required, so a selector can
// never name edges across the whole graph. An attribute matches only when it is
// present and equal; an absent attribute matches no value, the empty one included.
type EdgeSelector struct {
	From  string
	To    string
	Type  string
	Attrs map[string]string
}

// validate refuses a selector that names neither or both endpoints, no type, or an
// attribute the SQL store would match differently from MemStore, so both stores
// remove the same set or both refuse:
//   - a key carrying a double quote or a backslash: selectorWhere places the key in
//     a quoted JSON path label, where a quote ends the label and SQLite reads a
//     backslash as an escape, so `ab` would select the attribute "ab";
//   - a key or value that is not valid UTF-8: marshalAttrs stores attributes as JSON,
//     which replaces such bytes, so json_extract never equals the bytes MemStore
//     compares.
func (sel EdgeSelector) validate() error {
	if (sel.From == "") == (sel.To == "") || sel.Type == "" {
		return ErrInvalidSelector
	}
	for k, v := range sel.Attrs {
		if k == "" || strings.ContainsAny(k, `"\`) || !utf8.ValidString(k) || !utf8.ValidString(v) {
			return ErrInvalidSelector
		}
	}
	return nil
}

// matches reports whether r is an edge sel names.
func (sel EdgeSelector) matches(r *Relationship) bool {
	if r.Type != sel.Type {
		return false
	}
	if sel.From != "" && r.From != sel.From {
		return false
	}
	if sel.To != "" && r.To != sel.To {
		return false
	}
	for k, want := range sel.Attrs {
		if got, ok := r.Attrs[k]; !ok || got != want {
			return false
		}
	}
	return true
}
