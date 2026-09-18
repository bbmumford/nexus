package nexus

import (
	"context"
	"errors"
)

// ErrNotFound is returned by GetEntity for an unknown id.
var ErrNotFound = errors.New("nexus: not found")

// ErrCardinalityFloor is returned by DeleteRelationshipKeepingMin when removing the
// edge would leave fewer than the required number pointing at the target.
var ErrCardinalityFloor = errors.New("nexus: cardinality floor")

// GraphStore persists opaque entities and typed relationships. A consumer supplies a *sql.DB store
// (the shipped standalone default), a mesh-RPC gateway, or the reference MemStore. It interprets
// NOTHING — opaque ids/types/attrs move in and out; the edge walks below are what the resolver runs.
//
// Deleting an entity also removes every edge incident to it (no dangling edges); this is a store
// responsibility so a consumer never has to sweep edges by hand.
type GraphStore interface {
	// ── Entities ──
	PutEntity(ctx context.Context, e *Entity) error
	GetEntity(ctx context.Context, id string) (*Entity, error)
	DeleteEntity(ctx context.Context, id string) error
	ListEntitiesByType(ctx context.Context, typ string) ([]Entity, error)

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
	HasRelationship(ctx context.Context, from, to, typ string) (bool, error)
	// OutEdges returns edges FROM `from`; InEdges returns edges TO `to`. With one or more types the
	// result is filtered to those edge-types; with none, all edges are returned. These two are the
	// primitives the resolver walks — any precedence ladder is expressed as OutEdges/InEdges hops.
	OutEdges(ctx context.Context, from string, types ...string) ([]Relationship, error)
	InEdges(ctx context.Context, to string, types ...string) ([]Relationship, error)
}
