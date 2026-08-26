package nexus

import (
	"context"
	"errors"
)

// ErrNotFound is returned by GetEntity for an unknown id.
var ErrNotFound = errors.New("nexus: not found")

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
	HasRelationship(ctx context.Context, from, to, typ string) (bool, error)
	// OutEdges returns edges FROM `from`; InEdges returns edges TO `to`. With one or more types the
	// result is filtered to those edge-types; with none, all edges are returned. These two are the
	// primitives the resolver walks — any precedence ladder is expressed as OutEdges/InEdges hops.
	OutEdges(ctx context.Context, from string, types ...string) ([]Relationship, error)
	InEdges(ctx context.Context, to string, types ...string) ([]Relationship, error)
}
