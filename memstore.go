package nexus

import (
	"context"
	"sort"
	"sync"
)

// MemStore is the in-memory reference GraphStore — the test/dev backend and the semantic reference a
// durable (*sql.DB) store must match. Thread-safe. It interprets nothing; edges are (from,to,type)
// tuples scanned for the out/in walks the resolver uses.
type MemStore struct {
	mu       sync.Mutex
	entities map[string]*Entity
	edges    []*Relationship
}

// NewMemStore builds an empty graph.
func NewMemStore() *MemStore {
	return &MemStore{entities: map[string]*Entity{}}
}

var _ GraphStore = (*MemStore)(nil)

func (m *MemStore) PutEntity(_ context.Context, e *Entity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entities[e.ID] = e.Clone()
	return nil
}

func (m *MemStore) GetEntity(_ context.Context, id string) (*Entity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entities[id]
	if !ok {
		return nil, ErrNotFound
	}
	return e.Clone(), nil
}

// DeleteEntity removes the node and every edge incident to it (no dangling edges).
func (m *MemStore) DeleteEntity(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.entities[id]; !ok {
		return ErrNotFound
	}
	delete(m.entities, id)
	kept := m.edges[:0]
	for _, e := range m.edges {
		if e.From == id || e.To == id {
			continue
		}
		kept = append(kept, e)
	}
	m.edges = kept
	return nil
}

func (m *MemStore) ListEntitiesByType(_ context.Context, typ string) ([]Entity, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Entity
	for _, e := range m.entities {
		if e.Type == typ {
			out = append(out, *e.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// PutRelationship upserts the edge identified by (From, To, Type).
func (m *MemStore) PutRelationship(_ context.Context, r *Relationship) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range m.edges {
		if e.From == r.From && e.To == r.To && e.Type == r.Type {
			m.edges[i] = r.Clone()
			return nil
		}
	}
	m.edges = append(m.edges, r.Clone())
	return nil
}

func (m *MemStore) DeleteRelationship(_ context.Context, from, to, typ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.edges[:0]
	for _, e := range m.edges {
		if e.From == from && e.To == to && e.Type == typ {
			continue
		}
		kept = append(kept, e)
	}
	m.edges = kept
	return nil
}

// DeleteRelationshipKeepingMin holds the same lock across the count and the delete,
// so a second caller observing the count waits for the first to finish rather than
// deciding against a total that is about to change.
func (m *MemStore) DeleteRelationshipKeepingMin(_ context.Context, from, to, typ string, min int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var incoming int
	for _, e := range m.edges {
		if e.To == to && e.Type == typ {
			incoming++
		}
	}
	if incoming <= min {
		return ErrCardinalityFloor
	}
	kept := m.edges[:0]
	for _, e := range m.edges {
		if e.From == from && e.To == to && e.Type == typ {
			continue
		}
		kept = append(kept, e)
	}
	m.edges = kept
	return nil
}

func (m *MemStore) HasRelationship(_ context.Context, from, to, typ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.edges {
		if e.From == from && e.To == to && e.Type == typ {
			return true, nil
		}
	}
	return false, nil
}

func typeSet(types []string) map[string]struct{} {
	if len(types) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(types))
	for _, t := range types {
		s[t] = struct{}{}
	}
	return s
}

func (m *MemStore) OutEdges(_ context.Context, from string, types ...string) ([]Relationship, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	filter := typeSet(types)
	var out []Relationship
	for _, e := range m.edges {
		if e.From != from {
			continue
		}
		if filter != nil {
			if _, ok := filter[e.Type]; !ok {
				continue
			}
		}
		out = append(out, *e.Clone())
	}
	return out, nil
}

func (m *MemStore) InEdges(_ context.Context, to string, types ...string) ([]Relationship, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	filter := typeSet(types)
	var out []Relationship
	for _, e := range m.edges {
		if e.To != to {
			continue
		}
		if filter != nil {
			if _, ok := filter[e.Type]; !ok {
				continue
			}
		}
		out = append(out, *e.Clone())
	}
	return out, nil
}
