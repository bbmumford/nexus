package nexus

import (
	"context"
	"sort"
	"sync"
)

// MemStore is the in-memory reference GraphStore — the test/dev backend and the semantic reference a
// durable (*sql.DB) store must match. Thread-safe. It interprets nothing; edges are (from,to,type)
// tuples scanned for the out/in walks the resolver uses.
//
// The graph is held as an immutable state value. Every write builds the next state from a copy and
// swaps it in, so a reader holding a state is never written under: that is what lets View read one
// snapshot without locking writers out, and lets Update discard its copy to roll back.
type MemStore struct {
	wmu sync.Mutex // serialises writers: every write method and every Update
	mu  sync.Mutex // guards st
	st  *memState
}

// memState is one version of the graph. A state that has been swapped in is never
// modified; the stored *Entity and *Relationship values are replaced, never mutated.
type memState struct {
	entities map[string]*Entity
	edges    []*Relationship
}

// NewMemStore builds an empty graph.
func NewMemStore() *MemStore {
	return &MemStore{st: &memState{entities: map[string]*Entity{}}}
}

var (
	_ GraphStore = (*MemStore)(nil)
	_ TxStore    = (*MemStore)(nil)
)

// snapshot returns the current state, which the caller may read without a lock.
func (m *MemStore) snapshot() *memState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st
}

// write applies fn to a private copy of the current state and swaps the copy in
// when fn returns nil. A failing fn leaves the store as it was.
func (m *MemStore) write(fn func(next *memState) error) error {
	m.wmu.Lock()
	defer m.wmu.Unlock()
	next := m.snapshot().clone()
	if err := fn(next); err != nil {
		return err
	}
	m.mu.Lock()
	m.st = next
	m.mu.Unlock()
	return nil
}

func (s *memState) clone() *memState {
	cp := &memState{entities: make(map[string]*Entity, len(s.entities)), edges: make([]*Relationship, len(s.edges))}
	for k, v := range s.entities {
		cp.entities[k] = v
	}
	copy(cp.edges, s.edges)
	return cp
}

// View runs fn over the state current when it is called.
func (m *MemStore) View(_ context.Context, fn func(r GraphReader) error) error {
	return fn(memReader{st: m.snapshot()})
}

// Update runs fn over a private copy and swaps it in when fn returns nil.
func (m *MemStore) Update(_ context.Context, fn func(w TxWriter) error) error {
	return m.write(func(next *memState) error { return fn(memWriter{memReader{st: next}}) })
}

func (m *MemStore) PutEntity(ctx context.Context, e *Entity) error {
	return m.write(func(next *memState) error { return memWriter{memReader{next}}.PutEntity(ctx, e) })
}

func (m *MemStore) GetEntity(ctx context.Context, id string) (*Entity, error) {
	return memReader{m.snapshot()}.GetEntity(ctx, id)
}

// DeleteEntity removes the node and every edge incident to it (no dangling edges).
func (m *MemStore) DeleteEntity(ctx context.Context, id string) error {
	return m.write(func(next *memState) error { return memWriter{memReader{next}}.DeleteEntity(ctx, id) })
}

func (m *MemStore) ListEntitiesByType(ctx context.Context, typ string) ([]Entity, error) {
	return memReader{m.snapshot()}.ListEntitiesByType(ctx, typ)
}

// PutRelationship upserts the edge identified by (From, To, Type).
func (m *MemStore) PutRelationship(ctx context.Context, r *Relationship) error {
	return m.write(func(next *memState) error { return memWriter{memReader{next}}.PutRelationship(ctx, r) })
}

func (m *MemStore) DeleteRelationship(ctx context.Context, from, to, typ string) error {
	return m.write(func(next *memState) error {
		return memWriter{memReader{next}}.DeleteRelationship(ctx, from, to, typ)
	})
}

// DeleteRelationshipKeepingMin counts and deletes while holding the writer lock, so
// a second caller observing the count waits for the first to finish rather than
// deciding against a total that is about to change.
func (m *MemStore) DeleteRelationshipKeepingMin(ctx context.Context, from, to, typ string, min int) error {
	return m.write(func(next *memState) error {
		return memWriter{memReader{next}}.DeleteRelationshipKeepingMin(ctx, from, to, typ, min)
	})
}

func (m *MemStore) HasRelationship(ctx context.Context, from, to, typ string) (bool, error) {
	return memReader{m.snapshot()}.HasRelationship(ctx, from, to, typ)
}

func (m *MemStore) OutEdges(ctx context.Context, from string, types ...string) ([]Relationship, error) {
	return memReader{m.snapshot()}.OutEdges(ctx, from, types...)
}

func (m *MemStore) InEdges(ctx context.Context, to string, types ...string) ([]Relationship, error) {
	return memReader{m.snapshot()}.InEdges(ctx, to, types...)
}

// memReader answers GraphReader over one state.
type memReader struct{ st *memState }

func (r memReader) GetEntity(_ context.Context, id string) (*Entity, error) {
	e, ok := r.st.entities[id]
	if !ok {
		return nil, ErrNotFound
	}
	return e.Clone(), nil
}

func (r memReader) ListEntitiesByType(_ context.Context, typ string) ([]Entity, error) {
	var out []Entity
	for _, e := range r.st.entities {
		if e.Type == typ {
			out = append(out, *e.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r memReader) HasRelationship(_ context.Context, from, to, typ string) (bool, error) {
	for _, e := range r.st.edges {
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

func (r memReader) OutEdges(_ context.Context, from string, types ...string) ([]Relationship, error) {
	return r.edgesWhere(func(e *Relationship) bool { return e.From == from }, types), nil
}

func (r memReader) InEdges(_ context.Context, to string, types ...string) ([]Relationship, error) {
	return r.edgesWhere(func(e *Relationship) bool { return e.To == to }, types), nil
}

func (r memReader) edgesWhere(match func(*Relationship) bool, types []string) []Relationship {
	filter := typeSet(types)
	var out []Relationship
	for _, e := range r.st.edges {
		if !match(e) {
			continue
		}
		if filter != nil {
			if _, ok := filter[e.Type]; !ok {
				continue
			}
		}
		out = append(out, *e.Clone())
	}
	return out
}

// memWriter writes one private state, the copy write or Update made. It is never
// handed a state another reader holds.
type memWriter struct{ memReader }

func (w memWriter) PutEntity(_ context.Context, e *Entity) error {
	w.st.entities[e.ID] = e.Clone()
	return nil
}

func (w memWriter) DeleteEntity(_ context.Context, id string) error {
	if _, ok := w.st.entities[id]; !ok {
		return ErrNotFound
	}
	delete(w.st.entities, id)
	w.removeEdges(func(e *Relationship) bool { return e.From == id || e.To == id })
	return nil
}

func (w memWriter) PutRelationship(_ context.Context, r *Relationship) error {
	for i, e := range w.st.edges {
		if e.From == r.From && e.To == r.To && e.Type == r.Type {
			w.st.edges[i] = r.Clone()
			return nil
		}
	}
	w.st.edges = append(w.st.edges, r.Clone())
	return nil
}

func (w memWriter) DeleteRelationship(_ context.Context, from, to, typ string) error {
	w.removeEdges(func(e *Relationship) bool { return e.From == from && e.To == to && e.Type == typ })
	return nil
}

func (w memWriter) DeleteRelationshipKeepingMin(ctx context.Context, from, to, typ string, min int) error {
	var incoming int
	for _, e := range w.st.edges {
		if e.To == to && e.Type == typ {
			incoming++
		}
	}
	if incoming <= min {
		return ErrCardinalityFloor
	}
	return w.DeleteRelationship(ctx, from, to, typ)
}

func (w memWriter) DeleteEdgesWhere(_ context.Context, sel EdgeSelector) ([]EdgeKey, error) {
	if err := sel.validate(); err != nil {
		return nil, err
	}
	var keys []EdgeKey
	w.removeEdges(func(e *Relationship) bool {
		if !sel.matches(e) {
			return false
		}
		keys = append(keys, EdgeKey{From: e.From, To: e.To, Type: e.Type})
		return true
	})
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.From != b.From {
			return a.From < b.From
		}
		if a.To != b.To {
			return a.To < b.To
		}
		return a.Type < b.Type
	})
	return keys, nil
}

// removeEdges drops every edge drop reports. It filters in place, which is safe
// only because the state is the writer's private copy.
func (w memWriter) removeEdges(drop func(*Relationship) bool) {
	kept := w.st.edges[:0]
	for _, e := range w.st.edges {
		if drop(e) {
			continue
		}
		kept = append(kept, e)
	}
	for i := len(kept); i < len(w.st.edges); i++ {
		w.st.edges[i] = nil
	}
	w.st.edges = kept
}
