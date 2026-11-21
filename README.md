# nexus

A small, scope-agnostic relationship graph. It stores opaque entities and directed, typed edges between them, and answers questions such as "which partition does this subject belong to" or "who owns this" by running a precedence ladder of rules the caller supplies. It knows graph mechanics only: "user", "organisation", "partition" and "member" are strings the caller assigns.

## Use

```sh
go get github.com/bbmumford/nexus
```

```go
g := nexus.NewSQLStore(db) // any database/sql driver; or nexus.NewMemStore()
if err := g.InitSchema(ctx); err != nil { /* ... */ } // idempotent; once at boot

_ = g.PutEntity(ctx, &nexus.Entity{ID: "subject:ada", Type: "user"})
_ = g.PutRelationship(ctx, &nexus.Relationship{From: "subject:ada", To: "partition:acme", Type: "member"})

res, err := nexus.Resolve(ctx, g, "subject:ada",
	nexus.OutTargets("owner"),                       // first rung: a direct owner edge
	nexus.Via("member", nexus.OutTargets("owner")),  // then: the owner of anything ada is a member of
)
switch {
case res.Resolved:  // exactly one value: res.Value, produced by rung res.Via
case res.Ambiguous: // the first rung that matched gave several; res.Candidates lists them
}
```

The first rung that yields any value decides; later rungs never override it, but every value any rung produced stays in `Candidates`. A rule error fails the whole resolution closed. `ResolveIn` and `ResolveSet` run ladders of read rules (`OutTargetsIn`, `InSourcesIn`, `ViaIn`) over any `GraphReader`, a transaction's reader included, and `DeleteRelationshipKeepingMin` deletes an edge only while more than a minimum of that type remain, counting and deleting in one operation (`ErrCardinalityFloor` otherwise).

## Consumers

ORBTR library, ai cortex, worker and the api.orbtr.ai and node.orbtr.ai endpoints, io constellation, agent, tools and the node.orbtr.io endpoint, and the census package, which builds its accounts layer on it.
