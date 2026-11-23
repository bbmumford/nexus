# nexus

nexus is a small relationship graph. It stores opaque entities and directed, typed edges between them, and answers questions such as "which partition does this subject belong to" or "who owns this" by running a precedence ladder of rules the caller supplies. It knows graph mechanics only: "user", "organisation", "partition" and "member" are strings the caller assigns.

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

## Dependencies

No other package of this estate, and no external module in its production
code: `SQLStore` runs on any `database/sql` driver. Tests only:
`modernc.org/sqlite`.

## Used by

Importers in the ORBTR estate:

- `github.com/ORBTR/Library/domain/identityhost`, `domain/enrollmenthost`,
  `authadapters`, `authgw`, `grantresolve`, `grantwrite`, `livemembership`,
  `founderbackfill` and more: membership, grants and who-belongs-where.
- `github.com/ORBTR/cortex/authority/approval/roster`, `operations/firing`,
  `operations/planlimit`, `credential/api_key/keyseal` and
  `credential/api_key/internal/grantfixture`.
- `github.com/ORBTR/api.orbtr.ai`, `github.com/ORBTR/ai/node.orbtr.ai`,
  `github.com/ORBTR/node.orbtr.io` and
  `github.com/ORBTR/tools/schemaref/manifest`.
- `github.com/bbmumford/census`, which builds its accounts layer on it.
- Tests only: packages of `github.com/ORBTR/constellation`,
  `github.com/ORBTR/worker` and `github.com/ORBTR/agent`.

## Platforms and build

Pure Go, no cgo and no build tags. The module's `go` directive is 1.25.0.

## Licence

MIT. Copyright (c) 2026 Brandon Miller-Mumford. The full text is in
[LICENSE](LICENSE).

## Contributing

[AGENTS.md](AGENTS.md) holds the repository's two rules, for people and coding
agents alike: no temporary documents in the repository, and conventional commit
messages that stand on their own. [CLAUDE.md](CLAUDE.md) imports it for Claude
Code.
