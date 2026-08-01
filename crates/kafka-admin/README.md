# kafka-admin

**Kafka 4.x admin client.** Part of [kaas-lib](https://kaas-rs.github.io/kaas-lib).

Topics, configs, offsets, groups, ACLs, quotas, SCRAM credentials,
partition reassignments and transactions — 31 admin RPCs.

Every call naming several resources returns one result per resource,
`Vec<(Id, Result<T, Error>)>`, never `Result<Vec<T>, Error>`. Describing 500
topics while 3 are mid-deletion returns 497 descriptions and 3 errors.

Kafka 4.x groups come in four kinds described by different RPCs with different
response shapes. They are kept distinct rather than flattened, with an
`Unrecognized` variant so a streams group renders instead of taking down the
group list.

## Documentation

- [Admin operations](https://kaas-rs.github.io/kaas-lib/guide/admin.html)
- [The four group kinds](https://kaas-rs.github.io/kaas-lib/compat/group-kinds.html)

Full book: <https://kaas-rs.github.io/kaas-lib/>

## Licence

Apache-2.0
