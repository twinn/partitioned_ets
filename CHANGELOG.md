# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-04-10

### Added

- Full `:ets` API surface routed through rendezvous hashing (HRW) over a
  cluster-wide `{node, partition}` shard set.
- Automatic handoff of affected entries on graceful node joins and leaves.
- Multi-partition support (`:partitions` option) for write-contention relief.
- `use PartitionedEts` macro for one-module-per-table convenience wrappers.
- Overridable `hash/2`, `node_added/1`, and `node_removed/1` callbacks.
- Paginated variants of `match/3`, `select/3`, `match_object/3`, and
  `select_reverse/3` with opaque continuation tokens.

[0.1.0]: https://github.com/twinn/partitioned_ets/releases/tag/v0.1.0
