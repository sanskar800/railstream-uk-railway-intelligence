# ADR-006: BACKWARD_TRANSITIVE compatibility for every contract

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

The contracts in `contracts/` are JSON Schemas in the Schema Registry (architecture.md §6). Compatibility mode decides which schema changes the registry accepts. Two facts drive the choice. First, the raw archive is the source of truth and is **replayed from its first day** (2026-11-05) through the newest pipeline: any message ever captured must still pass the current contract. Second, the producer is Network Rail, which we do not control: we cannot upgrade "producers first", and the feed may add fields at any time.

## Options considered

1. **BACKWARD** (registry default): a new version must accept data valid under the *previous* version only. Two successive changes can together break old data (v1 → v2 → v3, each step compatible, v3 rejecting v1 data), which a replay from day one would hit.
2. **FORWARD / FULL**: protect old consumers reading new data. Our consumers are upgraded with the contract (same repository), so this protects nothing we have, and FULL forbids useful changes.
3. **BACKWARD_TRANSITIVE**: a new version must accept data valid under **every** earlier version: exactly "the whole archive stays valid".
4. **NONE**: no protection.

## Decision

**BACKWARD_TRANSITIVE** for every subject, set by `make schemas` when a subject is created. Because Redpanda's JSON Schema check misses rules inside `if/then` branches (findings F15), `make schemas` also refuses any change that adds a required field anywhere, and it checks each registered version explicitly (Redpanda has no all-versions endpoint).

## Consequences

- Allowed: adding optional fields, removing a requirement, widening an enum or pattern. Refused: new required fields, narrower enums or patterns, type changes. A stricter rule becomes a silver data-quality check (it flags rather than rejects) or a new versioned topic.
- The open content model (unknown fields allowed) keeps a new upstream field from dead-lettering messages; a field the pipeline starts using is added as optional.
- CI runs the same check against the contracts on the base branch, so an incompatible change fails the pull request, not the running pipeline.
- Wrong if: Network Rail makes an incompatible change itself (e.g. removes a required field). Then the contract cannot follow transitively; that is a breaking upstream change, handled with a new contract version and a dated note, and the DLQ rate shows it immediately.
