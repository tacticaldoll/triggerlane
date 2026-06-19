# Development Flow

This project uses OpenSpec for spec-driven development. `AGENTS.md` is the
authoritative contributor and agent guide; this file is a short checklist.

## One Change

1. Explore current specs and code before editing:
   - `openspec list --specs`
   - `openspec list`
   - read relevant files under `openspec/specs/`
2. Propose the change:
   - `openspec new change "<change-name>"`
   - write `proposal.md`, `design.md`, `tasks.md`, and delta specs
   - commit as `docs(<change-name>): propose <summary>`
3. Apply the change:
   - implement against `openspec/changes/<change-name>/specs/`
   - check off tasks only after code and tests pass
   - commit coherent compiling milestones as `feat(...)` or `fix(...)`
4. Sync verified semantics:
   - promote verified delta specs into `openspec/specs/`
   - commit as `docs(specs): sync <change-name>`
5. Archive the completed change:
   - `openspec archive <change-name>`
   - commit as `chore(openspec): archive <change-name>`

## Baseline

The `0.1.0 baseline` was established as a single squashed commit: the living specs
under `openspec/specs/` are its durable source of truth, while the change artifacts
that produced it (proposals, tasks, delta specs) were scaffolding and were not
retained. So `openspec/changes/archive/` is intentionally empty at baseline — it is
populated only as changes made *after* 0.1.0 complete the lifecycle above. An empty
archive therefore means "no post-baseline change has been archived yet," not "no
significant work has happened."

## Commit Granularity

Apply commits should be larger than individual task checkboxes and smaller than
an entire risky feature. Prefer one commit per coherent milestone that builds,
tests, and preserves the spec contract.

Avoid:

- committing unrelated docs, refactors, and behavior together
- checking off `tasks.md` before the Definition of Done passes
- syncing `openspec/specs/` before implementation has been verified

## Worklane Dependency Policy

Worklane is a repository-managed git submodule at `worklane/`. Local
development uses workspace path dependencies into that submodule, and non-local
reproducibility follows the submodule commit recorded in this repository.

Declare Worklane crates only in the root workspace dependency table. Member
crates should consume them with `*.workspace = true` so the Worklane revision is
visible in one place.

Production-facing Triggerlane crates may depend on `worklane-core` contracts.
Concrete Worklane broker implementation crates appear only in tests, examples, and
the single CLI broker composition seam (`connect_broker`), which selects the
durable backend — `worklane-sqlite` by default, or `worklane-postgres` /
`worklane-redis`. The in-memory `worklane-memory` broker stays test/example-only.

## Definition Of Done

Run these from the workspace root:

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```
