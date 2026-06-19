# AGENTS.md

Meta-guideline for any AI coding agent working in this repository. Read this
first.

## This Project Uses OpenSpec

The source of truth lives in `openspec/`, which is version-controlled and
agent-agnostic.

- `openspec/specs/` - the living specification of what the system currently is.
- `openspec/changes/` - active change proposals as delta specs.
- `openspec/changes/archive/` - completed changes.

Per-agent command files such as `.codex/`, `.claude/`, and editor-specific shims
are per-clone generated files and are not committed. After cloning, generate
your own with:

```bash
openspec init --tools codex
# or: openspec init --tools claude,cursor,github-copilot
```

## Workflow

Follow this lifecycle:

```text
explore -> propose -> apply -> sync -> archive
```

1. **Explore**: think and investigate only. Do not write feature code outside of
   a change.
2. **Propose**: create a change with `proposal.md`, `design.md`, `tasks.md`, and
   delta specs.
3. **Apply**: implement tasks one at a time, checking each off in `tasks.md`
   only after verification.
4. **Sync**: merge verified delta specs back into `openspec/specs/`.
5. **Archive**: move the completed change to
   `openspec/changes/archive/YYYY-MM-DD-<name>/`.

## OpenSpec CLI

If your agent has no OpenSpec slash commands, use the CLI:

```bash
openspec list [--json] [--specs]
openspec new change "<name>"
openspec status --change "<name>" --json
openspec instructions <artifact> --change "<name>"
openspec archive <name>
```

## Rules

- Before implementing anything, read the relevant files in `openspec/specs/` and
  the active change's artifacts.
- Do not write feature code without an active change proposal that contains
  tasks.
- Write OpenSpec `proposal.md` files with CLI-compatible section headers:
  `## Why` and `## What Changes`.
- Keep changes minimal and scoped to the task being implemented.
- Treat `openspec/specs/` as the truth. Reflect requirement changes there via
  the sync step, not by editing code silently.
- Keep project-specific contract, terms, and priorities in `PROJECT.md`.
- Treat the `worklane/` submodule as read-only upstream. Never modify Worklane
  source from this repository, even though it is an editable local checkout.
  Worklane is Triggerlane's external contract dependency, not part of
  Triggerlane. If Triggerlane needs a Worklane change, raise it as feedback or a
  change proposal to the Worklane upstream repository; after Worklane publishes
  it, repin the submodule here. The only valid local change to `worklane/` is
  updating the recorded submodule commit (repin).

## Repository Documents

- `README.md` is the human entry point: project summary, release path, setup,
  and navigation.
- `PROJECT.md` is the project contract: purpose, core boundaries, terminology,
  and prioritization rules.
- `BACKLOG.md` is the planning map for future OpenSpec changes.

## Language

- Write OpenSpec artifacts, ADRs, code comments, and commit messages in English.
- Converse with users in the language they use.

## Commits

Use Conventional Commits:

```text
type(scope): summary
```

Use lowercase imperative mood and keep the summary at 72 characters or fewer.
Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `build`,
`ci`.

### Commit Flow

- **Propose**: `docs(<change>): propose <summary>`
- **Apply**: `feat(<change>): <summary>` or `fix(<change>): <summary>`
- **Sync**: `docs(specs): sync <change>`
- **Archive**: `chore(openspec): archive <change>`

Never bundle unrelated changes into one commit.

## Definition Of Done

Run these from the workspace root before checking off a task, syncing specs, or
archiving a change:

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

If a command cannot run in the current environment, report that explicitly.
