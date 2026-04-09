# Project guidance for Agents

## Objective

Help incrementally normalize a large, messy raster estate toward:

- COG or COG-like storage
- STAC exposure
- repeatable, low-risk daily progress

## Current phase

Phase 1 only: inventory and metadata capture into SQLite.

## Constraints

- Prefer Python and SQLite.
- Prefer GDAL (Python bindings or CLI) over ArcPy.
- Do not overwrite source rasters.
- Keep changes small and reviewable.
- Favor idempotent scripts and append-only metadata collection.
- Avoid premature architecture.

## When editing code

- Keep dependencies minimal.
- Add docstrings where they clarify behavior.
- Keep CLI flags explicit rather than clever.
- Prefer standard library unless a small dependency materially improves clarity.
- Preserve backward-compatible database migrations when possible.
- Use red-green TDD whenever touching executable code: add (or update) a failing test, make it pass, then refactor if needed before moving on.

## Near-term roadmap

1. inventory rasters into SQLite
2. add issue/triage detection
3. create processing queue
4. add conversion/validation pipeline
5. generate STAC items and collections

## Validation

For code changes, prefer running cli, library if needed:

```bash
# cli tool:
raster-repo --help

# library:
uv run python -m raster_inventory --help
uv run python -m raster_inventory.inventory --help
```

Run tests before finishing.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
