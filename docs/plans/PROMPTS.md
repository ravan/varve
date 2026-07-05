# Session Prompts

Copy-paste these to drive each session. Substitute only the `<N — name>` slice identifier
(e.g. `2 — bitemporal core`), exactly as it appears in `varve-v1-roadmap.md`.

## 1. Plan a slice

```text
Use the superpowers writing-plans skill to create the detailed implementation plan for
Varve slice <N — name>.

Inputs, in this order:
1. docs/plans/STATUS.md — current position, environment facts, and decisions made in
   earlier slices (these OVERRIDE any stale assumption in the roadmap).
2. docs/plans/varve-v1-roadmap.md — this slice's entry (scope, task list, exit criteria)
   and the Global Constraints section, which apply to every task.
3. docs/design/2026-07-04-varve-design.md — the spec sections cited by the slice entry.
4. The actual current code in crates/ — plan against what exists, not what was planned.
   Where the roadmap cites XTDB porting references, read those files under refs/xtdb/
   before designing the tasks.

Requirements:
- Follow the format and granularity of docs/plans/2026-07-04-slice-00-foundation.md and
  ...slice-01-walking-skeleton.md: every task has Files, Interfaces (exact signatures),
  failing-test-first steps with complete code, run commands with expected output, and a
  commit step. TDD is non-negotiable. No placeholders.
- Cover the slice's roadmap task list completely and end with the slice exit checklist
  (including updating STATUS.md and ticking the roadmap boxes).
- We in development. Never any backward compatibility, always production code.
- Write plan in chuncks to prevent Claude API errors for too large plan.
- Verify claimed APIs against the pinned dependency versions in Cargo.toml (and note,
  as in slice 1, that test code is the contract where sketches must adapt).
- Save as docs/plans/<today's date>-slice-<NN>-<kebab-name>.md, run the writing-plans
  self-review (spec coverage, placeholder scan, type consistency), then commit the plan.

Do not start implementing — planning only. When done, tell me the plan path and a
one-paragraph summary of the task breakdown.
```

## 2. Execute a slice (after its plan exists)

```text
Read docs/plans/STATUS.md, then execute the detailed plan for Varve slice <N — name>
(docs/plans/*slice-<NN>*.md) using the superpowers subagent-driven-development skill,
task by task, resuming from the first unchecked task. TDD strictly: never write
implementation before its failing test. Global Constraints in
docs/plans/varve-v1-roadmap.md apply to every task. Commit after every green cycle.
When the slice exit checklist passes (or the session must end), update STATUS.md
(position, decisions, deviations, demo command), tick roadmap/plan checkboxes, and
commit. Never leave red tests at a session boundary.
```
