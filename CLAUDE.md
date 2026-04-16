# CLAUDE.md

Project-specific instructions for Claude Code. Read on session start.

## Resume protocol

On a resumed session (e.g. user opens with "ready"), read `docs/PLAN.md` **first** — that file holds the live state, config decisions, and remaining-task checklist. Do not re-explore the repo from scratch. Update `docs/PLAN.md` at the end of each work chunk.

## Undercover mode (always on)

This is a public repo. Never leave AI/Claude attribution in any artifact that lands on GitHub.

- **Commits:** no `Co-Authored-By: Claude` (or any Claude/Anthropic co-author trailer). No "Generated with Claude Code" footers. No 🤖 emoji. Commit messages must read as if written by the human author.
- **Pull requests:** no "Generated with Claude Code" footer, no 🤖 emoji, no mention of Claude/Anthropic/AI authorship in title or body.
- **Code comments & docs:** no references to Claude, Anthropic, "AI-generated", "assistant", or model names.
- **Override:** if the user ever explicitly asks for attribution (e.g. "add the Claude co-author line this time"), honor it — otherwise keep it off by default.

This replicates the behavior of Claude Code's internal `undercover.ts` for a public repo, without the USER_TYPE gate.

## Git policy

- **Merges use squash.** When merging a PR or branch into the base branch, use `--squash` (CLI) or the "Squash and merge" button (GitHub). One feature/fix → one commit on the base branch. Do not use regular merge commits or rebase-and-merge unless the user explicitly asks.
- **Never force-push** to `main` / `master`. Confirm before force-pushing any branch.
- **Never skip hooks** (`--no-verify`) unless the user explicitly requests it.
- **New commits, not amends.** Don't `--amend` published commits.

## Acting carefully

- Confirm before destructive or shared-state actions (force-push, branch deletion, `terraform destroy`, `rm -rf`, GitHub-visible operations like creating issues or commenting on PRs).
- Local, reversible edits (file changes, tests, `terraform plan`) can be made without asking.
