# Operating piloop

This file is for a coding agent that a human asks to set up, run or review a piloop campaign. To change piloop's own
code, read [CONTRIBUTING.md](CONTRIBUTING.md) instead.

piloop runs an autoresearch campaign on a git repo. A daemon starts coding agents (Claude Code, Codex or pi). Each
agent tries an idea in its own git worktree. piloop scores every commit with the project's evaluate command and keeps
what beats the head. You, the agent that the human talks to, set up the campaign and act as its orchestrator.

## The skills

Each task has an [Agent Skill](https://agentskills.io). Read the one that matches the task. Each `SKILL.md` is
complete on its own.

| Skill | Use it to |
|---|---|
| [piloop-setup](skills/piloop-setup/SKILL.md) | Set up or change a campaign in the current repo: check the machine, interview the human, write `brief.md` and `piloop.toml`, register the project, start the daemon. |
| [piloop-operate](skills/piloop-operate/SKILL.md) | Orchestrate a running campaign: the inbox loop, ideas and priorities, escalations and decisions, sweeps, steering workers. |
| [piloop-review](skills/piloop-review/SKILL.md) | Review a campaign during or after a run: are the gains real, did an agent game the evaluation, where did the time and tokens go. |

`piloop install-skills` copies these skills into `~/.claude/skills` (Claude Code) and `~/.agents/skills` (Codex, pi,
Gemini CLI, Cursor), for each of these harnesses on the `PATH`. The installer and `piloop upgrade` run it, so the
skills always match the installed CLI. `piloop doctor` reports a missing or old copy.

## The interface is the CLI

The database is the contract, and the `piloop` CLI is the only way in:

- `piloop sql "<one statement>"` reads and writes the database.
- `piloop schema` prints the conventions and every table. Read it before your first write.
- `piloop status` shows the state of each project.
- `piloop inbox` lists what waits for a decision.
- `piloop inbox wait` blocks until something needs you.
- `piloop --help` lists every other command.

## Rules

- Run every command as `PILOOP_ACTOR=orchestrator piloop …`. Your writes are then signed as yours, and
  `piloop inbox wait` skips them.
- Record every decision that you take for the human as a `decision` comment with a priority. The human reviews and
  revises these decisions in the inbox. You do not have to wait for the human.
- If `PILOOP_ACTOR` is already set to a `worker#`, `watchdog#` or `research#` id, piloop started you. Follow your
  brief, not these skills.

## Documentation

The documentation is at [piloop.autoresearch.inc](https://piloop.autoresearch.inc). It is also in `docs/` in this repo.
