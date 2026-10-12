---
name: piloop-setup
description: Set up or re-configure a piloop autoresearch campaign in the current repo, from any harness with a shell (Claude Code, Codex, pi, Gemini CLI, Cursor). Use when the human asks to set up, start, bootstrap or change a piloop project; do not use it in an agent that piloop launched (PILOOP_ACTOR is set).
---

# Set up a piloop campaign

You do the setup. The human answers an interview and logs in where a CLI needs a browser.

- Work in the repo of this session.
- Run every command yourself.
- After each step, report the result in one line.

## 1. Check the machine

Run each check yourself:

- `piloop --help`. If piloop is missing, install it with
  `curl -fsSL https://raw.githubusercontent.com/hiloopai/piloop/main/install.sh | sh`, then retry.
- `git config user.email`. The baseline commit needs a git identity.
- `uv --version`.
- `herdr workspace list`. Herdr gives every worker a visible pane. Without Herdr, set `runtime = "subprocess"` in step 3.
- `herdr integration status`. Herdr tells a working agent from an idle one through a hook that it installs per harness.
  - Every harness that the project enables must show `current`.
  - To fix one, run `herdr integration install pi|claude|codex`.
  - A pi worker without the hook stops with `prompt_lost`.
- For each harness the human wants, check the version and the login:
  - pi: `pi --version`, and look at `~/.pi/agent/auth.json`.
  - Claude Code: `claude --version` and `claude auth status`.
  - Codex: `codex --version` and `codex login status`.
- `entire --version`. This shows if session capture is on.
  - Session capture keeps the full session of every agent in the repo as an [Entire](https://github.com/entireio/cli)
    checkpoint. Read one with `entire checkpoint explain <workers.checkpoint_id>`.
  - It is on whenever `entire` is installed. Install it at any time, also later:
    `curl -fsSL https://entire.io/install.sh | bash`.
  - There is nothing to configure: `piloop init` writes `.entire/settings.json`. No data leaves the machine.
  - Tell the human in one line if it is on.

Fix what you can. A broken environment is yours to debug: read the error, fix the cause, check again.
The only fixes you give to the human are logins: `pi` then `/login`, `claude auth login`, `codex login`.
Ask for all of them once, together.

## 2. Interview the human

The interview is a gate. Write no file before the human answers the interview or explicitly skips it.

1. Read the repo first: the README, the data layout, the training and evaluation scripts, `git log`, the dependency
   files.
2. Form a view of the problem. A Kaggle competition, an LLM eval, a latency benchmark and a research codebase need
   different questions.
3. Choose the questions for this problem and this person. The topics below are a checklist, not a script:
   - Skip what the repo already answers. Say what you read, so that the human can object.
   - Merge, reorder and add questions that fit the problem.
   - Stop when you have enough to write the brief.
   - Do not take answers from an example project. The blank template gives the shape, not the content.

Topics to cover:

- **Objective.** The one primary metric, its direction, and what "done" is (a target value, a leaderboard position, a
  deadline).
  - Several objectives for one artifact are *tracks* of one project, not several projects. An example is the
    accuracy and the serving cost of one model.
  - Each track has its own primary metric, a `margin` and an optional `floor` that bind the candidates of the other
    tracks, and a share of the workers.
  - A yardstick (a validation dataset, the speed of the evaluator) is never a track. It changes only through a new
    evaluator version that a human approves.
- **Measurement.** The evaluate command. How long one measurement takes. Is it deterministic (fixed seeds and
  splits)? How noisy is it between identical runs? Which split is the holdout? Is there a hidden test set?
- **Guardrails.** What must not regress (latency, model size, cost). Each one becomes one more `[[metrics]]` table,
  with its direction.
- **Editable surface and forbidden moves.** The files a worker may change. The frozen files (data preparation, the
  evaluator, the tokenizer). Forbidden dependencies and techniques (no external data, no pretrained model X).
- **Budget and concurrency.** Wall-clock time per run. Compute (GPUs, cores). The total budget of the campaign (runs,
  hours or dollars). How many workers the machine can run at the same time.
- **Prior art and beliefs.** What the human tried, what they believe works, what result would surprise them.
- **Harnesses and models.** Which ones are installed and logged in (from step 1). Which ones run workers and which run
  research. The runtime: Herdr when it is available, else headless.
- **Escalation policy.** What the human wants to be asked about (a new dependency, data changes, a cost above X), and
  what workers decide alone.

How to ask:

- Use the question tool of your harness when it has one (pi: `ask`, Claude Code: `AskUserQuestion`).
- Ask one question, or a few related ones, per call. Never put the whole checklist in one form.
- Pre-fill the options from what the repo suggests. Put your best guess first. Allow a free-form answer.
- Without a question tool, ask the same questions in the chat, a few at a time.

Close the interview: read your understanding back in a few lines.
The human's confirmation is the contract for the rest of the setup.
When a later choice depends on an answer, quote that answer.

## 3. Write the files

Run `piloop init .`. When there is no `piloop.toml`, it writes `piloop.toml`, `brief.md` and an `evaluate.py` stub.

- It also writes `.entire/settings.json` and `.entire/.gitignore`.
- It keeps existing files.
- It runs `git init` and commits when necessary.
- It stops while `evaluate.py` is the untouched stub.

Then edit the files. Put each answer in one place only.

### brief.md

- The eight headings of the template follow the interview topics. Keep the headings that fit. Remove or add
  headings to fit the problem.
- Write it to the worker: "you may edit…", "never touch…".
- The editable surface, the prior art and the escalation policy go only here.
- Keep it under one page.

### piloop.toml

Map each answer to its keys:

- `name`.
- **Objective and guardrails → `[[metrics]]`**, one table for each metric.
  - The objective: `role = "primary"`.
  - Each guardrail: `role = "guardrail"` with a `margin` and an optional hard `floor`. The `margin` is the regression
    that the human accepts, in the units of the metric.
  - The daemon enforces margins and floors. Never write them only in `brief.md`.
- **Several tracks:**
  - Each primary metric names its `track`.
  - Each primary has a `margin`: what a keep on another track may give up per step.
  - Each primary may have a `floor`: a value it never goes past. Only an edit to `piloop.toml` moves it.
  - Each track has a `[tracks.<name>]` table with its `share` (the shares sum to 1) and an optional `addendum` (a brief
    file for the runs of that track).
  - piloop refuses `[stop] target` when there are several tracks.
- **Measurement → `evaluate`, `evaluate_timeout`.**
- **Budget → `[roles.worker]` `timeout` and `max_concurrent`, and `[roles.research]`.**
- **Prior art → `[roles.research]` `context`.** List the error analyses and evaluation reports of the repo (where the
  current system loses to a reference). Every research sweep starts from them.
- **Outside techniques → `[roles.research.scout]`**, only when the human wants them.
  - `interval` is in seconds and has no default.
  - The X search needs `[mcp.xai]` and its key.
- **Harnesses → `harnesses`, `[model]`, `runtime`.** Remove the harnesses that the human does not have.
- **`link`:** gitignored data or virtualenvs that the evaluate command needs inside a worktree.
- **Objective target and budget → `[stop]`:** `target`, `max_runs`, `max_hours`, and `plateau` when the human wants the
  campaign to end by itself. Leave `[stop]` out only if the human explicitly wants an open-ended campaign.
- **Money spent outside piloop (a GPU cloud, a paid API) → `[spend]`:**
  - `command` prints the spend so far in USD.
  - `notices` are thresholds that post a note.
  - `limit` is hard: at the limit the daemon stops launches. Never write the budget only in a brief.
  - Put the secrets of the command in the environment of the daemon, never in the repo.

The file comments every key with its default. https://piloop.autoresearch.inc/reference/configuration documents every
key, and https://piloop.autoresearch.inc/concepts/roles documents the `[roles.*]` tables.

### The evaluate command

Wire the existing measurement, or write `evaluate.py`. The contract:

- It runs from the repo root of a clean worktree.
- It prints `METRIC <name>=<value>` for every metric in `piloop.toml`.
- It exits 0. A non-zero exit is recorded as a crash.
- It is deterministic: fixed seeds and splits.

Run it once now. Compare the METRIC lines and the duration with what the human said about measurement.

Show both files to the human. Ask once more before you register.

## 4. Register

Run `piloop init .` again. It commits your edits, measures HEAD as the baseline and registers the project.

Tell the human the baseline value in one line. A surprising value means that the evaluate command is wrong.

## 5. Start the daemon

Run `piloop serve --detach`. The log is at `$PILOOP_HOME/serve.log`. `piloop stop` stops the daemon.

Run `piloop status <name>`. It shows the pid of the daemon and the project.

## 6. Start the backlog

Do one of these:

- Add two or three ideas with the human: `piloop idea add <name> "<title>" --hypothesis "…" --priority 1`.
- Ask for a research sweep: `piloop launch <name> research`.

## 7. Choose the orchestrator

While the campaign runs, someone must answer escalations and record decisions. That is you: follow the
`piloop-operate` skill and its loop on `piloop inbox wait`. Run it in a background subagent if the human wants this
session back. Ask the human which, in one question.

## Re-configure an existing project

1. Run the same interview, but pre-fill it from the current `brief.md` and `piloop.toml`, not from guesses. The first
   option you offer is always what the project says now.
2. Ask only about what the human wants to change.
3. Apply the change to both files and show them.
4. Run `piloop init .` again. It reads the config again.

A changed evaluate command or metric set makes a new evaluator, so old scores never count as new ones.

## What happens next

- `piloop status` prints the head, the running agents, the last candidates and the items in the human's inbox.
- `piloop watch` shows the same, live, in the human's terminal.
- `piloop sql "<statement>"` runs SQL.
- Workers run as tabs in the workspace of this project in the `piloop` Herdr session. `piloop attach <worker_id>`
  focuses one.
- Every commit a worker makes becomes a candidate, measured by the evaluate command.
- With `entire` installed, `entire checkpoint explain <id>` on the checkpoint of a session tells why a commit was made.
- Escalations go to the human's inbox. The orchestrator (any harness that runs `piloop-operate` and wakes on
  `piloop inbox wait`) decides them, with the human when they are present. It records there every decision it takes
  for the human.
