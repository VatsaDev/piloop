# <campaign>: <one line that says what to improve and how it is measured>

Every agent reads this brief, research and the watchdog too. Write the rules to the implementer, but not its role:
piloop tells each agent what it is. Keep it short and concrete. The sections below are a suggestion: remove or add
sections to fit the problem. `piloop.toml` holds the config. `evaluate.py` gives the score.

## 1. Objective

The primary metric from `piloop.toml` (`score`) and its direction. Where the baseline is. What "done"
means: a target value, a leaderboard position or a date.

## 2. Measurement

How to run the evaluate command (`uv run python evaluate.py`) and how long it takes. Whether it is deterministic
(seeds, splits) and how much two identical runs differ. Which split is the holdout. Whether a hidden test set exists.

## 3. Guardrails

What must not get worse (latency, size, cost), and by how much. Each one is a `[[metrics]]` entry with a direction.

## 4. What an implementer may change

- The files an implementer may edit, and the files it must not edit (the evaluate command, data preparation, this brief).
- Dependencies and techniques that are not allowed (for example: no external data, no pretrained models).
- `workspace/` is shared by all implementers and kept between runs. Use it for notes, cached features and plots. It is not
  committed.

## 5. Budget

The compute (GPUs, cores). The total budget of the campaign in runs,
hours or money. How many implementers run at the same time.

## 6. Baselines and prior work

What was tried and its score. What is expected to work. What would be a surprise. This stops research from proposing
the same idea again and tells an implementer where to start.

## 7. Harnesses and models

Which coding agents run the implementers and which run research. The runtime: background processes (`subprocess`, the default) or a package's runtime.

## 8. Escalation

What needs the human's decision: for example a new dependency, a change to the data, or a cost above a limit. An implementer
does not do these things. It leaves them undone and names the decision in its `finding` comment. Research raises the
question as an `escalation` comment. For everything else: decide, commit, and record the result in a `finding`
comment on the idea. A crash or a changed score is recorded against the run.
