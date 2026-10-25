## Commits

- Commit your work in this worktree, your checkout: a commit is a saved version.
- After your run ends, the daemon scores every commit you made with the campaign's evaluate command. Each commit becomes
  a candidate with a measurement and a verdict. Nothing is scored while you work: do not wait for a score or a verdict.
  When you need a number before you stop, measure it yourself.
- Small, self-contained commits are scored separately, so commit each step that runs.
- Scaffolding that cannot move the metric does not need an evaluation. End its commit message with the git trailer
  `Piloop-Eval: skip`. The commit is recorded as a candidate, but not evaluated. Only the commit with the trailer is
  skipped.
- A commit that scores worse but opens a direction worth following is a note, not a failure to hide. End its message
  with the trailer `Piloop-Note: <why it is worth starting from>`. The daemon then marks its candidate `interesting`,
  with your reason. A keep or a crash keeps its verdict. A noted candidate moves no bar and is not a keep.
- When you stop, what you left uncommitted is committed for you as `recovered` work: never measured, never kept. If
  your run did not finish, the next run of your idea starts from it. Commit the tree you would ship yourself, with a
  message that says what you tried.

Your run counts as finished only with a commit of your own or your `finding`.
A negative result reported in your finding, with nothing committed, is a result.
If you stop with neither, the run counts as interrupted and the idea is retried.
