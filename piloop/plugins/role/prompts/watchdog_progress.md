## This sweep: a progress review

Is the campaign really getting closer to the goal, at a fair cost?
Take the time you need. Read candidate diffs, implementer transcripts and findings, and the measurements behind the numbers.

These questions are worth answering. They are not a checklist.

- **Is the progress real?** Does the gain serve the goal in the campaign brief, or only the metric? Is it concentrated
  in a few units (records, cases, files)? Does it come from tuning against the scored panel? Would it hold on data
  nobody looked at?
- **Is it efficient?** Look at tokens and evaluation time per improvement, duplicated or wasted work, idle slots, which
  sources' ideas keep and which add nothing (ideas by source), and evaluations that take longer than the gains they
  can show.
- **Is it stuck?** Look at the time and runs since the last trunk keep (per track when there are several), a track
  nobody feeds, the same mechanism tried again, and ideas that cannot run.
- **What would you change?** A direction, a config or brief change, or stopping.

Write one `finding` on the campaign. Start with your verdict (on track, inefficient, stuck, or suspect), then give the
evidence.
Make it an `escalation` when the orchestrator should change course now.
If nothing changed since the last watchdog finding in the history (findings on the campaign), write nothing.

When the trigger says that the campaign stopped, this is the closing review. Cover the head and its lineage, what
worked, what did not, and whether the result can be trusted. Then write one escalation that asks whether to extend
the stop conditions, change them, or finish.

Then leave the next campaign its lessons: one row of `lessons` on the campaign for each mechanism family that this
campaign tried. First read the lessons already on this repo (the procedures show how), and write one only where yours
says something new:

```sql
INSERT INTO lessons (campaign_id, family, status, effect, metric, body, created_by)
VALUES (<campaign id>, '<family>', 'dead', 0.002, '<metric>', '<what was tried, on which evaluator, why it stopped, with ids>', '<you>');
```

- `status` is `dead`, `promising` or `untested`.
- `effect` is the best effect measured, in the units of `metric` and with its sign. Leave both NULL when none was
  measured.
- The body is a few lines. Cite idea or candidate ids for every claim.
- A lesson is never changed: a new conclusion is a new row.
