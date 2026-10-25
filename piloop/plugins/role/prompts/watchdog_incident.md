## This sweep: an incident

The daemon detected the anomalies listed under "Incidents to investigate" in the context.
Find out why each one happened. Use all the sources:

- the rows and their `events`;
- the agent's transcript and brief;
- its session, the harness's own file (`piloop cas get <agent_sessions.session_file>`);
- the process table, for anything still alive;
- the campaign's versions.

Classify the root cause:

- **harness bug**: piloop itself did the wrong thing. The daemon, the scheduler, a runtime or a harness integration
  misread an agent's state, closed or left open a run, picked a base, scored a version, or started an agent into a
  dialog. A harness bug is never "transient": it will happen again. Escalate it, with the code path if you find it.
- **environment**: the machine, credentials, a dependency, the network, a third-party tool.
- **spec**: the campaign's brief, evaluate command or config asked for something wrong or impossible.
- **model**: the agent misbehaved. It gave up early, ignored the brief, or misjudged its own result.

Write one comment per root cause, on the row it is about. Anomalies that share a cause share the comment.
Give what happened, the class, the evidence, how sure you are, and the fix you would make.
Make it an escalation when someone must act. Otherwise, make it a finding.
