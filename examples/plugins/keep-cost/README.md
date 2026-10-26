# piloop-keep-cost

A plugin that adds a [stop rule](https://piloop.autoresearch.inc/architecture/extension-points#stop-conditions). It
compares the fact that `spend` names, since the latest keep, with its value per keep so far. Past `escalate_at` times,
it asks a human once for each keep, and the campaign goes on. Past `stop_at` times, it stops the campaign. It is the
built-in rule `limits` with these two lines, so `max_hours`, `hold_at` and the other limits still apply. It needs a
source in `[[facts]]` that reads `spend`.

```bash
uv tool install piloop --with ./examples/plugins/keep-cost   # or: uv pip install ./examples/plugins/keep-cost
piloop plugins                                               # lists `keep_cost  piloop-keep-cost 0.1.0`
```

Select it in `piloop.toml`, then `piloop init .`:

```toml
[[facts]]
provider = "campaign"

[[facts]]
provider = "optimization"

[[facts]]
provider = "command"
command = ["uv", "run", "scripts/spend.py"]   # prints METRIC spend=<usd>
metrics = ["spend"]

[plugins.stop_rule]
provider = "keep_cost"
spend = "spend"
escalate_at = 2
stop_at = 4
hold_at = { spend = 5000 }
```
