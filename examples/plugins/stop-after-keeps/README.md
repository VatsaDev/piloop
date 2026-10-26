# piloop-stop-after-keeps

A plugin that adds a [stop rule](https://piloop.autoresearch.inc/plugins/rules#stop-rules). On each
pass, the daemon asks the stop rule whether the campaign goes on, holds, or stops. This
rule stops the campaign after a number of keeps. Use it for a campaign that wants a few good changes and no more. It
is the built-in rule `limits` with one more limit, so `max_hours`, `hold_at` and the other limits still apply. The
rule imports only its contract and the built-in rule, so it is a pure plugin.

```bash
uv tool install piloop --with ./examples/plugins/stop-after-keeps   # or: uv pip install ./examples/plugins/stop-after-keeps
piloop plugins                                                      # lists `after_keeps  piloop-stop-after-keeps 0.1.0`
```

Select it in `piloop.toml`, then `piloop init .`:

```toml
[plugins.stop_rule]
provider = "after_keeps"
keeps = 3
max_hours = 12
```
