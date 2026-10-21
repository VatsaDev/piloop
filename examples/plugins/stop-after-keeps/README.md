# piloop-stop-after-keeps

A plugin that adds a [stop rule](https://piloop.autoresearch.inc/architecture/extension-points#stop-conditions). On each
tick (every 2 seconds by default), the daemon asks the stop rule whether the campaign goes on, holds, or stops. This
rule stops the campaign after a number of keeps. Use it for a campaign that wants a few good changes and no more. It
calls the built-in rule `limits` for `max_hours` and `max_tokens` of `[stop]`, and for `[spend]`, so they still
apply. The other limits of `[stop]` do not. The rule imports only the Python standard library, its contract and the
built-in rule, so it is a pure plugin.

```bash
uv tool install piloop --with ./examples/plugins/stop-after-keeps   # or: uv pip install ./examples/plugins/stop-after-keeps
piloop plugins                                                      # lists `after_keeps  piloop-stop-after-keeps 0.1.0`
```

Select it in `piloop.toml`, then `piloop init .`:

```toml
[plugins.stop_rule]
provider = "after_keeps"
keeps = 3
```
