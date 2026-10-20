# piloop-min-gain

A plugin that adds a [verdict rule](https://piloop.autoresearch.inc/architecture/extension-points#verdict-rules).
After piloop measures a candidate, it asks the verdict rule whether to keep the candidate. This rule keeps a candidate
only when it beats its bar by at least `min_gain` on the primary metric. The bar is the candidate that it must beat.

- A gain of at least `min_gain` is `keep`.
- A smaller gain is `interesting`.
- No gain is `discard`.
- A guardrail metric past its floor is `discard`.

Use it when a tiny gain is not worth a keep, for example on a compressed size. The rule imports only the Python
standard library and its contract, so it is a pure plugin.

```bash
uv tool install piloop --with ./examples/plugins/min-gain   # or: uv pip install ./examples/plugins/min-gain
piloop plugins                                              # lists `min_gain  piloop-min-gain 0.1.0`
```

Select it in `piloop.toml`, then `piloop init .`:

```toml
[plugins.verdict_rule]
provider = "min_gain"
min_gain = 100   # in the primary metric's units: 100 bytes for a size
```
