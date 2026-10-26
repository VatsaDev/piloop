# piloop-min-gain

A plugin that adds a [verdict rule](https://piloop.autoresearch.inc/plugins/rules#verdict-rules).
After piloop measures a candidate, it asks the verdict rule whether to keep the candidate. This rule calls the
built-in rule, `bootstrap`, and changes one thing: a keep whose gain is below `min_gain` becomes `interesting`. The
gain is the difference between the candidate and its bar, the candidate that it must beat.

The confidence, the guardrails and the reasons stay those of `bootstrap`. Use the rule when a tiny gain is not worth
a keep, for example on a compressed size. The rule imports only the Python standard library, its contract and
`bootstrap`, so it is a pure plugin.

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
