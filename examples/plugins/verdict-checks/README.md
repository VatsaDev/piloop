# piloop-verdict-checks

A plugin that other plugins can extend.

- It declares a capability, `verdict check`. A verdict check looks at a candidate that the built-in verdict rule
  `bootstrap` keeps, and can refuse the keep.
- It adds a verdict rule, `checked`. After piloop measures a candidate, it asks `checked` whether to keep it.
  `checked` calls `bootstrap`, then each verdict check that its parameter `checks` names. When a check refuses,
  `checked` returns `interesting` with the reason of the check.

Other plugins add verdict checks. [`no-unit-regression`](../no-unit-regression) adds one.

```bash
uv tool install piloop --with ./examples/plugins/verdict-checks --with ./examples/plugins/no-unit-regression
piloop plugins          # lists `verdict checks (pure tier, piloop_verdict_checks.checks)` and its provider
```

Select it in `piloop.toml`, then `piloop init .`:

```toml
[plugins.verdict_rule]
provider = "checked"
checks = ["no_unit_regression"]
```
