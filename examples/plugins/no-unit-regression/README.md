# piloop-no-unit-regression

A plugin that extends another plugin. It adds a verdict check, `no_unit_regression`, to the capability
`verdict check` that [`verdict-checks`](../verdict-checks) declares. The verdict rule `checked` of `verdict-checks`
runs it.

The check refuses a keep when a unit of the primary metric got worse. A unit is one record, file or example that the
evaluate command measures. A person then looks at the candidate. The check needs per-unit results.

```bash
uv tool install piloop --with ./examples/plugins/verdict-checks --with ./examples/plugins/no-unit-regression
```

```toml
[plugins.verdict_rule]
provider = "checked"
checks = ["no_unit_regression"]
```
