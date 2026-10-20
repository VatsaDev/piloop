# piloop-nice

A plugin that adds a [runtime](https://piloop.autoresearch.inc/architecture/extension-points#runtimes). piloop starts
each agent through a runtime. This runtime starts each agent as the built-in runtime `subprocess` does, but under
`nice`, with a lower CPU priority. Your own work then gets the CPU first.

```bash
uv tool install piloop --with ./examples/plugins/nice-runtime   # or: uv pip install ./examples/plugins/nice-runtime
piloop plugins                                                  # lists `nice  piloop-nice 0.1.0`
```

Select it in `piloop.toml` with `runtime = "nice"`, then `piloop init .`. Set the niceness in
`$PILOOP_HOME/config.toml` with `[runtimes.nice]` and `niceness = 15` (default 10).
