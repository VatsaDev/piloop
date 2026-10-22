# piloop-inline

A plugin that adds an [executor](https://piloop.autoresearch.inc/architecture/extension-points#executors). It runs
each job at once, in a thread of the daemon: no leases, no evaluation slots, and nothing survives a restart.

```bash
uv tool install piloop --with ./examples/plugins/inline-executor   # or: uv pip install ./examples/plugins/inline-executor
piloop plugins                                                     # lists `inline  piloop-inline 0.1.0`
```

Select it in `$PILOOP_HOME/config.toml` with `[plugins.executor]` and `provider = "inline"`, then `piloop restart`.
