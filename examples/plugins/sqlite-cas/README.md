# piloop-sqlite-cas

A plugin that adds a [CAS provider](https://piloop.autoresearch.inc/plugins/infrastructure#cas-providers). Its
store is covalence's `Cas.sqlite`, as it is: all the blobs in one SQLite file.

```bash
uv tool install piloop --with ./examples/plugins/sqlite-cas   # or: uv pip install ./examples/plugins/sqlite-cas
piloop plugins                                                # lists `sqlite  piloop-sqlite-cas 0.1.0`
```

Select it in `$PILOOP_HOME/config.toml`, then restart the daemon (`piloop restart`):

```toml
[plugins.cas]
provider = "sqlite"
path = "/var/lib/piloop/blobs.sqlite"
```
