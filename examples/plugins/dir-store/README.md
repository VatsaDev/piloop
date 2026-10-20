# piloop-dir-store

A plugin that adds a [storage backend](https://piloop.autoresearch.inc/architecture/extension-points#storage-backends).
piloop copies each file of its artifact store to the storage backend that you select. This backend copies the files
to a directory, for example a network share that is mounted on the machine.

```bash
uv tool install piloop --with ./examples/plugins/dir-store   # or: uv pip install ./examples/plugins/dir-store
piloop plugins                                               # lists `dir  piloop-dir-store 0.1.0`
```

Select it in `$PILOOP_HOME/config.toml`, then restart the daemon (`piloop restart`):

```toml
[storage.remote]
kind = "dir"
root = "/mnt/archive/piloop"
```
