# piloop-pick-by-priority

A [pure](https://piloop.autoresearch.inc/architecture/extension-points#pure-plugins) plugin: data in, data out. It
picks the next idea to run: the highest priority, the one tried least first. It imports only the standard library and
its contract (`contract.py`), so the same contract could be served from another process or by a Wasm component.

piloop does not read the `piloop.example.pickers` group yet: planning is not a plugin capability. piloop's tests
declare the capability, install this package and pick through it:

```bash
uv pip install ./examples/plugins/pick-by-priority
uv run pytest tests/test_plugins.py -k pure
```
