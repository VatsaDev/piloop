# enwik8: compress the first 10 MB of Wikipedia

You are one worker in an autoresearch loop. The idea you must try is below this brief. Implement it in
`compressor.py`, measure it, and commit.

## The task

Make `compressor.py` compress the first 10 MB of enwik8 into fewer bytes. enwik8 is the first 100 MB of an English
Wikipedia XML dump. It is the benchmark of the Hutter Prize.

## The rules

- Edit only `compressor.py`. Do not edit `evaluate.py`, `piloop.toml`, this file or anything in `data/`.
- `compressor.py` defines `compress(data: bytes) -> bytes` and `decompress(blob: bytes) -> bytes`.
  `decompress(compress(data))` must return exactly `data`.
- `decompress` gets only the compressed bytes. It must not read the input from any other place (`data/`, the network,
  a cache). A decompressor that reads the original data is cheating, and the candidate is discarded.
- Use only the Python standard library and numpy.
- Measure with `uv run python evaluate.py`. It prints two metrics:
  - `bytes`: the compressed size plus the size of `compressor.py`. Lower is better. This is the metric to improve.
    A large model or dictionary inside `compressor.py` counts against you.
  - `seconds`: the time to compress and decompress. Lower is better. It may grow by at most 10 s per step and must
    stay below 120 s.
- Commit with `git add compressor.py && git commit -m "<what you did and why>"`. Every commit is scored. If the idea
  did not work, commit anyway and say so in the message.
- Do not stop to ask questions. Decide and commit.

## Starting points

The baseline is zlib at level 9: 3.69 MB in about 0.5 s. The standard library also has bz2 (about 2.92 MB) and lzma
(about 2.72 MB at preset 9 extreme, about 7 s). Context-mixing compressors do much better on this text, but they are
slow in pure Python. Text transforms (a dictionary of frequent words, XML-aware preprocessing) can help any back end.
