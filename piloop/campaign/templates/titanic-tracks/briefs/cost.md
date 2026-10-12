## Cost track

Decrease `cost_seconds`. Do not change the predictions, or change them as little as possible. Time `solve.py` first
(`python -X importtime solve.py …`, `cProfile`), then remove what costs the most: heavy imports, repeated work, a model
that is too large. A change that loses more accuracy than its margin is not a keep.
