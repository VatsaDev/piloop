## Cost track

Decrease `cost_seconds`. Do not change the predictions, or change them as little as possible. Time `solve.py` first
(`python -X importtime solve.py …`, `cProfile`), then remove what costs the most: heavy imports, repeated work, a model
that is too large. The accuracy margin bounds the 95% worst case of the paired change, not its mean, so a cost keep
can change only a few predictions.
