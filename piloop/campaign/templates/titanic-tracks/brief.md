# Titanic survival: accurate, and cheap to run

## The rules

- Edit only `solve.py`. Do not edit `evaluate.py`, `data/`, `briefs/` or this file.
- piloop runs `python solve.py --train <csv> --test <csv> --out <csv>`. It must write a CSV with the columns
  `PassengerId,Survived` and one row for each test row.
- Measure with `uv run python evaluate.py`. It prints `{"metrics": {"accuracy": <float>, "cost_seconds": <float>}, …}`
  (the CPU seconds that `solve.py` takes to train and predict, on one thread). Run it before you commit. If it fails,
  the candidate is a crash.
- Use only pandas, numpy and scikit-learn (see `pyproject.toml`). Do not add dependencies.
- `workspace/` is shared by all implementers and kept between runs. Use it for notes, cached features and plots. It is not
  committed.
- Commit with `git add solve.py && git commit -m "<what you did and why>"`. Every commit is scored, and the message
  records what you tried. If the idea did not work, commit anyway, say so, and leave a `finding` comment on the idea.
- Do not stop to ask questions. Decide and commit.

## Two tracks, one trunk

The campaign has two objectives. A keep on either track must improve its own track's metric and hold the other one:
an accuracy keep may not make `solve.py` much slower, and a cost keep may not make it much less accurate. Your track,
and the margin and floor you must hold, are below.

## Context

Kaggle's Titanic. The features are Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin and Embarked. Strong
signals: sex, class, fare, the title in the name, family size, and age with good imputation. About 0.83 holdout
accuracy is a good ceiling for tabular models. Most of the baseline's CPU cost is the import of pandas and
scikit-learn, not the fit.
