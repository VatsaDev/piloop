"""Baseline: logistic regression on sex and class. Implementers edit this file."""

import argparse

import pandas as pd
from sklearn.linear_model import LogisticRegression

FEATURES = ['female', 'Pclass']


def features(df: pd.DataFrame) -> pd.DataFrame:
    """The model's inputs, one row per passenger."""
    return pd.DataFrame({'female': (df['Sex'] == 'female').astype(int), 'Pclass': df['Pclass']})


def main() -> None:
    """Fit on the train split, write one prediction per test row."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', required=True)
    parser.add_argument('--test', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()

    train, test = pd.read_csv(args.train), pd.read_csv(args.test)
    model = LogisticRegression().fit(features(train)[FEATURES], train['Survived'])
    preds = model.predict(features(test)[FEATURES])
    pd.DataFrame({'PassengerId': test['PassengerId'], 'Survived': preds}).to_csv(args.out, index=False)


if __name__ == '__main__':
    main()
