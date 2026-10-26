"""The tables of ``optimization``: its evaluators, ideas, runs, candidates, measurements and techniques."""

from pathlib import Path

from piloop.db.models import Candidate, Evaluation, Evaluator, Idea, IdeaLink, Measurement, Run, Technique
from piloop.db.schema import Schema
from piloop.db.triggers import on

TABLES = (Evaluator, Idea, IdeaLink, Run, Candidate, Measurement, Evaluation, Technique)
SCHEMA = Schema(tables=TABLES, versions=Path(__file__).parent / 'migrations', triggers=on(TABLES))
