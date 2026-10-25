"""Generate ``docs/reference/openapi.json``: the OpenAPI document of the API, which ``GET /v1/openapi.json`` serves."""

import json

from piloop.daemon.api import openapi
from scripts.generate import ROOT

PATH = ROOT / 'docs' / 'reference' / 'openapi.json'


def generate() -> str:
    """Return the document of the routes of ``piloop/daemon/api.py``, indented, with its keys in order."""
    return json.dumps(openapi(), indent=2) + '\n'
