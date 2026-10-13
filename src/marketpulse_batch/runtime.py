"""Dagster code-location bootstrap; business assets are added by the job task."""

from dagster import Definitions
import os
from .settings import load_settings

load_settings(os.environ)
defs = Definitions()
