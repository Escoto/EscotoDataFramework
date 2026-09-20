"""Layer 5 — Output: verb-based writers (Delta tables and files)."""

from data_framework.output.base import Requirements, Writer
from data_framework.output.registry import VERB_REQUIREMENTS, WRITERS

__all__ = ["Writer", "Requirements", "VERB_REQUIREMENTS", "WRITERS"]
