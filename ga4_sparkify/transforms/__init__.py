"""Pure, engine-agnostic DataFrame transforms."""

from .flatten_events import flatten_events
from .flatten_sessions import flatten_sessions

__all__ = ["flatten_events", "flatten_sessions"]
