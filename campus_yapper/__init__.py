"""yapper_python

A message broker client for Python.

This package provides the Yapper class for sending and receiving events.
"""
# This file is required to make this directory a package.
# See https://docs.python.org/3/tutorial/modules.html#packages for more
# information.

from .base import Event, EventHandler, YapperInterface
from .backends.sqlite import SqliteYapper


def create(*args, **kwargs) -> YapperInterface:
    """Factory function to get a Yapper client instance."""
    return SqliteYapper(*args, **kwargs)


# The __all__ variable is used to define the public API of this module.
# See https://docs.python.org/3/tutorial/modules.html#importing-from-a-package
# for more information.
__all__ = [
    "Event",
    "EventHandler",
    "YapperInterface",
    "create",
]
