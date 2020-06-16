"""
Logs module has only one class - LogsSender.
There is no need to aggregate logs of wrap them, they're wrapped on the
client's side.
"""
from ._sender import LogsSender

__all__ = ["LogsSender"]
