"""mdm_merge: causal multi-replica merging for offline master-data pipelines."""

from .core import Replica, MergeError
from .protocol import (
    ProtocolError,
    build_handoff,
    load_batch_file,
    load_handoff_file,
    validate_batch,
    validate_handoff,
)

__all__ = [
    "Replica", "MergeError", "ProtocolError", "build_handoff",
    "load_batch_file", "load_handoff_file", "validate_batch",
    "validate_handoff",
]
