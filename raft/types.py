from __future__ annotations

import asyncio
import struct
import enum
from dataclasses import dataclass, field
from typing import Final, Dict, Any, Optional

# Constants for binary layout and protocol framing
STRUCT_LOG_ENTRY_HEADER_FORMAT: Final[str] = ">QQBI"
STRUCT_LOG_ENTRY_HEADER_SIZE: Final[int] = struct.calcsize(STRUCT_LOG_ENTRY_HEADER_FORMAT)


class OpType(enum.IntEnum):
    """Enumeration of operation types supported by the Raft log replication engine."""
    NOOP = 0x00
    CLIENT_WRITE = 0x01
    CONFIG_CHANGE = 0x02
    CHECKPOINT_MARKER = 0x03


class NodeRole(enum.IntEnum):
    """Finite State Machine roles for Raft consensus nodes."""
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


@dataclass(frozen=True, slots=True)
class LogEntry:
    """Immutable Raft Log Entry packed via a strict structural layout:
    [term:uint64, index:uint64, op_type:uint8, payload_len:uint32, payload:bytes]
    """
    term: int
    index: int
    op_type: OpType
    payload: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.term, int) or self.term < 0:
            raise ValueError(f"Invalid term value: {self.term}. Must be a non-negative uint64.")
        if not isinstance(self.index, int) or self.index < 0:
            raise ValueError(f"Invalid index value: {self.index}. Must be a non-negative uint64.")
        if not isinstance(self.op_type, (int, OpType)):
            raise TypeError(f"Invalid op_type type: {type(self.op_type)}")
        if not isinstance(self.payload, bytes):
            raise TypeError(f"Payload must be bytes, got {type(self.payload)}")
        if len(self.payload) > 0xFFFFFFFF:
            raise ValueError(f"Payload length {len(self.payload)} exceeds max uint32 size.")

    def pack(self) -> bytes:
        """Serializes the immutable log entry into its binary representation with structural layout."""
        payload_len = len(self.payload)
        header = struct.pack(
            STRUCT_LOG_ENTRY_HEADER_FORMAT,
            self.term,
            self.index,
            int(self.op_type),
            payload_len
        )
        return header + self.payload

    @classmethod
    def unpack(cls, buffer: bytes) -> tuple[LogEntry, int]:
        """Deserializes a LogEntry from a binary buffer, returning the instance and total bytes consumed."""
        if len(buffer) < STRUCT_LOG_ENTRY_HEADER_SIZE:
            raise BufferError(
                f"Buffer length {len(buffer)} is smaller than required header size {STRUCT_LOG_ENTRY_HEADER_SIZE}."
            )
        
        term, index, op_val, payload_len = struct.unpack(
            STRUCT_LOG_ENTRY_HEADER_FORMAT,
            buffer[:STRUCT_LOG_ENTRY_HEADER_SIZE]
        )
        
        total_size = STRUCT_LOG_ENTRY_HEADER_SIZE + payload_len
        if len(buffer) < total_size:
            raise BufferError(
                f"Incomplete payload: buffer has {len(buffer)} bytes, expected {total_size} bytes."
            )
            
        payload = buffer[STRUCT_LOG_ENTRY_HEADER_SIZE:total_size]
        
        try:
            op_type = OpType(op_val)
        except ValueError:
            op_type = OpType.NOOP

        entry = cls(
            term=term,
            index=index,
            op_type=op_type,
            payload=payload
        )
        return entry, total_size


@dataclass
class PersistentState:
    """Raft persistent state on all servers (updated on stable storage before responding to RPCs)."""
    current_term: int = 0
    voted_for: Optional[str] = None
    log: list[LogEntry] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.current_term, int) or self.current_term < 0:
            raise ValueError("current_term must be a non-negative integer.")


@dataclass
class VolatileStateLeader:
    """Volatile state on leaders (reinitialized after election)."""
    next_index: dict[str, int] = field(default_factory=dict)
    match_index: dict[str, int] = field(default_factory=dict)


@dataclass
class VolatileStateServer:
    """Volatile state on all servers."""
    commit_index: int = 0
    last_applied: int = 0
    role: NodeRole = NodeRole.FOLLOWER
    leader_id: Optional[str] = None
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    condition: asyncio.Condition = field(default_factory=lambda: asyncio.Condition(asyncio.Lock()))

    def __post_init__(self) -> None:
        if not isinstance(self.commit_index, int) or self.commit_index < 0:
            raise ValueError("commit_index must be non-negative.")
        if not isinstance(self.last_applied, int) or self.last_applied < 0:
            raise ValueError("last_applied must be non-negative.")