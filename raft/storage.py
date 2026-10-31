import asyncio
import mmap
import os
import struct
import zlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


class StorageError(Exception):
    """Base exception for all storage and WAL related errors."""

    pass


class ChecksumError(StorageError):
    """Raised when a WAL entry fails its CRC32 checksum verification."""

    pass


class CorruptionError(StorageError):
    """Raised when the WAL structure or index pointers are irrecoverably corrupted."""

    pass


@dataclass(frozen=True)
class LogEntry:
    """Immutable Raft log entry packed via strict struct layout:

    [term:uint64, index:uint64, op_type:uint8, payload_len:uint32, payload:bytes]
    Total header size: 8 + 8 + 1 + 4 = 21 bytes.
    """

    term: int
    index: int
    op_type: int
    payload: bytes

    STRUCT_FORMAT = ">QQBI"
    HEADER_SIZE = struct.calcsize(STRUCT_FORMAT)

    def pack(self) -> bytes:
        """Serializes the LogEntry into a binary frame with a trailing CRC32 checksum."""
        payload_len = len(self.payload)
        header = struct.pack(
            self.STRUCT_FORMAT,
            self.term,
            self.index,
            self.op_type,
            payload_len,
        )
        frame = header + self.payload
        checksum = zlib.crc32(frame) & 0xFFFFFFFF
        return frame + struct.pack(">I", checksum)

    @classmethod
    def unpack(cls, buffer: bytes, offset: int = 0) -> Tuple["LogEntry", int]:
        """Unpacks a LogEntry from a binary buffer starting at the given offset.

        Verifies the trailing CRC32 checksum. Returns the LogEntry instance and the
        new offset.
        """
        if len(buffer) - offset < cls.HEADER_SIZE + 4:
            raise CorruptionError("Buffer too small to contain a valid log entry header and checksum.")

        header_end = offset + cls.HEADER_SIZE
        header_data = buffer[offset:header_end]
        term, index, op_type, payload_len = struct.unpack(cls.STRUCT_FORMAT, header_data)

        total_frame_len = cls.HEADER_SIZE + payload_len
        if len(buffer) - offset < total_frame_len + 4:
            raise CorruptionError("Buffer truncated: payload length exceeds available bytes.")

        payload_end = header_end + payload_len
        payload = buffer[header_end:payload_end]

        checksum_end = payload_end + 4
        stored_checksum = struct.unpack(">I", buffer[payload_end:checksum_end])[0]

        frame_data = buffer[offset:payload_end]
        calculated_checksum = zlib.crc32(frame_data) & 0xFFFFFFFF

        if stored_checksum != calculated_checksum:
            raise ChecksumError(
                f"CRC32 mismatch at offset {offset}: stored {stored_checksum:#010x}, calculated {calculated_checksum:#010x}"
            )

        entry = cls(term=term, index=index, op_type=op_type, payload=payload)
        return entry, checksum_end


class WriteAheadLog:
    """Append-only disk-backed Write-Ahead Log utilizing memory-mapping (mmap)

    for zero-copy read access and structured fsync batching to guarantee ACID
    durability.
    """

    def __init__(self, wal_path: str, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.wal_path = wal_path
        self._loop = loop or asyncio.get_event_loop()
        self._file_obj = None
        self._fileno = -1
        self._mmap_obj: Optional[mmap.mmap] = None
        self._file_size = 0
        self._write_lock = asyncio.Lock()
        self._index_offsets: Dict[int, int] = {}
        self._open_and_recover()

    def _open_and_recover(self) -> None:
        """Opens the WAL file, creates it if missing, and performs sequential

        recovery scanning to populate the index offset map and validate CRC32
        checksums.
        """
        file_exists = os.path.exists(self.wal_path)
        if not file_exists:
            dir_name = os.path.dirname(self.wal_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            with open(self.wal_path, "wb") as f:
                pass

        self._file_obj = open(self.wal_path, "r+b")
        self._fileno = self._file_obj.fileno()
        self._file_size = os.path.getsize(self.wal_path)

        if self._file_size > 0:
            self._mmap_obj = mmap.mmap(self._fileno, 0, access=mmap.ACCESS_READ)
            self._recover_index()
        else:
            self._mmap_obj = None

    def _recover_index(self) -> None:
        """Scans the memory-mapped file sequentially, validating every frame

        and registering index-to-offset mappings. Truncates trailing corruption
        if detected.
        """
        if not self._mmap_obj:
            return

        offset = 0
        valid_bytes = 0
        mmap_len = len(self._mmap_obj)

        while offset < mmap_len:
            try:
                # Peek if we have enough space for at least header + checksum (25 bytes)
                if mmap_len - offset < LogEntry.HEADER_SIZE + 4:
                    break

                # Read header to determine payload length
                header_data = self._mmap_obj[offset : offset + LogEntry.HEADER_SIZE]
                _, index, _, payload_len = struct.unpack(LogEntry.STRUCT_FORMAT, header_data)

                frame_size = LogEntry.HEADER_SIZE + payload_len + 4
                if offset + frame_size > mmap_len:
                    break

                # Validate frame via LogEntry.unpack
                _, next_offset = LogEntry.unpack(self._mmap_obj, offset)
                self._index_offsets[index] = offset
                valid_bytes = next_offset
                offset = next_offset
            except (ChecksumError, CorruptionError, struct.error):
                # Stop recovery at the first corruption boundary (crash consistency truncation point)
                break

        # If corruption was found at the tail, truncate the file to the last valid byte
        if valid_bytes < mmap_len:
            self._mmap_obj.close()
            self._file_obj.seek(valid_bytes)
            self._file_obj.truncate()
            self._file_size = valid_bytes
            if self._file_size > 0:
                self._mmap_obj = mmap.mmap(self._fileno, 0, access=mmap.ACCESS_READ)
            else:
                self._mmap_obj = None

    async def append(self, entry: LogEntry) -> int:
        """Asynchronously appends a LogEntry to the WAL, syncing to disk.

        Returns the index of the appended entry.
        """
        async with self._write_lock:
            packed_data = entry.pack()
            current_offset = self._file_size

            # Perform blocking I/O on thread pool to avoid blocking event loop
            await self._loop.run_in_executor(None, self._sync_write, packed_data, current_offset)

            self._index_offsets[entry.index] = current_offset
            return entry.index

    def _sync_write(self, data: bytes, offset: int) -> None:
        """Blocking helper to write data to disk, update mmap, and issue fsync."""
        if self._mmap_obj:
            self._mmap_obj.close()
            self._mmap_obj = None

        self._file_obj.seek(offset)
        self._file_obj.write(data)
        self._file_obj.flush()
        os.fsync(self._fileno)

        self._file_size = os.path.getsize(self.wal_path)
        self._mmap_obj = mmap.mmap(self._fileno, 0, access=mmap.ACCESS_READ)

    async def append_batch(self, entries: List[LogEntry]) -> None:
        """Asynchronously appends a batch of LogEntries to the WAL with a single fsync."""
        if not entries:
            return

        async with self._write_lock:
            chunks = bytearray()
            offsets_to_update = []
            current_offset = self._file_size

            for entry in entries:
                packed = entry.pack()
                offsets_to_update.append((entry.index, current_offset + len(chunks)))
                chunks.extend(packed)

            await self._loop.run_in_executor(None, self._sync_write, bytes(chunks), current_offset)

            for idx, off in offsets_to_update:
                self._index_offsets[idx] = off

    def read(self, index: int) -> LogEntry:
        """Zero-copy read of a LogEntry by its Raft index using the mmap region."""
        if index not in self._index_offsets:
            raise StorageError(f"Log entry at index {index} not found in WAL index.")

        if not self._mmap_obj:
            raise StorageError("WAL mmap region is not initialized.")

        offset = self._index_offsets[index]
        entry, _ = LogEntry.unpack(self._mmap_obj, offset)
        return entry

    def read_range(self, start_index: int, end_index: int) -> List[LogEntry]:
        """Reads a range of log entries [start_index, end_index] inclusively."""
        entries = []
        for idx in range(start_index, end_index + 1):
            if idx in self._index_offsets:
                entries.append(self.read(idx))
            else:
                break
        return entries

    async def truncate(self, up_to_index: int) -> None:
        """Truncates the WAL up to the specified index (used during snapshot compaction)."""
        async with self._write_lock:
            # Find all entries with index > up_to_index and remove from index map
            keys_to_remove = [idx for idx in self._index_offsets.keys() if idx > up_to_index]
            if not keys_to_remove and not self._index_offsets:
                return

            # If we are truncating everything or up to an index that leaves entries
            surviving_indices = [idx for idx in self._index_offsets.keys() if idx <= up_to_index]

            if not surviving_indices:
                # Truncate entirely
                await self._loop.run_in_executor(None, self._full_truncate)
                self._index_offsets.clear()
                return

            min_surviving_index = min(surviving_indices)
            new_start_offset = self._index_offsets[min_surviving_index]

            # Read all surviving data
            if self._mmap_obj:
                surviving_data = self._mmap_obj[new_start_offset:]
            else:
                surviving_data = b""

            await self._loop.run_in_executor(None, self._rewrite_wal, surviving_data, surviving_indices)

    def _full_truncate(self) -> None:
        if self._mmap_obj:
            self._mmap_obj.close()
            self._mmap_obj = None
        self._file_obj.seek(0)
        self._file_obj.truncate(0)
        self._file_obj.flush()
        os.fsync(self._fileno)
        self._file_size = 0

    def _rewrite_wal(self, data: bytes, surviving_indices: List[int]) -> None:
        if self._mmap_obj:
            self._mmap_obj.close()
            self._mmap_obj = None

        self._file_obj.seek(0)
        self._file_obj.truncate(0)
        self._file_obj.write(data)
        self._file_obj.flush()
        os.fsync(self._fileno)

        self._file_size = os.path.getsize(self.wal_path)
        self._mmap_obj = mmap.mmap(self._fileno, 0, access=mmap.ACCESS_READ)

        # Rebuild index offsets for surviving entries
        new_index_offsets = {}
        offset = 0
        mmap_len = len(self._mmap_obj)

        while offset < mmap_len:
            try:
                header_data = self._mmap_obj[offset : offset + LogEntry.HEADER_SIZE]
                _, index, _, payload_len = struct.unpack(LogEntry.STRUCT_FORMAT, header_data)
                new_index_offsets[index] = offset
                _, next_offset = LogEntry.unpack(self._mmap_obj, offset)
                offset = next_offset
            except Exception:
                break

        self._index_offsets = new_index_offsets

    def close(self) -> None:
        """Safely closes file descriptors and releases memory maps."""
        if self._mmap_obj:
            self._mmap_obj.close()
            self._mmap_obj = None
        if self._file_obj:
            self._file_obj.close()
            self._file_obj = None


class StateMachine:
    """Abstract transition state dictionary protected by Read-Copy-Update (RCU)

    semantic patterns during checkpoint cycles.
    """

    def __init__(self):
        self._state: Dict[bytes, bytes] = {}
        self._lock = asyncio.Lock()
        self._last_applied_index = 0
        self._last_applied_term = 0

    async def get(self, key: bytes) -> Optional[bytes]:
        """Thread-safe lock-free read access under RCU semantics."""
        # Python references are atomic; returning dictionary lookup directly
        return self._state.get(key)

    async def apply(self, entry: LogEntry) -> bytes:
        """Applies a LogEntry transactionally to the state machine, updating

        internal RCU references and tracking applied indexes.
        """
        async with self._lock:
            # Simple key-value state machine protocol encoded in payload:
            # Op Type 1: SET (payload format: key_len(4) + val_len(4) + key + val)
            # Op Type 2: DELETE (payload format: key)
            if entry.op_type == 1:
                if len(entry.payload) >= 8:
                    key_len, val_len = struct.unpack(">II", entry.payload[:8])
                    key = entry.payload[8 : 8 + key_len]
                    val = entry.payload[8 + key_len : 8 + key_len + val_len]

                    # RCU Pattern: Construct a new dictionary copy and swap reference atomically
                    new_state = dict(self._state)
                    new_state[key] = val
                    self._state = new_state
            elif entry.op_type == 2:
                key = entry.payload
                if key in self._state:
                    new_state = dict(self._state)
                    del new_state[key]
                    self._state = new_state

            self._last_applied_index = entry.index
            self._last_applied_term = entry.term
            return entry.payload

    async def create_checkpoint(self) -> Tuple[int, int, bytes]:
        """Creates an RCU-isolated snapshot checkpoint of the current state

        dictionary and packages it into a compressed tarball/byte stream.
        """
        async with self._lock:
            # Capture consistent point-in-time snapshot of state dictionary reference
            snapshot_dict = self._state
            applied_index = self._last_applied_index
            applied_term = self._last_applied_term

        # Serialize state dictionary via length-prefixed key-value pairs compressed with zlib
        serialized_items = bytearray()
        for k, v in snapshot_dict.items():
            serialized_items.extend(struct.pack(">II", len(k), len(v)) + k + v)

        compressed_data = zlib.compress(bytes(serialized_items))
        meta_header = struct.