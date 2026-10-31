import asyncio
import hashlib
import json
import logging
import os
import struct
import tarfile
import tempfile
from typing import AsyncIterator, Dict, Any, Optional, Tuple

logger = logging.getLogger("raft-core-engine.snapshot")

class SnapshotError(Exception):
    """Base exception for all snapshot and compaction related failures."""
    pass

class SnapshotChecksumError(SnapshotError):
    """Raised when snapshot payload integrity verification fails via SHA-256."""
    pass

class SnapshotVersionError(SnapshotError):
    """Raised when snapshot header version is incompatible with the engine."""
    pass

class SnapshotManager:
    """
    Manages state machine snapshots, atomic file replacement, WAL truncation,
    and chunked streaming transfers across network partitions for Raft nodes.
    
    Snapshot File Binary Layout:
        [ Magic: 4 bytes ('RAFT') ]
        [ Version: uint32 (big-endian) ]
        [ Last Included Index: uint64 (big-endian) ]
        [ Last Included Term: uint64 (big-endian) ]
        [ Checksum: 32 bytes (SHA-256 of payload tarball) ]
        [ Payload Length: uint64 (big-endian) ]
        [ Payload: tar.gz compressed bytes ]
    """
    
    MAGIC = b'RAFT'
    VERSION = 1
    HEADER_FORMAT = '>4sQ T Q 32s Q' # Note: Spacing handled in explicit unpack/pack
    HEADER_STRUCT = struct.Struct('>4s I Q Q 32s Q')
    CHUNK_SIZE = 64 * 1024  # 64 KB chunks for network streaming

    def __init__(self, snapshot_dir: str, wal_manager: Any, state_machine: Any) -> str:
        self.snapshot_dir = os.path.abspath(snapshot_dir)
        self.wal_manager = wal_manager
        self.state_machine = state_machine
        self.lock = asyncio.Lock()
        
        os.makedirs(self.snapshot_dir, exist_ok=True)

    async def create_snapshot(self, last_included_index: int, last_included_term: int) -> str:
        """
        Takes an atomic point-in-time checkpoint of the StateMachine using RCU semantics,
        serializes it into a compressed tarball, computes cryptographic integrity hashes,
        flushes to disk atomically, and truncates the underlying WAL.
        """
        async with self.lock:
            temp_tar_fd, temp_tar_path = tempfile.mkstemp(dir=self.snapshot_dir, suffix='.tar.gz')
            os.close(temp_tar_fd)

            try:
                # 1. Extract state machine data safely using RCU-like snapshot pattern
                state_data = await self.state_machine.get_checkpoint()
                
                # 2. Serialize state to a temporary JSON file inside the tarball structure
                state_json_bytes = json.dumps(state_data, sort_keys=True, indent=2).encode('utf-8')
                
                def _build_tarball() -> bytes:
                    with tarfile.open(temp_tar_path, "w:gz") as tar:
                        tarinfo = tarfile.TarInfo(name="state_machine.json")
                        tarinfo.size = len(state_json_bytes)
                        import io
                        tar.addfile(tarinfo, io.BytesIO(state_json_bytes))
                    
                    with open(temp_tar_path, "rb") as f:
                        return f.read()

                loop = asyncio.get_running_loop()
                payload_bytes = await loop.run_in_executor(None, _build_tarball)
                
                # 3. Compute SHA-256 Checksum of the payload
                hasher = hashlib.sha256()
                hasher.update(payload_bytes)
                checksum = hasher.digest()

                # 4. Construct snapshot file name
                snapshot_filename = f"snapshot-{last_included_index}-{last_included_term}.snap"
                final_path = os.path.join(self.snapshot_dir, snapshot_filename)
                temp_snap_fd, temp_snap_path = tempfile.mkstemp(dir=self.snapshot_dir, suffix='.tmp')
                os.close(temp_snap_fd)

                # 5. Write binary header and payload to temp file
                header = self.HEADER_STRUCT.pack(
                    self.MAGIC,
                    self.VERSION,
                    last_included_index,
                    last_included_term,
                    checksum,
                    len(payload_bytes)
                )

                def _write_snapshot() -> None:
                    with open(temp_snap_path, "wb") as f:
                        f.write(header)
                        f.write(payload_bytes)
                        f.flush()
                        os.fsync(f.fileno())

                await loop.run_in_executor(None, _write_snapshot)

                # 6. Atomic rename to replace/create active snapshot
                os.replace(temp_snap_path, final_path)
                logger.info(f"Successfully created snapshot at index {last_included_index}, term {last_included_term}: {final_path}")

                # 7. Truncate WAL up to last_included_index safely
                await self.wal_manager.truncate_prefix(last_included_index, last_included_term)

                return final_path

            except Exception as e:
                logger.error(f"Failed to create snapshot at index {last_included_index}: {e}")
                raise SnapshotError(f"Snapshot generation failed: {e}") from e
            finally:
                if os.path.exists(temp_tar_path):
                    try:
                        os.unlink(temp_tar_path)
                    except OSError:
                        pass

    async def restore_latest_snapshot(self) -> Optional[Tuple[int, int]]:
        """
        Scans the snapshot directory, discovers the highest index snapshot,
        verifies its cryptographic checksum, and restores the state machine.
        Returns (last_included_index, last_included_term) if successful, else None.
        """
        async with self.lock:
            snapshots = []
            for filename in os.listdir(self.snapshot_dir):
                if filename.endswith(".snap") and filename.startswith("snapshot-"):
                    try:
                        parts = filename[:-5].split("-")
                        idx = int(parts[1])
                        term = int(parts[2])
                        snapshots.append((idx, term, filename))
                    except (IndexError, ValueError):
                        continue

            if not snapshots:
                logger.info("No existing snapshots found on disk.")
                return None

            # Sort descending by index, then term
            snapshots.sort(key=lambda x: (x[0], x[1]), reverse=True)
            latest_idx, latest_term, latest_filename = snapshots[0]
            target_path = os.path.join(self.snapshot_dir, latest_filename)

            logger.info(f"Restoring from latest snapshot: {target_path} (Index: {latest_idx}, Term: {latest_term})")

            loop = asyncio.get_running_loop()
            
            def _read_and_verify() -> Dict[str, Any]:
                with open(target_path, "rb") as f:
                    header_size = self.HEADER_STRUCT.size
                    header_data = f.read(header_size)
                    if len(header_data) < header_size:
                        raise SnapshotError("Truncated snapshot file header.")

                    magic, version, idx, term, checksum, payload_len = self.HEADER_STRUCT.unpack(header_data)

                    if magic != self.MAGIC:
                        raise SnapshotError(f"Invalid snapshot magic bytes: {magic}")
                    if version != self.VERSION:
                        raise SnapshotVersionError(f"Unsupported snapshot version: {version}")

                    payload_bytes = f.read(payload_len)
                    if len(payload_bytes) != payload_len:
                        raise SnapshotError("Truncated snapshot payload data.")

                    # Verify Checksum
                    hasher = hashlib.sha256()
                    hasher.update(payload_bytes)
                    computed_checksum = hasher.digest()

                    if computed_checksum != checksum:
                        raise SnapshotChecksumError(
                            f"Snapshot checksum mismatch! Expected {checksum.hex()}, got {computed_checksum.hex()}"
                        )

                    # Extract state machine json from tarball
                    with tempfile.TemporaryDirectory() as tmpdir:
                        tar_path = os.path.join(tmpdir, "restore.tar.gz")
                        with open(tar_path, "wb") as tf:
                            tf.write(payload_bytes)

                        with tarfile.open(tar_path, "r:gz") as tar:
                            member = tar.getmember("state_machine.json")
                            f_obj = tar.extractfile(member)
                            if f_obj is None:
                                raise SnapshotError("Failed to extract state_machine.json from snapshot archive.")
                            state_data = json.load(f_obj)
                            return state_data

            try:
                state_data = await loop.run_in_executor(None, _read_and_verify)
                await self.state_machine.restore_checkpoint(state_data)
                logger.info(f"State machine successfully restored to index {latest_idx}, term {latest_term}.")
                return latest_idx, latest_term
            except Exception as e:
                logger.error(f"Failed to restore snapshot {target_path}: {e}")
                raise SnapshotError(f.format(e)) from e

    async def stream_snapshot(self, snapshot_path: str) -> AsyncIterator[bytes]:
        """
        Asynchronously streams a snapshot file in chunks over the network transport layer,
        prefixing chunks with metadata headers for streaming integrity verification.
        """
        if not os.path.exists(snapshot_path):
            raise SnapshotError(f"Snapshot path does not exist for streaming: {snapshot_path}")

        file_size = os.path.getsize(snapshot_path)
        
        # Yield metadata frame first: [Total Size: uint64]
        yield struct.pack('>Q', file_size)

        loop = asyncio.get_running_loop()

        def _file_chunk_generator():
            with open(snapshot_path, "rb") as f:
                while True:
                    chunk = f.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    yield chunk

        # Stream file chunks asynchronously
        chunks = _file_chunk_generator()
        while True:
            chunk = await loop.run_in_executor(None, next, chunks, None)
            if chunk is None:
                break
            yield chunk

    async def receive_snapshot(self, metadata: Dict[str, Any], chunk_stream: AsyncIterator[bytes]) -> Tuple[int, int]:
        """
        Receives an incremental chunked snapshot transfer from a leader node, writes it safely
        to a temporary staging area, verifies integrity via checksum validation, commits it to disk,
        and triggers a full state machine restoration.
        """
        async with self.lock:
            temp_snap_fd, temp_snap_path = tempfile.mkstemp(dir=self.snapshot_dir, suffix='.recv.tmp')
            os.close(temp_snap_fd)

            total_received = 0
            expected_size = None

            try:
                loop = asyncio.get_running_loop()

                def _write_chunk(path: str, data: bytes) -> None:
                    with open(path, "ab") as f:
                        f.write(data)

                async for chunk in chunk_stream:
                    if expected_size is None:
                        # First 8 bytes of the stream indicate total file size
                        if len(chunk) < 8:
                            raise SnapshotError("Malformed snapshot stream: missing size header.")
                        expected_size = struct.unpack('>Q', chunk[:8])[0]
                        remaining = chunk[8:]
                        if remaining:
                            total_received += len(remaining)
                            await loop.run_in_executor(None, _write_chunk, temp_snap_path, remaining)
                    else:
                        total_received += len(chunk)
                        await loop.run_in_executor(None, _write_chunk, temp_snap_path, chunk)

                if expected_size is not None and total_received != expected_size:
                    raise SnapshotError(f"Incomplete snapshot transfer: expected {expected_size} bytes, received {total_received}.")

                # Validate and parse received snapshot file structure
                def _validate_and_parse() -> Tuple[int, int, str]:
                    with open(temp_snap_path, "rb") as f:
                        header_size = self.HEADER_STRUCT.size
                        header_data = f.read(header_size)
                        if len(header_data) < header_size:
                            raise SnapshotError("Received truncated snapshot header.")

                        magic, version, idx, term, checksum, payload_len = self.HEADER_STRUCT.unpack(header_data)

                        if magic != self.MAGIC:
                            raise SnapshotError(f"Invalid received snapshot magic bytes: {magic}")
                        if version != self.VERSION:
                            raise SnapshotVersionError(f"Unsupported received snapshot version: {version}")

                        payload_bytes = f.read(payload_len)
                        if len(payload_bytes) != payload_len:
                            raise SnapshotError("Received truncated snapshot payload data.")

                        hasher = hashlib.sha256()
                        hasher.update(payload_bytes)
                        computed_checksum = hasher.digest()

                        if computed_checksum != checksum:
                            raise SnapshotChecksumError(
                                f"Received snapshot checksum mismatch! Expected {checksum.hex()}, got {computed_checksum.hex()}"
                            )

                        final_filename = f"snapshot-{idx}-{term}.snap"
                        return idx, term, final_filename

                last_idx, last_term, final_filename = await loop.run_in_executor(None, _validate_and_parse)
                final_path = os.path.join(self.snapshot_dir, final_filename)

                # Atomic swap into active storage
                os.replace(temp_snap_path, final_path)
                logger.info(f"Successfully received and stored snapshot at index {last_idx}, term {last_term}: {final_path}")

                # Restore state machine using the newly received snapshot
                await self.restore_latest_snapshot()

                # Truncate underlying WAL up to the received snapshot index
                await self.wal_manager.truncate_prefix(last_idx, last_term)

                return last_idx, last_term

            except Exception as e:
                logger.error(f"Failed processing received snapshot transfer: {e}")
                if os.path.exists(temp_snap_path):
                    try:
                        os.unlink(temp_snap_path)
                    except OSError:
                        pass
                raise SnapshotError(f"Snapshot reception and installation failed: {e}") from e