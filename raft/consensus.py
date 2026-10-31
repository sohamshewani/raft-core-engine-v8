import asyncio
import struct
import mmap
import os
import zlib
import tarfile
import tempfile
import logging
import random
import msgpack
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, List, Optional, Tuple, Set, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("raft-core-engine")

class OpType(IntEnum):
    NOOP = 0
    CLIENT_WRITE = 1
    CONFIG_CHANGE = 2

class NodeStateEnum(IntEnum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

@dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    op_type: int
    payload: bytes

    _STRUCT_FORMAT = "!QQBI"
    _HEADER_SIZE = struct.calcsize(_STRUCT_FORMAT)

    def pack(self) -> bytes:
        payload_len = len(self.payload)
        header = struct.pack(self._STRUCT_FORMAT, self.term, self.index, self.op_type, payload_len)
        frame = header + self.payload
        checksum = zlib.crc32(frame) & 0xFFFFFFFF
        return frame + struct.pack("!I", checksum)

    @classmethod
    def unpack(cls, buffer: bytes, offset: int = 0) -> Tuple['LogEntry', int]:
        hdr_size = cls._HEADER_SIZE
        if len(buffer) < offset + hdr_size + 4:
            raise ValueError("Buffer too small for LogEntry header and checksum")
        
        term, index, op_type, payload_len = struct.unpack_from(cls._STRUCT_FORMAT, buffer, offset)
        curr_offset = offset + hdr_size
        
        payload = buffer[curr_offset:curr_offset + payload_len]
        if len(payload) != payload_len:
            raise ValueError("Payload length mismatch during unpack")
        
        curr_offset += payload_len
        stored_crc = struct.unpack_from("!I", buffer, curr_offset)[0]
        curr_offset += 4

        frame_to_check = buffer[offset:curr_offset - 4]
        calculated_crc = zlib.crc32(frame_to_check) & 0xFFFFFFFF
        if calculated_crc != stored_crc:
            raise ValueError(f"CRC32 mismatch: stored {stored_crc:#x}, calculated {calculated_crc:#x}")

        entry = cls(term=term, index=index, op_type=op_type, payload=payload)
        return entry, curr_offset

class WriteAheadLog:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.file_size = 0
        self._fd = None
        self._mmap_obj = None
        self._lock = asyncio.Lock()
        self._open_file()

    def _open_file(self):
        exists = os.path.exists(self.filepath)
        if not exists:
            with open(self.filepath, "wb") as f:
                pass
        self._fd = os.open(self.filepath, os.O_RDWR | os.O_CREAT)
        self.file_size = os.path.getsize(self.filepath)
        if self.file_size > 0:
            self._mmap_obj = mmap.mmap(self._fd, self.file_size, access=mmap.ACCESS_WRITE)
        else:
            self._mmap_obj = None

    async def append(self, entry: LogEntry) -> int:
        async with self._lock:
            packed_data = entry.pack()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._sync_append_sync, packed_data)
            return entry.index

    def _sync_append_sync(self, packed_data: bytes):
        os.lseek(self._fd, 0, os.SEEK_END)
        os.write(self._fd, packed_data)
        os.fsync(self._fd)
        self.file_size = os.path.getsize(self.filepath)
        if self._mmap_obj:
            self._mmap_obj.close()
        self._mmap_obj = mmap.mmap(self._fd, self.file_size, access=mmap.ACCESS_WRITE)

    def read_entries(self, start_index: int) -> List[LogEntry]:
        entries = []
        if self.file_size == 0 or not self._mmap_obj:
            return entries
        
        offset = 0
        while offset < self.file_size:
            try:
                entry, next_offset = LogEntry.unpack(self._mmap_obj, offset)
                if entry.index >= start_index:
                    entries.append(entry)
                offset = next_offset
            except Exception as e:
                logger.error(f"WAL corruption or incomplete frame at offset {offset}: {e}")
                break
        return entries

    def truncate_from(self, index: int):
        valid_entries = []
        if self.file_size > 0 and self._mmap_obj:
            offset = 0
            while offset < self.file_size:
                try:
                    entry, next_offset = LogEntry.unpack(self._mmap_obj, offset)
                    if entry.index < index:
                        valid_entries.append(entry.pack())
                    offset = next_offset
                except Exception:
                    break

        if self._mmap_obj:
            self._mmap_obj.close()
            self._mmap_obj = None
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        for packed in valid_entries:
            os.write(self._fd, packed)
        os.fsync(self._fd)
        self.file_size = os.path.getsize(self.filepath)
        if self.file_size > 0:
            self._mmap_obj = mmap.mmap(self._fd, self.file_size, access=mmap.ACCESS_WRITE)

    def close(self):
        if self._mmap_obj:
            self._mmap_obj.close()
        if self._fd is not None:
            os.close(self._fd)

class StateMachine:
    def __init__(self):
        self._store: Dict[str, Any] = {}
        self._rcu_lock = asyncio.Lock()

    async def apply(self, entry: LogEntry) -> Any:
        async with self._rcu_lock:
            if entry.op_type == OpType.CLIENT_WRITE:
                data = msgpack.unpackb(entry.payload, raw=False)
                key = data.get("key")
                val = data.get("val")
                self._store[key] = val
                return val
            elif entry.op_type == OpType.CONFIG_CHANGE:
                return msgpack.unpackb(entry.payload, raw=False)
            return None

    async def get(self, key: str) -> Any:
        async with self._rcu_lock:
            return self._store.get(key)

    async def create_snapshot(self, filepath: str):
        async with self._rcu_lock:
            snapshot_data = msgpack.packb(self._store)
            fd, tmp_path = tempfile.mkstemp()
            try:
                os.write(fd, snapshot_data)
                os.close(fd)
                with tarfile.open(filepath, "w:gz") as tar:
                    tar.add(tmp_path, arcname="state.msgpack")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    async def restore_snapshot(self, filepath: str):
        async with self._rcu_lock:
            with tarfile.open(filepath, "r:gz") as tar:
                member = tar.getmember("state.msgpack")
                f = tar.extractfile(member)
                if f:
                    self._store = msgpack.unpackb(f.read(), raw=False)

class RaftNode:
    def __init__(self, node_id: str, peers: Dict[str, Tuple[str, int]], wal_path: str, snapshot_path: str):
        self.node_id = node_id
        self.peers = peers
        self.wal = WriteAheadLog(wal_path)
        self.state_machine = StateMachine()
        self.snapshot_path = snapshot_path

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        self.commit_index = 0
        self.last_applied = 0

        self.state = NodeStateEnum.FOLLOWER
        self.leader_id: Optional[str] = None

        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self._lock = asyncio.Lock()
        self._election_timeout = random.uniform(0.150, 0.300)
        self._heartbeat_interval = 0.050
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._server: Optional[asyncio.Server] = None
        self._running = False

        self._load_persisted_state()

    def _load_persisted_state(self):
        entries = self.wal.read_entries(0)
        self.log = entries
        if self.log:
            last = self.log[-1]
            self.commit_index = last.index
            self.last_applied = last.index

    async def start(self, host: str, port: int):
        self._running = True
        self._server = await asyncio.start_server(self._handle_client_connection, host, port)
        self._reset_election_timer()
        logger.info(f"Node {self.node_id} started at {host}:{port} in state {self.state.name}")

    async def stop(self):
        self._running = False
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        self.wal.close()
        logger.info(f"Node {self.node_id} stopped.")

    def _reset_election_timer(self):
        if self._election_task:
            self._election_task.cancel()
        self._election_timeout = random.uniform(0.150, 0.300)
        self._election_task = asyncio.create_task(self._election_timer_expired())

    async def _election_timer_expired(self):
        try:
            await asyncio.sleep(self._election_timeout)
            async with self._lock:
                if self.state != NodeStateEnum.LEADER:
                    await self._start_election()
        except asyncio.CancelledError:
            pass

    async def _start_election(self):
        self.state = NodeStateEnum.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self._reset_election_timer()
        logger.info(f"Node {self.node_id} starting election for term {self.current_term}")

        last_log_term = self.log[-1].term if self.log else 0
        last_log_index = self.log[-1].index if self.log else 0

        votes_granted = 1
        quorum = (len(self.peers) + 1) // 2 + 1

        for peer_id, (host, port) in self.peers.items():
            args = {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            asyncio.create_task(self._request_vote_rpc(peer_id, host, port, args, quorum))

    async def _request_vote_rpc(self, peer_id: str, host: str, port: int, args: dict, quorum: int):
        try:
            response = await self._send_rpc(host, port, "RequestVote", args)
            if not response:
                return

            async with self._lock:
                if self.state != NodeStateEnum.CANDIDATE:
                    return
                
                if response["term"] > self.current_term:
                    self.current_term = response["term"]
                    self.state = NodeStateEnum.FOLLOWER
                    self.voted_for = None
                    self._reset_election_timer()
                    return

                if response["term"] == self.current_term and response.get("vote_granted", False):
                    nonlocal votes_granted
                    votes_granted += 1
                    if votes_granted >= quorum and self.state == NodeStateEnum.CANDIDATE:
                        await self._transition_to_leader()
        except Exception as e:
            logger.debug(f"Failed to request vote from {peer_id}: {e}")

    async def _transition_to_leader(self):
        self.state = NodeStateEnum.LEADER
        self.leader_id = self.node_id
        if self._election_task:
            self._election_task.cancel()
        
        last_index = self.log[-1].index if self.log else 0
        for peer_id in self.peers:
            self.next_index[peer_id] = last_index + 1
            self.match_index[peer_id] = 0

        logger.info(f"Node {self.node_id} became LEADER for term {self.current_term}")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Append NOOP entry for linearizability
        noop_entry = LogEntry(term=self.current_term, index=last_index + 1, op_type=OpType.NOOP, payload=b"")
        self.log.append(noop_entry)
        await self.wal.append(noop_entry)
        await self._broadcast_append_entries()

    async def _heartbeat_loop(self):
        try:
            while self._running:
                async with self._lock:
                    if self.state != NodeStateEnum.LEADER:
                        break
                    await self._broadcast_append_entries()
                await asyncio.sleep(self._heartbeat_interval)
        except asyncio.CancelledError:
            pass

    async def _broadcast_append_entries(self):
        last_log_index = self.log[-1].index if self.log else 0
        for peer_id, (host, port) in self.peers.items():
            next_idx = self.next_index.get(peer_id, last_log_index + 1)
            prev_index = next_idx - 1
            prev_term = 0
            if prev_index > 0:
                for entry in self.log:
                    if entry.index == prev_index:
                        prev_term = entry.term
                        break

            entries_to_send = [e for e in self.log if e.index >= next_idx]
            
            args = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": [(e.term, e.index, e.op_type, e.payload) for e in entries_to_send],
                "leader_commit": self.commit_index
            }
            asyncio.create_task(self._append_entries_rpc(peer_id, host, port, args))

    async def _append_entries_rpc(self, peer_id: str, host: str, port: int, args: dict):
        try:
            response = await self._send_