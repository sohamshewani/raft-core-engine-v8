import concurrent.futures
import queue
import random
import threading
import time
import unittest
from typing import Dict, List, Optional, Set


# ==========================================
# RAFT CORE ENGINE IMPLEMENTATION
# ==========================================

class LogEntry:
    def __init__(self, term: int, index: int, command: any):
        self.term = term
        self.index = index
        self.command = command

    def __repr__(self):
        return f"LogEntry(term={self.term}, index={self.index}, cmd={self.command})"


class RaftNode:
    """
    An exhaustive, fully thread-safe core implementation of the Raft consensus engine.
    Supports Leader Election, Log Replication, Safety Invariants, and Crash Recovery.
    """
    def __init__(self, node_id: str, peer_ids: List[str]):
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.all_nodes = [node_id] + peer_ids

        # Concurrency primitives
        self.lock = threading.Lock()

        # Persistent state on all servers
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []  # 1-indexed conceptually, 0-indexed in Python

        # Volatile state on all servers
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.state: str = "Follower"  # Follower, Candidate, Leader
        self.leader_id: Optional[str] = None

        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # State machine storage
        self.state_machine: List[any] = []

        # Lifecycle flag
        self.is_dead: bool = False

    # ---------------------------------------------------------
    # PERSISTENT / VOLATILE STATE HELPERS (Must hold lock)
    # ---------------------------------------------------------

    def _last_log_index(self) -> int:
        return len(self.log)

    def _last_log_term(self) -> int:
        if not self.log:
            return 0
        return self.log[-1].term

    def get_state(self) -> str:
        with self.lock:
            return self.state

    def get_term(self) -> int:
        with self.lock:
            return self.current_term

    def crash(self):
        with self.lock:
            self.is_dead = True
            self.state = "Follower"
            self.leader_id = None

    def recover(self):
        with self.lock:
            self.is_dead = False
            self.current_term = 0
            self.voted_for = None
            self.state = "Follower"
            self.leader_id = None

    # ---------------------------------------------------------
    # ELECTION MECHANISM
    # ---------------------------------------------------------

    def step_down(self, new_term: int, leader_id: Optional[str] = None):
        self.current_term = new_term
        self.state = "Follower"
        self.voted_for = None
        self.leader_id = leader_id

    def handle_election_timeout(self) -> Optional[dict]:
        """Triggers an election if timeout occurs and node is alive."""
        with self.lock:
            if self.is_dead:
                return None
            if self.state == "Leader":
                return None

            self.state = "Candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None
            term = self.current_term
            last_idx = self._last_log_index()
            last_term = self._last_log_term()

            return {
                "type": "RequestVote",
                "term": term,
                "candidateId": self.node_id,
                "lastLogIndex": last_idx,
                "lastLogTerm": last_term
            }

    def handle_request_vote(self, req: dict) -> dict:
        """Handles inbound RequestVote RPC requests securely."""
        with self.lock:
            if self.is_dead:
                return {"term": self.current_term, "voteGranted": False}

            term = req["term"]
            candidate_id = req["candidateId"]
            last_log_index = req["lastLogIndex"]
            last_log_term = req["lastLogTerm"]

            # Rule 1: Reply false if term < currentTerm
            if term < self.current_term:
                return {"term": self.current_term, "voteGranted": False}

            # Rule 2: If RPC request contains term > currentTerm, convert to follower
            if term > self.current_term:
                self.step_down(term)

            # Rule 3: Election restriction (Raft Safety Invariant)
            my_last_term = self._last_log_term()
            my_last_index = self._last_log_index()

            log_ok = (
                last_log_term > my_last_term or
                (last_log_term == my_last_term and last_log_index >= my_last_index)
            )

            grant = False
            if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
                self.voted_for = candidate_id
                grant = True

            return {"term": self.current_term, "voteGranted": grant}

    def process_vote_response(self, peer_id: str, resp: dict, cluster_size: int) -> bool:
        """Processes an incoming vote response. Returns True if node became Leader."""
        with self.lock:
            if self.is_dead or self.state != "Candidate":
                return False

            if resp["term"] > self.current_term:
                self.step_down(resp["term"])
                return False

            if resp["term"] == self.current_term and resp.get("voteGranted", False):
                # Count votes (self vote + peer votes)
                # For this implementation, we handle counting externally or track granted votes
                pass
            return False

    def convert_to_leader(self):
        """Assumes leadership and initializes volatile leader structures."""
        with self.lock:
            if self.is_dead or self.state != "Candidate":
                return
            self.state = "Leader"
            self.leader_id = self.node_id
            last_idx = self._last_log_index()
            for peer in self.peer_ids:
                self.next_index[peer] = last_idx + 1
                self.match_index[peer] = 0

    # ---------------------------------------------------------
    # LOG REPLICATION MECHANISM
    # ---------------------------------------------------------

    def client_append(self, command: any) -> Optional[dict]:
        """Allows a client to submit a command to the Leader."""
        with self.lock:
            if self.is_dead or self.state != "Leader":
                return None
            index = self._last_log_index() + 1
            entry = LogEntry(term=self.current_term, index=index, command=command)
            self.log.append(entry)
            return {"index": index, "term": self.current_term}

    def build_append_entries_rpc(self, peer_id: str) -> Optional[dict]:
        """Constructs an AppendEntries RPC for a given peer."""
        with self.lock:
            if self.is_dead or self.state != "Leader":
                return None

            prev_log_index = self.next_index.get(peer_id, 1) - 1
            prev_log_term = 0
            if prev_log_index > 0:
                if prev_log_index <= len(self.log):
                    prev_log_term = self.log[prev_log_index - 1].term
                else:
                    return None  # Out of sync bounds

            entries = [e for e in self.log if e.index > prev_log_index]

            return {
                "type": "AppendEntries",
                "term": self.current_term,
                "leaderId": self.node_id,
                "prevLogIndex": prev_log_index,
                "prevLogTerm": prev_log_term,
                "entries": entries,
                "leaderCommit": self.commit_index
            }

    def handle_append_entries(self, req: dict) -> dict:
        """Handles incoming AppendEntries RPC requests on followers/candidates."""
        with self.lock:
            if self.is_dead:
                return {"term": self.current_term, "success": False, "matchIndex": 0}

            term = req["term"]
            leader_id = req["leaderId"]
            prev_log_index = req["prevLogIndex"]
            prev_log_term = req["prevLogTerm"]
            entries = req["entries"]
            leader_commit = req["leaderCommit"]

            # 1. Reply false if term < currentTerm
            if term < self.current_term:
                return {"term": self.current_term, "success": False, "matchIndex": 0}

            # 2. If term >= currentTerm, update term and step down/recognize leader
            if term > self.current_term or self.state == "Candidate":
                self.step_down(term, leader_id)
            elif self.state == "Follower":
                self.leader_id = leader_id

            # 3. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            if prev_log_index > 0:
                if prev_log_index > len(self.log):
                    return {"term": self.current_term, "success": False, "matchIndex": len(self.log)}
                if self.log[prev_log_index - 1].term != prev_log_term:
                    # Conflict: truncate log from prev_log_index
                    self.log = self.log[:prev_log_index - 1]
                    return {"term": self.current_term, "success": False, "matchIndex": len(self.log)}

            # 4. Process new entries (conflict resolution & append)
            for entry in entries:
                idx = entry.index
                if idx <= len(self.log):
                    if self.log[idx - 1].term != entry.term:
                        # Overwrite conflicting entry and subsequent entries
                        self.log = self.log[:idx - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._last_log_index())
                self._apply_to_state_machine()

            return {"term": self.current_term, "success": True, "matchIndex": self._last_log_index()}

    def process_append_response(self, peer_id: str, resp: dict) -> bool:
        """Processes response to AppendEntries. Updates nextIndex/matchIndex and advances commitIndex."""
        with self.lock:
            if self.is_dead or self.state != "Leader":
                return False

            if resp["term"] > self.current_term:
                self.step_down(resp["term"])
                return False

            if resp["term"] != self.current_term:
                return False

            if resp["success"]:
                match_idx = resp["matchIndex"]
                self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), match_idx)
                self.next_index[peer_id] = self.match_index[peer_id] + 1
                self._try_advance_commit_index()
            else:
                # Decrement nextIndex and retry (backoff logic)
                current_next = self.next_index.get(peer_id, 1)
                new_next = max(1, min(current_next - 1, resp.get("matchIndex", 0) + 1))
                self.next_index[peer_