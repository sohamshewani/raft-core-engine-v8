import asyncio
import struct
import logging
from typing import Dict, Tuple, Optional, Callable, Awaitable

logger = logging.getLogger("raft.network")

class NetworkError(Exception):
    """Base exception for network layer errors."""
    pass

class ConnectionClosedError(NetworkError):
    """Raised when a peer connection is closed unexpectedly."""
    pass

class SerializationError(NetworkError):
    """Raised when message framing, packing, or unpacking fails."""
    pass

class RaftNetwork:
    """
    Non-blocking asynchronous TCP/IP transport layer for the Raft consensus engine.
    Utilizes length-prefixed framing (uint32 big-endian length headers) for 
    reliable RPC payload transmission over raw TCP sockets.
    """

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.server: Optional[asyncio.Server] = None
        
        # Mapping of peer_id -> (asyncio.StreamWriter, asyncio.StreamReader)
        self.connections: Dict[str, Tuple[asyncio.StreamWriter, asyncio.StreamReader]] = {}
        
        # Mapping of peer_id -> (host, port)
        self.peer_addresses: Dict[str, Tuple[str, int]] = {}
        
        # Registered RPC handlers: message_type (str) -> async callback
        self.rpc_handlers: Dict[str, Callable[[str, bytes], Awaitable[bytes]]] = {}
        
        # Concurrency protection for connection mutations
        self._conn_lock = asyncio.Lock()
        
        # Active server tasks tracking
        self._active_tasks: set[asyncio.Task] = set()

    def register_handler(self, msg_type: str, handler: Callable[[str, bytes], Awaitable[bytes]]) -> None:
        """Register an asynchronous callback handler for a specific RPC message type."""
        self.rpc_handlers[msg_type] = handler

    def register_peer(self, peer_id: str, host: str, port: int) -> None:
        """Register network routing information for a cluster peer."""
        self.peer_addresses[peer_id] = (host, port)

    async def start(self) -> None:
        """Initialize and start the inbound TCP server socket."""
        try:
            self.server = await asyncio.start_server(
                self._handle_incoming_connection,
                self.host,
                self.port,
                reuse_address=True
            )
            addr = self.server.sockets[0].getsockname()
            logger.info(f"RaftNetwork transport bound and listening on {addr}")
        except Exception as e:
            logger.error(f"Failed to bind network transport on {self.host}:{self.port}: {e}")
            raise NetworkError(f"Server bind failure: {e}") from e

    async def stop(self) -> None:
        """Gracefully shutdown the network transport, closing all active client/server streams."""
        logger.info("Initiating shutdown of RaftNetwork transport layer...")
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        async with self._conn_lock:
            for peer_id, (writer, reader) in list(self.connections.items()):
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    logger.warning(f"Error closing writer for peer {peer_id}: {e}")
            self.connections.clear()

        # Cancel any ongoing background tasks spawned by incoming connections
        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()
        
        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            self._active_tasks.clear()

        logger.info("RaftNetwork transport layer shutdown completed successfully.")

    async def _handle_incoming_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Accepts and processes incoming TCP connections from remote peers in the cluster."""
        peer_addr = writer.get_extra_info('peername')
        peer_id = f"{peer_addr[0]}:{peer_addr[1]}" if peer_addr else "unknown_peer"
        logger.debug(f"Accepted inbound TCP connection from {peer_id}")

        task = asyncio.current_task()
        if task:
            self._active_tasks.add(task)

        try:
            while True:
                # Read 4-byte big-endian length header
                length_prefix = await reader.readexactly(4)
                if not length_prefix:
                    break
                
                msg_len, = struct.unpack(">I", length_prefix)
                if msg_len == 0:
                    raise SerializationError("Received zero-length frame from peer.")
                
                payload = await reader.readexactly(msg_len)
                
                # Extract message type prefix (assume first 1 byte or fixed scheme, 
                # here we parse standard envelope: [type_len:uint8][type_str][data])
                if len(payload) < 1:
                    raise SerializationError("Payload missing type header.")
                
                type_len = payload[0]
                if len(payload) < 1 + type_len:
                    raise SerializationError("Malformed payload: type string truncated.")
                
                msg_type = payload[1:1 + type_len].decode('utf-8')
                message_body = payload[1 + type_len:]

                handler = self.rpc_handlers.get(msg_type)
                if not handler:
                    logger.error(f"No registered RPC handler for message type: {msg_type}")
                    response_payload = self._serialize_error(f"Unknown message type: {msg_type}")
                else:
                    try:
                        response_payload = await handler(peer_id, message_body)
                    except Exception as handler_err:
                        logger.error(f"Error executing handler for {msg_type}: {handler_err}")
                        response_payload = self._serialize_error(str(handler_err))

                # Send length-prefixed response
                resp_len = len(response_payload)
                writer.write(struct.pack(">I", resp_len) + response_payload)
                await writer.drain()

        except asyncio.IncompleteReadError:
            logger.debug(f"Inbound connection from {peer_id} closed cleanly by peer.")
        except ConnectionResetError:
            logger.warning(f"Inbound connection from {peer_id} forcefully reset.")
        except Exception as e:
            logger.error(f"Error processing stream from {peer_id}: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            if task and task in self._active_tasks:
                self._active_tasks.remove(task)
            logger.debug(f"Cleaned up inbound connection stream for {peer_id}")

    async def _get_or_create_connection(self, peer_id: str) -> Tuple[asyncio.StreamWriter, asyncio.StreamReader]:
        """Retrieve an existing connected socket for a peer or establish a new non-blocking TCP connection."""
        async with self._conn_lock:
            if peer_id in self.connections:
                writer, reader = self.connections[peer_id]
                if not writer.is_closing():
                    return writer, reader
                else:
                    # Clean up stale dead connection
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass
                    del self.connections[peer_id]

            if peer_id not in self.peer_addresses:
                raise NetworkError(f"Peer address configuration missing for target ID: {peer_id}")

            host, port = self.peer_addresses[peer_id]
            try:
                # Establish connection with a strict timeout guard
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=2.0
                )
                self.connections[peer_id] = (writer, reader)
                logger.debug(f"Established new outbound TCP connection to peer {peer_id} at {host}:{port}")
                return writer, reader
            except asyncio.TimeoutError as te:
                logger.error(f"Timeout connecting to peer {peer_id} at {host}:{port}")
                raise NetworkError(f"Connection timeout to peer {peer_id}") from te
            except Exception as e:
                logger.error(f"Failed to connect to peer {peer_id} at {host}:{port}: {e}")
                raise NetworkError(f"Connection failure to peer {peer_id}: {e}") from e

    async def send_rpc(self, peer_id: str, msg_type: str, payload: bytes, timeout: float = 3.0) -> bytes:
        """
        Sends a length-prefixed RPC message to a designated peer and awaits 
        the length-prefixed response message.
        """
        type_bytes = msg_type.encode('utf-8')
        if len(type_bytes) > 255:
            raise SerializationError("RPC message type string exceeds maximum length of 255 bytes.")
        
        # Construct envelope: [type_len:uint8][type_bytes][payload]
        envelope = struct.pack(">B", len(type_bytes)) + type_bytes + payload
        frame = struct.pack(">I", len(envelope)) + envelope

        attempts = 2
        for attempt in range(1, attempts + 1):
            try:
                writer, reader = await self._get_or_create_connection(peer_id)
                
                writer.write(frame)
                await writer.drain()

                # Read response with explicit timeout
                response_task = reader.readexactly(4)
                length_prefix = await asyncio.wait_for(response_task, timeout=timeout)
                
                resp_len, = struct.unpack(">I", length_prefix)
                if resp_len == 0:
                    raise SerializationError("Received zero-length response frame from peer.")

                response_body = await asyncio.wait_for(reader.readexactly(resp_len), timeout=timeout)
                return response_body

            except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError) as net_err:
                logger.warning(f"Network transport error communicating with peer {peer_id} (Attempt {attempt}/{attempts}): {net_err}")
                
                # Invalidate broken connection cache
                async with self._conn_lock:
                    if peer_id in self.connections:
                        w, r = self.connections.pop(peer_id)
                        try:
                            w.close()
                            await w.wait_closed()
                        except Exception:
                            pass
                
                if attempt == attempts:
                    raise NetworkError(f"Failed RPC communication with peer {peer_id} after {attempts} attempts.") from net_err
                await asyncio.sleep(0.05 * attempt)
            except Exception as e:
                logger.error(f"Unexpected transmission error to {peer_id}: {e}")
                raise NetworkError(f"Unexpected network fault: {e}") from e

        raise NetworkError(f"Exhausted all transmission retries for peer {peer_id}")

    @staticmethod
    def _serialize_error(error_msg: str) -> bytes:
        """Internal helper to serialize error messages back over transport streams."""
        err_bytes = error_msg.encode('utf-8')
        return struct.pack(">B", 5) + b"ERROR" + err_bytes