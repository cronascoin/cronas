# peer.py

import asyncio
import json
import logging
import os
import aiofiles
import socket
import ntplib
import time
import random
import errno
import base64
from collections import deque, defaultdict
from message import MessageHandler  # Ensure message.py is in the same directory or adjust the import path
import stun  # Ensure stun is installed or remove if not needed

logger = logging.getLogger(__name__)

class Peer:
    def __init__(
        self,
        host,
        port,
        crypto,
        version,
        server_id,
        max_peers=10,
        addnode=None,
        debug=False
    ):
        """
        Initialize the Peer instance.

        Args:
            host (str): The host IP address to bind the server.
            port (int): The listening port.
            crypto (Crypto): A Crypto instance for cryptographic operations.
            version (str): The protocol version for this peer.
            server_id (str): Unique server identifier provided by app.py.
            max_peers (int, optional): Max number of concurrent peer connections. Defaults to 10.
            addnode (list, optional): List of seed node addresses in "ip:port" format. Defaults to None.
            debug (bool, optional): Enable debug logging. Defaults to False.
        """
        self.host = host
        self.port = port
        self.crypto = crypto
        self.version = version
        self.server_id = server_id
        self.debug = debug
        self.max_peers = max_peers

        # Initialize addnode list
        self.addnode = set(addnode) if addnode else set()

        # Peer connections
        self.active_peers = {}  # {server_id: peer_info_dict}
        self.known_peers = set()
        self.peers_file = "peers_file.json"  # JSON file for peer addresses
        self.revoked_peers = set()

        # Retry parameters
        self.max_retries = 5
        self.initial_backoff = 2
        self.backoff_factor = 2

        # Data redundancy
        self.chunk_sources = {}  # {(start, end): [peer_ids]}
        self.chunk_cache = defaultdict(dict)  # {(start, end): {peer_id: chunk_data}}

        # Synchronization
        self.chunk_size = 1048576  # 1 MB
        self.background_tasks = []
        self.shutdown_flag = False

        # Heartbeat Records
        self.heartbeat_records = defaultdict(deque)  # {server_id: deque_of_heartbeats}

        # Message processing
        self.received_messages = []
        self.message_lock = asyncio.Lock()
        self.seen_message_ids = deque(maxlen=10000)  # store seen message IDs
        self.seen_message_lock = asyncio.Lock()

        # External IP and NTP offset
        self.external_ip, self.external_p2p_port = self.get_external_ip()
        self.ntp_offset = self.get_ntp_offset()

        # Load revoked peers
        self.load_revoked_peers()

        # Initialize MessageHandler
        self.message = MessageHandler(self)

        # Uptime tracking
        self.uptime = 0
        self.uptime_task = asyncio.create_task(self.track_uptime())

        # Asynchronously load peers
        asyncio.create_task(self.load_peers())

    # ----------------------------------------------------------------
    # External IP and NTP Utilities
    # ----------------------------------------------------------------

    def get_stun_info(self):
        """
        Retrieves external IP and port information using STUN.

        Returns:
            tuple: (external_ip, external_port) or (None, None) on failure.
        """
        try:
            nat_type, external_ip, external_port = stun.get_ip_info(
                stun_host='stun.l.google.com', stun_port=19302
            )
            logger.info(f"NAT Type: {nat_type}, External IP: {external_ip}, External Port: {external_port}")
            return external_ip, external_port
        except Exception as e:
            logger.error(f"Failed to get IP info via STUN: {e}")
            return None, None

    def get_out_ip(self):
        """
        Detect the external IP address using a simple socket call.

        Returns:
            str or None: External IP address or None if detection fails.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                ip = s.getsockname()[0]
            logger.info(f"External IP via socket: {ip}")
            return ip
        except Exception as e:
            logger.error(f"Failed to detect external IP address via socket: {e}")
            return None

    def get_external_ip(self):
        """
        Retrieves external IP and port using STUN or fallback method.

        Returns:
            tuple: (external_ip, external_port) or (external_ip, None) if STUN fails.
        """
        external_ip, external_port = self.get_stun_info()
        if external_ip:
            return external_ip, external_port
        external_ip = self.get_out_ip()
        return external_ip, None

    def get_ntp_offset(self):
        """
        Calculates the NTP time offset.

        Returns:
            float: Offset in seconds or 0 on failure.
        """
        try:
            client = ntplib.NTPClient()
            response = client.request('pool.ntp.org', version=3)
            ntp_time = response.tx_time
            local_time = time.time()
            offset = ntp_time - local_time
            logger.info(f"NTP offset calculated: {offset} seconds.")
            return offset
        except Exception as e:
            logger.error(f"Failed to get NTP offset: {e}")
            return 0

    # ----------------------------------------------------------------
    # Revoked Peers Management
    # ----------------------------------------------------------------

    def load_revoked_peers(self):
        """Load the revoked peers list from disk."""
        if os.path.exists("revoked_peers.json"):
            try:
                with open("revoked_peers.json", "r") as f:
                    revoked = json.load(f)
                    self.revoked_peers = set(revoked)
                logger.info("Revoked peers list loaded from disk.")
            except Exception as e:
                logger.error(f"Failed to load revoked peers list: {e}")
                self.revoked_peers = set()
        else:
            self.revoked_peers = set()
            logger.info("No existing revoked peers list found; starting empty.")

    def save_revoked_peers(self):
        """Save the revoked peers list to disk."""
        try:
            with open("revoked_peers.json", "w") as f:
                json.dump(list(self.revoked_peers), f, indent=4)
            logger.info("Revoked peers list saved to disk.")
        except Exception as e:
            logger.error(f"Failed to save revoked peers list: {e}")

    # ----------------------------------------------------------------
    # Peers Data Management
    # ----------------------------------------------------------------

    async def load_peers(self):
        """Load known peers from storage and add seed nodes."""
        if os.path.exists(self.peers_file):
            try:
                async with aiofiles.open(self.peers_file, 'r') as f:
                    data = await f.read()
                    self.known_peers = set(json.loads(data))
                logger.info(f"Loaded {len(self.known_peers)} known peers from {self.peers_file}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON in {self.peers_file}: {e}")
            except Exception as e:
                logger.error(f"Failed to load peers from {self.peers_file}: {e}")
        else:
            logger.info("No existing peers file found; starting with an empty list.")

        # Add seeds from addnode
        self.known_peers.update(self.addnode)
        logger.info(f"Added {len(self.addnode)} seed peers from addnode list.")

    async def write_peers_to_file(self):
        """Asynchronously write the known peers to JSON file."""
        try:
            async with aiofiles.open(self.peers_file, 'w') as f:
                await f.write(json.dumps(list(self.known_peers), indent=4))
            logger.info(f"Peers list written to {self.peers_file}")
        except Exception as e:
            logger.error(f"Failed to write peers to {self.peers_file}: {e}")

    async def schedule_rewrite(self):
        """Schedule a rewrite of the peers file."""
        asyncio.create_task(self.write_peers_to_file())

    def is_valid_peer(self, peer_info):
        """
        Validate the peer info string in 'ip:port' format.

        Args:
            peer_info (str): Peer address (e.g. '127.0.0.1:8000').

        Returns:
            bool: True if valid format & port range, else False.
        """
        parts = peer_info.split(':')
        if len(parts) != 2:
            return False
        ip, port_str = parts
        try:
            port = int(port_str)
            if port < 1 or port > 65535:
                return False
            socket.inet_aton(ip)  # Check IPv4 format
        except (ValueError, socket.error):
            return False
        return True

    # ----------------------------------------------------------------
    # Peer Connection Management
    # ----------------------------------------------------------------

    async def connect_to_peer(self, host, port, max_retries=None, initial_backoff=None):
        """
        Connect to a peer at the given host:port with retry logic.

        Args:
            host (str): The target IP address.
            port (int): The target port number.
            max_retries (int, optional): Max connection retries.
            initial_backoff (int, optional): Initial sleep before retry.
        """
        peer_info = f"{host}:{port}"
        if not self.is_valid_peer(peer_info):
            logger.warning(f"Invalid peer address: {peer_info}")
            return

        if len(self.active_peers) >= self.max_peers:
            logger.info(
                f"Max peers ({self.max_peers}) reached; skipping connection to {peer_info}."
            )
            return

        retries = 0
        backoff = initial_backoff or self.initial_backoff
        retries_limit = max_retries or self.max_retries

        while retries < retries_limit and not self.shutdown_flag:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                logger.info(f"Connected to {peer_info} on attempt {retries + 1}")

                # Send our hello message
                await self.message.send_hello_message(writer)

                # Begin listening for messages from this peer
                asyncio.create_task(self.listen_to_peer(reader, writer))
                break  # Successful connection
            except (ConnectionRefusedError, asyncio.TimeoutError) as e:
                retries += 1
                logger.error(f"Connection to {peer_info} failed (attempt {retries}): {e}")
                if retries < retries_limit:
                    logger.info(f"Retrying in {backoff} seconds...")
                    await asyncio.sleep(backoff)
                    backoff *= self.backoff_factor
                else:
                    logger.error(f"Max retries ({retries_limit}) reached for {peer_info}. Aborting.")

    async def listen_to_peer(self, reader, writer):
        """
        Continuously listen for messages from a connected peer.

        Args:
            reader (asyncio.StreamReader): The reader for incoming data.
            writer (asyncio.StreamWriter): The writer for outgoing data.
        """
        peer_info = writer.get_extra_info('peername')
        try:
            while not reader.at_eof():
                message = await self.message.receive_message(reader)
                await self.message.process_message(message, reader, writer)
        except Exception as e:
            logger.error(f"Error listening to {peer_info}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Connection with {peer_info} closed.")
            # Optionally remove from active_peers if known server_id
            # e.g. if message had a "server_id" or stored in active_peers

    async def handle_peer_connection(self, reader, writer):
        """
        Handle an incoming connection from a peer.

        Args:
            reader (asyncio.StreamReader): The reader for incoming data.
            writer (asyncio.StreamWriter): The writer for outgoing data.
        """
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        if self.debug:
            logger.info(f"Incoming connection from {peer_info}")

        try:
            # The first message from a new peer should be a hello
            message = await self.message.receive_message(reader)

            if message.get("type") == "hello":
                peer_server_id = message.get("server_id")
                if peer_server_id in self.revoked_peers:
                    logger.info(f"Revoked peer {peer_server_id} tried to connect; closing.")
                    writer.close()
                    await writer.wait_closed()
                    return

                if peer_server_id in self.active_peers:
                    logger.info(
                        f"Already connected to server_id {peer_server_id}. Closing new connection."
                    )
                    writer.close()
                    await writer.wait_closed()
                    return

                if public_key_pem := message.get("public_key"):
                    self.crypto.add_peer_public_key(peer_server_id, public_key_pem)
                else:
                    logger.warning(f"No public key from peer {peer_server_id}.")

                # Add to active peers
                self.active_peers[peer_server_id] = {
                    'reader': reader,
                    'writer': writer,
                    'connected_at': time.time(),
                    'last_seen': time.time(),
                }
                logger.info(f"Connected to peer {peer_server_id}")

                # Process the hello message
                await self.message.process_message(message, reader, writer)

                # Start listening
                asyncio.create_task(self.listen_to_peer(reader, writer))
            else:
                logger.warning("First message was not 'hello'; closing connection.")
                writer.close()
                await writer.wait_closed()

        except ConnectionResetError as e:
            logger.error(f"Connection reset by {peer_info}: {e}")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logger.error(f"Error handling connection from {peer_info}: {e}")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def start_p2p_server(self):
        """
        Start the P2P server to accept incoming peer connections.
        """
        try:
            # reuse_port=True can help with quick restarts, but is OS-dependent.
            server = await asyncio.start_server(
                self.handle_peer_connection,
                self.host,
                self.port,
                reuse_address=True,
                reuse_port=True
            )
            self.p2p_server = server
            logger.info(f"P2P server v{self.version} listening on {self.host}:{self.port}")

            async with server:
                await server.serve_forever()

        except OSError as e:
            logger.error(f"Failed to start server on {self.host}:{self.port}: {e}")
            if e.errno == errno.EADDRINUSE:
                logger.error(
                    f"Port {self.port} is in use. Ensure it's free before retrying."
                )
            await self.close_p2p_server()
        except Exception as e:
            logger.error(f"Error starting P2P server: {e}")
            await self.close_p2p_server()

    async def close_p2p_server(self):
        """Close the P2P server gracefully."""
        if hasattr(self, 'p2p_server') and self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logger.info("P2P server closed.")
        else:
            logger.error("P2P server was not initialized or already closed.")

    async def start(self):
        """
        Entry point to start the Peer:
         - Load peers
         - Attempt connections to known peers
         - Start the P2P server
        """
        try:
            await self.load_peers()

            # Start connecting to known peers
            asyncio.create_task(self.connect_and_maintain())

            # Start the server to accept incoming connections
            await self.start_p2p_server()

        finally:
            await self.shutdown()

    async def connect_and_maintain(self):
        """
        Repeatedly try to connect to known peers (on a schedule).
        """
        while not self.shutdown_flag:
            logger.debug("Attempting connections to known peers...")
            await self.connect_to_known_peers()
            await asyncio.sleep(60)

    async def connect_to_known_peers(self):
        """
        Attempt to connect to known peers that are neither active nor revoked.
        """
        available_peers = [
            p for p in self.known_peers
            if p not in self.active_peers and p not in self.revoked_peers
        ]
        if not available_peers:
            logger.debug("No available peers to connect.")
            return

        current_peers = len(self.active_peers)
        needed = self.max_peers - current_peers
        if needed <= 0:
            logger.debug(f"Already at max_peers: {self.max_peers}.")
            return

        peers_to_connect = random.sample(available_peers, min(len(available_peers), needed))
        for peer_info in peers_to_connect:
            if self.is_valid_peer(peer_info):
                host, port = peer_info.split(':')
                port = int(port)
                asyncio.create_task(self.connect_to_peer(host, port))

        self.update_active_peers()

    def update_active_peers(self):
        """
        Placeholder: Add logic to prune dead peers, update stats, etc.
        """
        pass

    async def shutdown(self):
        """
        Gracefully shut down the Peer:
         - Cancel background tasks
         - Close all peer connections
         - Close the server
         - Save peers & revoked peers
        """
        logger.info("Shutting down Peer...")
        self.shutdown_flag = True

        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()

        # Close active peer connections
        for peer_id, info in list(self.active_peers.items()):
            writer = info['writer']
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"Closed connection with peer {peer_id}")
        self.active_peers.clear()

        # Close the P2P server
        await self.close_p2p_server()

        # Save known peers
        await self.write_peers_to_file()

        # Save revoked peers
        self.save_revoked_peers()

        logger.info("Peer shutdown complete.")

    async def revoke_peer(self, server_id, reason="No reason provided"):
        """
        Revoke a peer by its server_id and disconnect if connected.

        Args:
            server_id (str): The peer's unique identifier.
            reason (str): Reason for revocation.
        """
        if server_id in self.revoked_peers:
            logger.info(f"Peer {server_id} is already revoked.")
            return

        self.revoked_peers.add(server_id)
        logger.info(f"Revoking peer {server_id} for: {reason}")
        self.save_revoked_peers()

        # Disconnect if connected
        if server_id in self.active_peers:
            if peer_info := self.active_peers.pop(server_id, None):
                writer = peer_info['writer']
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()
                logger.info(f"Disconnected from revoked peer {server_id}")

        # Broadcast revocation if needed
        await self.message.broadcast_revocation(server_id, reason)

    # ----------------------------------------------------------------
    # Uptime Tracking
    # ----------------------------------------------------------------

    async def track_uptime(self):
        """
        Track the uptime of the Peer in seconds.
        """
        while not self.shutdown_flag:
            await asyncio.sleep(60)
            self.uptime += 60
            logger.debug(f"Peer uptime: {self.uptime} seconds")

    # ----------------------------------------------------------------
    # Transaction Broadcasting
    # ----------------------------------------------------------------

    async def broadcast_transaction(self, txid):
        """
        Broadcast a transaction ID to all connected peers.

        Args:
            txid (str): Transaction ID string.
        """
        if not self.active_peers:
            logger.warning("No active peers to broadcast transaction.")
            return

        message = {
            "type": "transaction",
            "data": {
                "txid": txid,
                "server_id": self.server_id,
                "timestamp": time.time() + self.ntp_offset
            }
        }

        # Serialize & sign
        serialized = self.message.serialize_message(message, include_signature=False)
        signature = self.crypto.sign(serialized)
        message['signature'] = base64.b64encode(signature).decode('utf-8')

        # Send to all active peers
        for peer_id, info in self.active_peers.items():
            writer = info['writer']
            if not writer.is_closing():
                try:
                    await self.message.send_message(writer, message)
                    logger.debug(f"Broadcasted transaction {txid} to peer {peer_id}")
                except Exception as e:
                    logger.error(f"Failed to broadcast transaction to {peer_id}: {e}")
            else:
                logger.warning(f"Connection closing, cannot send tx to {peer_id}")

    # ----------------------------------------------------------------
    # Additional Peer Methods (Chat example)
    # ----------------------------------------------------------------

    async def handle_incoming_chat_message(self, message):
        """
        Handle an incoming chat message (broadcast or direct).

        message structure example:
        {
            "from": <server_id>,
            "to": <optional_server_id>,
            "message": <text>,
            "timestamp": <float>,
            "message_id": <uuid/unique_id>
        }
        """
        from_sid = message.get("from")
        to_sid = message.get("to")  # optional
        text = message.get("message")
        timestamp = message.get("timestamp")
        message_id = message.get("message_id")

        # Basic validation
        if not all([from_sid, text, timestamp, message_id]):
            logger.warning("Received incomplete chat message; ignoring.")
            return

        # Check for duplicates
        async with self.seen_message_lock:
            if message_id in self.seen_message_ids:
                logger.debug(f"Duplicate chat message {message_id} ignored.")
                return
            self.seen_message_ids.append(message_id)

        if to_sid:
            # Directed message
            if to_sid == self.server_id:
                # This peer is the intended recipient
                logger.info(f"Direct message from {from_sid} to {self.server_id}: {text}")
                print(f"[Direct] {from_sid} -> me: {text}")
            else:
                # Forward to intended recipient if connected
                logger.debug(f"Message directed to {to_sid}, not me.")
                await self.forward_directed_message(to_sid, message)
        else:
            # Broadcast message
            logger.info(f"Broadcast message from {from_sid}: {text}")
            print(f"[Broadcast] {from_sid}: {text}")
            # Optionally rebroadcast to propagate across the network
            await self.rebroadcast_message(message, exclude_peer=from_sid)

    async def forward_directed_message(self, to_sid, message):
        """
        Forward a directed chat message to its intended peer if connected.
        """
        if peer_info := self.active_peers.get(to_sid):
            writer = peer_info['writer']
            try:
                await self.message.send_message(writer, {
                    "type": "chat",
                    "data": message
                })
                logger.debug(f"Forwarded directed chat message to {to_sid}")
            except Exception as e:
                logger.error(f"Failed to forward directed chat to {to_sid}: {e}")
        else:
            logger.warning(f"Cannot forward message; peer {to_sid} not connected.")

    async def rebroadcast_message(self, message, exclude_peer=None):
        """
        Rebroadcast a chat message to all active peers except exclude_peer.

        Args:
            message (dict): The chat message.
            exclude_peer (str): server_id to exclude (e.g. original sender).
        """
        for peer_id, peer_info in self.active_peers.items():
            if peer_id == exclude_peer:
                continue
            writer = peer_info['writer']
            if not writer.is_closing():
                try:
                    await self.message.send_message(writer, {
                        "type": "chat",
                        "data": message
                    })
                    logger.debug(f"Rebroadcasted chat message to {peer_id}")
                except Exception as e:
                    logger.error(f"Failed to rebroadcast chat to {peer_id}: {e}")
            else:
                logger.warning(f"Cannot rebroadcast to {peer_id}; connection closing.")
