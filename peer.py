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
import stun  # Ensure stun is installed or handle if not needed

logger = logging.getLogger(__name__)

class Peer:
    def __init__(self, host, port, crypto, version, server_id, max_peers=10, addnode=None, debug=False):
        """
        Initialize the Peer instance.

        Args:
            host (str): The host IP address.
            port (int): The listening port.
            crypto (Crypto): The Crypto instance for cryptographic operations.
            version (str): The protocol version for this peer.
            server_id (str): Unique server identifier provided by app.py.
            max_peers (int, optional): Maximum number of concurrent peer connections. Defaults to 10.
            addnode (list, optional): List of seed node addresses in "ip:port" format. Defaults to None.
            debug (bool, optional): Enable debug logging. Defaults to False.
        """
        self.host = host
        self.port = port
        self.crypto = crypto
        self.version = version
        self.server_id = server_id  # Use the server_id provided by app.py
        self.debug = debug
        self.max_peers = max_peers

        # Initialize addnode list
        self.addnode = set(addnode) if addnode else set()

        # Peer connections
        self.active_peers = {}  # Mapping of server_id to peer_info dict
        self.known_peers = set()
        self.peers_file = "peers_file.json"  # JSON file for peers
        self.revoked_peers = set()

        # Retry parameters
        self.max_retries = 5
        self.initial_backoff = 2
        self.backoff_factor = 2

        # Data redundancy
        self.chunk_sources = {}  # Mapping of (start_byte, end_byte) to peer_ids
        self.chunk_cache = defaultdict(dict)  # (start_byte, end_byte): {peer_id: chunk}

        # Synchronization
        self.chunk_size = 1048576  # 1MB
        self.background_tasks = []
        self.shutdown_flag = False

        # Heartbeat Records
        self.heartbeat_records = defaultdict(deque)  # server_id: deque of heartbeat records

        # Message Processing
        self.received_messages = []
        self.message_lock = asyncio.Lock()
        self.seen_message_ids = deque(maxlen=10000)  # Using deque to efficiently manage seen message IDs
        self.seen_message_lock = asyncio.Lock()  # Async lock for thread-safe access

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

        # Initialize peers
        asyncio.create_task(self.load_peers())

    # ----------------------------
    # External IP and NTP Utilities
    # ----------------------------

    def get_stun_info(self):
        """
        Retrieves external IP and port information using STUN.

        Returns:
            tuple: (external_ip, external_port) or (None, None) on failure.
        """
        try:
            nat_type, external_ip, external_port = stun.get_ip_info(stun_host='stun.l.google.com', stun_port=19302)
            logger.info(f"NAT Type: {nat_type}, External IP: {external_ip}, External Port: {external_port}")
            return external_ip, external_port
        except Exception as e:
            logger.error(f"Failed to get IP info via STUN: {e}")
            return None, None

    def get_out_ip(self):
        """
        Detects the external IP address using socket connection.

        Returns:
            str or None: External IP address or None if detection fails.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                # Doesn't have to be reachable
                s.connect(('8.8.8.8', 80))
                ip = s.getsockname()[0]
            logger.info(f"External IP via socket: {ip}")
            return ip
        except Exception as e:
            logger.error(f"Failed to detect external IP address via socket: {e}")
            return None

    def get_external_ip(self):
        """
        Retrieves the external IP and port using STUN or fallback method.

        Returns:
            tuple: (external_ip, external_port) or (external_ip, None) on fallback.
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
            float: Time offset in seconds or 0 on failure.
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

    # ----------------------------
    # Revoked Peers Management
    # ----------------------------

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
            logger.info("No existing revoked peers list found. Starting with an empty list.")

    def save_revoked_peers(self):
        """Save the revoked peers list to disk."""
        try:
            with open("revoked_peers.json", "w") as f:
                json.dump(list(self.revoked_peers), f, indent=4)
            logger.info("Revoked peers list saved to disk.")
        except Exception as e:
            logger.error(f"Failed to save revoked peers list: {e}")

    # ----------------------------
    # Peers Data Management
    # ----------------------------

    async def load_peers(self):
        """
        Load known peers from the peers storage file and include addnode list.
        """
        if os.path.exists(self.peers_file):
            try:
                async with aiofiles.open(self.peers_file, 'r') as f:
                    peers = await f.read()
                    self.known_peers = set(json.loads(peers))
                logger.info(f"Loaded {len(self.known_peers)} known peers from {self.peers_file}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON in {self.peers_file}: {e}")
            except Exception as e:
                logger.error(f"Failed to load peers from {self.peers_file}: {e}")
        else:
            logger.info("No existing peers file found. Starting with an empty peers list.")

        # Add seed nodes from addnode list
        self.known_peers.update(self.addnode)
        logger.info(f"Added {len(self.addnode)} seed peers from addnode list.")

    async def write_peers_to_file(self):
        """
        Asynchronously write the known peers to the peers_file.json.
        """
        try:
            async with aiofiles.open(self.peers_file, 'w') as f:
                await f.write(json.dumps(list(self.known_peers), indent=4))
            logger.info(f"Peers list successfully written to {self.peers_file}")
        except Exception as e:
            logger.error(f"Failed to write peers to {self.peers_file}: {e}")

    async def schedule_rewrite(self):
        """
        Schedule the rewriting of the peers list to persistent storage.
        """
        asyncio.create_task(self.write_peers_to_file())

    def is_valid_peer(self, peer_info):
        """
        Validate the peer information format.

        Args:
            peer_info (str): Peer address in 'ip:port' format.

        Returns:
            bool: True if valid, False otherwise.
        """
        parts = peer_info.split(':')
        if len(parts) != 2:
            return False
        ip, port = parts
        try:
            port = int(port)
            if port < 1 or port > 65535:
                return False
            socket.inet_aton(ip)  # Validates IPv4 address format
        except (ValueError, socket.error):
            return False
        return True

    # ----------------------------
    # Peer Connection Management
    # ----------------------------

    async def connect_to_peer(self, host, port, max_retries=None, initial_backoff=None):
        """
        Connect to a peer at the given host and port with retry logic.

        Args:
            host (str): The peer's IP address.
            port (int): The peer's port number.
            max_retries (int, optional): Maximum number of retries. Defaults to self.max_retries.
            initial_backoff (int, optional): Initial backoff time in seconds. Defaults to self.initial_backoff.
        """
        peer_info = f"{host}:{port}"
        if not self.is_valid_peer(peer_info):
            logger.warning(f"Invalid peer address: {peer_info}")
            return

        if len(self.active_peers) >= self.max_peers:
            logger.info(f"Maximum number of peers ({self.max_peers}) reached. Skipping connection to {peer_info}.")
            return

        retries = 0
        backoff = initial_backoff or self.initial_backoff
        retries_limit = max_retries or self.max_retries

        while retries < retries_limit and not self.shutdown_flag:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                logger.info(f"Connected to peer {peer_info} on attempt {retries + 1}")

                # Send hello message
                await self.message.send_hello_message(writer)

                # Start handling messages from this peer
                asyncio.create_task(self.listen_to_peer(reader, writer))
                break  # Exit after successful connection
            except (ConnectionRefusedError, asyncio.TimeoutError) as e:
                retries += 1
                logger.error(f"Connection to peer {peer_info} failed on attempt {retries}: {e}")
                if retries < retries_limit:
                    logger.info(f"Retrying in {backoff} seconds...")
                    await asyncio.sleep(backoff)
                    backoff *= self.backoff_factor  # Exponential backoff
                else:
                    logger.error(f"Exceeded maximum retries ({retries_limit}) for peer {peer_info}. Giving up.")

    async def listen_to_peer(self, reader, writer):
        """
        Continuously listen for messages from a peer.

        Args:
            reader (StreamReader): The StreamReader object for the connection.
            writer (StreamWriter): The StreamWriter object for the connection.
        """
        peer_info = writer.get_extra_info('peername')
        try:
            while not reader.at_eof():
                message = await self.message.receive_message(reader)
                await self.message.process_message(message, reader, writer)
        except Exception as e:
            logger.error(f"Error listening to peer {peer_info}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Connection with peer {peer_info} closed.")
            # Optionally remove from active_peers if server_id is known
            # This depends on how server_id is managed in process_message
            # Example:
            # server_id = message.get('server_id')
            # if server_id in self.active_peers:
            #     del self.active_peers[server_id]

    async def handle_peer_connection(self, reader, writer):
        """
        Handle incoming peer connections.

        Args:
            reader (StreamReader): The StreamReader object.
            writer (StreamWriter): The StreamWriter object.
        """
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"

        if self.debug:
            logger.info(f"Incoming connection from {peer_info}")

        try:
            # Receive the first message
            message = await self.message.receive_message(reader)
            if message.get("type") == "hello":
                peer_server_id = message.get("server_id")
                if peer_server_id in self.revoked_peers:
                    logger.info(f"Revoked peer {peer_server_id} attempted to connect. Closing connection.")
                    writer.close()
                    await writer.wait_closed()
                    return

                if peer_server_id in self.active_peers:
                    logger.info(f"Already connected to server_id {peer_server_id}. Closing new incoming connection.")
                    writer.close()
                    await writer.wait_closed()
                    return

                if public_key_pem := message.get("public_key"):
                    self.crypto.add_peer_public_key(peer_server_id, public_key_pem)
                else:
                    logger.warning(f"No public key provided by peer {peer_server_id}.")

                # Add to active peers
                self.active_peers[peer_server_id] = {
                    'reader': reader,
                    'writer': writer,
                    'connected_at': time.time(),
                    'last_seen': time.time()
                }
                logger.info(f"Connected to peer {peer_server_id}")

                # Proceed to handle the hello message
                await self.message.process_message(message, reader, writer)

                # Start listening to the peer
                asyncio.create_task(self.listen_to_peer(reader, writer))
            else:
                logger.warning("First message was not a hello message. Closing connection.")
                writer.close()
                await writer.wait_closed()
        except ConnectionResetError as e:
            logger.error(f"Connection reset by peer {peer_info} during handling: {e}")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logger.error(f"Unexpected error handling peer connection {peer_info}: {e}")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def start_p2p_server(self):
        """
        Start the P2P server to listen for incoming connections.
        """
        try:
            server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.port, reuse_address=True
            )
            self.p2p_server = server
            logger.info(f"P2P server version {self.version} listening on {self.host}:{self.port}")

            async with server:
                await server.serve_forever()
        except OSError as e:
            logger.error(f"Failed to start server on {self.host}:{self.port}: {e}")
            if e.errno == errno.EADDRINUSE:
                logger.error(f"Port {self.port} is already in use. Please ensure the port is free and try again.")
            await self.close_p2p_server()
        except Exception as e:
            logger.error(f"Error starting P2P server: {e}")
            await self.close_p2p_server()

    async def close_p2p_server(self):
        """
        Close the P2P server gracefully.
        """
        if hasattr(self, 'p2p_server') and self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logger.info("P2P server closed.")
        else:
            logger.error("P2P server was not initialized.")

    async def start(self):
        """
        Start the Peer instance, loading peers and starting the server.
        """
        try:
            await self.load_peers()

            # Start connecting to known peers
            asyncio.create_task(self.connect_and_maintain())

            # Start the P2P server
            await self.start_p2p_server()
        finally:
            await self.shutdown()

    async def connect_and_maintain(self):
        """
        Continuously attempt to connect to known peers.
        """
        while not self.shutdown_flag:
            logger.debug("Attempting to connect to known peers...")
            await self.connect_to_known_peers()
            await asyncio.sleep(60)  # Interval between connection attempts

    async def connect_to_known_peers(self):
        """
        Attempt to connect to known peers, excluding already connected ones.
        """
        available_peers = [peer for peer in self.known_peers if peer not in self.active_peers and peer not in self.revoked_peers]
        if not available_peers:
            logger.debug("No available peers to connect.")
            return

        # Determine the number of peers to connect based on max_peers
        current_peers = len(self.active_peers)
        peers_needed = self.max_peers - current_peers
        if peers_needed <= 0:
            logger.debug(f"Already connected to maximum peers ({self.max_peers}).")
            return

        peers_to_connect = random.sample(available_peers, min(len(available_peers), peers_needed))
        for peer_info in peers_to_connect:
            host, port = peer_info.split(':')
            port = int(port)
            asyncio.create_task(self.connect_to_peer(host, port))

        self.update_active_peers()

    def update_active_peers(self):
        """
        Update the list of active peers based on current connections.
        """
        # Placeholder for any logic to update active peers
        pass

    async def shutdown(self):
        """
        Gracefully shut down the Peer instance.
        """
        logger.info("Shutting down Peer...")
        self.shutdown_flag = True

        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()

        # Close all peer connections
        for peer_id, peer_info in list(self.active_peers.items()):
            writer = peer_info['writer']
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"Connection with peer {peer_id} closed during shutdown.")
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
        Revoke a peer by their server_id.

        Args:
            server_id (str): The unique identifier of the peer to revoke.
            reason (str, optional): Reason for revocation. Defaults to "No reason provided".
        """
        if server_id in self.revoked_peers:
            logger.info(f"Peer {server_id} is already revoked.")
            return

        self.revoked_peers.add(server_id)
        logger.info(f"Revoking peer {server_id} for: {reason}")
        self.save_revoked_peers()

        # Disconnect if currently connected
        if server_id in self.active_peers:
            peer_info = self.active_peers.pop(server_id)
            writer = peer_info['writer']
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"Disconnected from revoked peer {server_id}")

        # Delegate revocation broadcasting to MessageHandler
        await self.message.broadcast_revocation(server_id, reason)

    # ----------------------------
    # Uptime Tracking
    # ----------------------------

    async def track_uptime(self):
        """
        Track the uptime of the peer.
        """
        while not self.shutdown_flag:
            await asyncio.sleep(60)  # Update uptime every 60 seconds
            self.uptime += 60
            logger.debug(f"Peer uptime updated: {self.uptime} seconds")

    # ----------------------------
    # Transaction Broadcasting
    # ----------------------------

    async def broadcast_transaction(self, txid):
        """
        Broadcast a transaction to all connected peers.

        Args:
            txid (str): The transaction ID to broadcast.
        """
        if not self.active_peers:
            logger.warning("No active peers to broadcast the transaction.")
            return

        message = {
            "type": "transaction",
            "data": {
                "txid": txid,
                "server_id": self.server_id,
                "timestamp": time.time() + self.ntp_offset
            }
        }

        # Serialize and sign the message
        serialized_message = self.message.serialize_message(message, include_signature=False)
        signature = self.crypto.sign(serialized_message)
        message['signature'] = base64.b64encode(signature).decode('utf-8')

        # Send the message to all active peers
        for peer_id, peer_info in self.active_peers.items():
            writer = peer_info['writer']
            if not writer.is_closing():
                try:
                    await self.message.send_message(writer, message)
                    logger.debug(f"Broadcasted transaction {txid} to peer {peer_id}.")
                except Exception as e:
                    logger.error(f"Failed to broadcast transaction to peer {peer_id}: {e}")
            else:
                logger.warning(f"Cannot send transaction to peer {peer_id} as the connection is closing.")

    # ----------------------------
    # Additional Peer Methods
    # ----------------------------

    async def handle_incoming_chat_message(self, message):
        """
        Handle incoming chat messages (broadcast or direct).

        Args:
            message (dict): The incoming message containing fields like:
                            {
                            "from": <from_server_id>,
                            "to": <optional to_server_id>,
                            "message": <text>,
                            "timestamp": <float timestamp>,
                            "message_id": <unique_id>
                            }
        """
        from_server_id = message.get("from")
        to_server_id = message.get("to")  # Optional for directed messages
        message_text = message.get("message")
        timestamp = message.get("timestamp")
        message_id = message.get("message_id")

        # Validate essential fields
        if not all([from_server_id, message_text, timestamp, message_id]):
            logger.warning("Incomplete chat message received. Missing one of 'from', 'message', 'timestamp', or 'message_id'.")
            return

        # Check for duplicate messages
        async with self.seen_message_lock:
            if message_id in self.seen_message_ids:
                logger.debug(f"Duplicate chat message {message_id} received. Ignoring.")
                return
            self.seen_message_ids.append(message_id)

        # Determine if the message is directed or broadcast
        if to_server_id:
            # Directed message
            if to_server_id == self.server_id:
                # This peer is the intended recipient
                logger.info(f"Direct message from {from_server_id} to {self.server_id}: {message_text}")
                print(f"[Direct] {from_server_id}: {message_text}")
            else:
                # Not the intended recipient; optionally forward to the intended recipient
                logger.debug(f"Chat message directed to {to_server_id}, not this peer.")
                await self.forward_directed_message(to_server_id, message)
        else:
            # Broadcast message
            logger.info(f"Broadcast message from {from_server_id}: {message_text}")
            print(f"[Broadcast] {from_server_id}: {message_text}")

            # Optionally rebroadcast the message to other peers
            # This helps propagate the message across the network.
            await self.rebroadcast_message(message, exclude_peer=from_server_id)

    async def forward_directed_message(self, to_server_id, message):
        """
        Forward a directed chat message to the intended peer.

        Args:
            to_server_id (str): The server_id of the intended recipient.
            message (dict): The original chat message.
        """
        if peer_info := self.active_peers.get(to_server_id):
            writer = peer_info['writer']
            try:
                await self.message.send_message(writer, {
                    "type": "chat",
                    "data": message
                })
                logger.debug(f"Forwarded directed chat message to {to_server_id}.")
            except Exception as e:
                logger.error(f"Failed to forward message to {to_server_id}: {e}")
        else:
            logger.warning(f"Cannot forward message. Peer {to_server_id} is not connected.")

    async def rebroadcast_message(self, message, exclude_peer=None):
        """
        Rebroadcast a chat message to all connected peers except the excluded one.

        Args:
            message (dict): The chat message to rebroadcast.
            exclude_peer (str, optional): The server_id to exclude from rebroadcasting.
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
                    logger.debug(f"Rebroadcasted chat message to peer {peer_id}.")
                except Exception as e:
                    logger.error(f"Failed to rebroadcast chat message to peer {peer_id}: {e}")
            else:
                logger.warning(f"Cannot rebroadcast to peer {peer_id} as the connection is closing.")