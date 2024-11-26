# peer.py

import asyncio
import contextlib
import errno
import json
import logging
import os
import socket
import time
import aiofiles
import random
import stun
import ssl
import uuid
import ntplib  # Reintroduced for NTP synchronization
from collections import deque
from block_reward import BlockReward  # Import the BlockReward class

# Import cryptography modules for SSL certificate generation
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import datetime

logger = logging.getLogger(__name__)

def create_server_ssl_context(certfile: str, keyfile: str) -> ssl.SSLContext:
    """
    Creates an SSL context for the server using the provided certificate and key files.
    """
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.check_hostname = False  # Since we're using self-signed certificates
    context.verify_mode = ssl.CERT_NONE  # Not verifying client certificates
    context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return context

def create_client_ssl_context(certfile: str, keyfile: str, cafile: str = None) -> ssl.SSLContext:
    """
    Creates an SSL context for the client using the provided certificate and key files.
    Optionally, a CA file can be provided to verify server certificates.
    """
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cafile)
    context.check_hostname = False  # Disable hostname checking for self-signed certs
    context.verify_mode = ssl.CERT_NONE  # Not verifying server certificates
    context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return context

def get_stun_info():
    try:
        nat_type, external_ip, external_port = stun.get_ip_info(stun_host='stun.l.google.com', stun_port=19302)
        logger.info(f"NAT Type: {nat_type}, External IP: {external_ip}, External Port: {external_port}")
        return external_ip, external_port
    except Exception as e:
        logger.error(f"Failed to get IP info via STUN: {e}")
        return None, None

def get_out_ip():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
        logger.info(f"External IP via socket: {ip}")
        return ip
    except Exception as e:
        logger.error(f"Failed to detect external IP address via socket: {e}")
        return None

def get_external_ip():
    external_ip, external_port = get_stun_info()
    if external_ip:
        return external_ip, external_port
    external_ip = get_out_ip()
    return external_ip, None

class Peer:
    def __init__(self, host, p2p_port, server_id, version, max_peers=10, config=None):
        self.server_id = server_id
        self.host = host
        self.p2p_port = p2p_port
        self.peers = {}
        self.active_peers = {}  # Key: server_id, Value: peer info
        self.revoked_peers = set()  # Set to store revoked peer IDs
        self.load_revoked_peers()  # Load revoked peers at startup
        self.external_ip, self.external_p2p_port = get_external_ip()
        self.version = version
        self.file_lock = asyncio.Lock()
        self.peers_changed = False
        self.connections = {}  # Key: server_id, Value: (reader, writer)
        self.shutdown_flag = False
        self.heartbeat_tasks = {}  # Key: server_id, Value: task
        self.file_write_scheduled = False
        self.file_write_delay = 10
        self.max_peers = max_peers
        self.rewrite_lock = asyncio.Lock()
        self.connection_attempts = {}
        self.peers_connecting = set()
        self.peers_connecting_lock = asyncio.Lock()
        self.connection_attempts_lock = asyncio.Lock()
        self.config = config or {}
        self.debug = self.config.get('debug', 'false').lower() == 'true'
        self.p2p_server = None
        self.last_peer_list_request = {}
        self.ntp_offset = self.get_ntp_offset()  # Now implemented
        self.received_messages = []
        self.message_lock = asyncio.Lock()
        self.seen_message_ids = deque(maxlen=10000)  # Using deque to efficiently manage seen message IDs
        self.seen_message_lock = asyncio.Lock()  # Async lock for thread-safe access

        self.crypto = None  # Initialize crypto as None
        self.block_reward = BlockReward(self)  # Pass self to BlockReward

        # List to track background tasks
        self.background_tasks = []

        # Set the logging level based on the configuration
        log_level = self.config.get('log_level', 'INFO').upper()
        logger.setLevel(log_level)

        # Paths to SSL certificate and key files
        self.certfile = self.config.get('certfile', 'peer_certificate.crt')
        self.keyfile = self.config.get('keyfile', 'peer_private.key')
        self.cafile = self.config.get('cafile', None)  # Optional, if you have CA files

        # Generate SSL cert and key if they don't exist
        self.generate_ssl_certificates()

        # Check if the certificate and key files exist
        if not os.path.isfile(self.certfile):
            logger.error(f"Certificate file not found: {self.certfile}")
            raise FileNotFoundError(f"Certificate file not found: {self.certfile}")

        if not os.path.isfile(self.keyfile):
            logger.error(f"Key file not found: {self.keyfile}")
            raise FileNotFoundError(f"Key file not found: {self.keyfile}")

        # Create SSL contexts
        self.server_ssl_context = create_server_ssl_context(self.certfile, self.keyfile)
        self.client_ssl_context = create_client_ssl_context(self.certfile, self.keyfile, self.cafile)

    def set_crypto(self, crypto):
        """Set the Crypto instance for the Peer."""
        self.crypto = crypto
        if self.block_reward:
            self.block_reward.crypto = crypto

    def get_ntp_offset(self):
        """Calculate the NTP offset using an NTP server."""
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

    def generate_ssl_certificates(self):
        """Generate a self-signed SSL certificate and private key."""
        if os.path.isfile(self.certfile) and os.path.isfile(self.keyfile):
            logger.info("SSL certificate and key already exist. Skipping generation.")
            return

        logger.info("Generating self-signed SSL certificate and private key...")

        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        # Write private key to file
        with open(self.keyfile, "wb") as key_file:
            key_file.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,  # PKCS#1
                encryption_algorithm=serialization.NoEncryption()
            ))
        logger.info(f"Private key saved to {self.keyfile}")

        # Generate self-signed certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),  # Update as needed
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"California"),  # Update as needed
            x509.NameAttribute(NameOID.LOCALITY_NAME, u"San Francisco"),  # Update as needed
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"MyPeerOrganization"),  # Update as needed
            x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),  # Update as needed
        ])

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(days=365)
            )
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
                critical=False,
            )
            .sign(private_key, hashes.SHA256(), default_backend())
        )

        # Write certificate to file
        with open(self.certfile, "wb") as cert_file:
            cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
        logger.info(f"Self-signed certificate saved to {self.certfile}")

    async def send_message(self, writer, message):
        message_data = json.dumps(message).encode('utf-8')
        writer.write(message_data + b'\n')
        await writer.drain()

    async def receive_message(self, reader):
        try:
            data = await reader.readuntil(separator=b'\n')
            message = json.loads(data.decode())
            logger.debug(f"Received message: {message}")
            return message
        except asyncio.IncompleteReadError as e:
            logger.error(f"IncompleteReadError: {e}")
            raise
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            raise

    async def send_hello_message(self, writer):
        """Send hello message without certificate."""
        hello_message = {
            'type': 'hello',
            'version': self.version,
            'server_id': self.server_id,
            'timestamp': time.time() + self.ntp_offset
        }
        send_time = time.time()
        await self.send_message(writer, hello_message)
        logger.debug(f"Sent hello message: {hello_message}")
        return send_time

    async def send_ack_message(self, writer, version, server_id):
        ack_message = {
            "type": "ack",
            "version": version,
            "server_id": server_id,
            "timestamp": time.time() + self.ntp_offset
        }
        await self.send_message(writer, ack_message)
        logger.info(f"Sent ack message to peer with server_id: {server_id}")

    async def handle_hello_message(self, message, reader, writer):
        """Handle hello message."""
        receive_time = time.time()
        peer_server_id = message['server_id']
        peer_version = message.get('version', 'unknown')
        peer_timestamp = message.get('timestamp', receive_time)
        peer_info = writer.get_extra_info('peername')
        peer_info_str = f"{peer_info[0]}:{peer_info[1]}"

        # Calculate ping time
        ping = receive_time - peer_timestamp

        # Assuming all peers are trusted since SSL handles encryption and basic authentication

        # Check for existing connection to this server_id
        if peer_server_id in self.active_peers:
            logger.info(f"Already connected to server_id {peer_server_id}. Closing new incoming connection.")
            writer.close()
            await writer.wait_closed()
            return

        # Send ack message
        await self.send_ack_message(writer, self.version, self.server_id)

        # Register the connection
        local_addr, local_port = writer.get_extra_info('sockname')
        _, bound_port = writer.get_extra_info('peername')  # Only bound_port is used

        await self.register_peer_connection(
            reader, writer, peer_info_str, peer_server_id, local_addr, local_port, bound_port, peer_version, ping
        )

    async def handle_ack_message(self, message, reader, writer):
        """Handle ack message."""
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        remote_version = message.get("version", "unknown")
        remote_server_id = message.get("server_id", "unknown")

        local_addr, local_port = writer.get_extra_info('sockname')
        _, bound_port = writer.get_extra_info('peername')  # Only bound_port is used

        # Check for existing connection to this server_id
        if remote_server_id in self.active_peers:
            logger.info(f"Already connected to server_id {remote_server_id}. Closing new connection.")
            writer.close()
            await writer.wait_closed()
            return

        receive_time = time.time()
        send_time = self.active_peers.get(remote_server_id, {}).get('send_time', receive_time)
        ping = receive_time - send_time

        # Register the peer connection
        await self.register_peer_connection(
            reader, writer, peer_info, remote_server_id, local_addr, local_port, bound_port, remote_version, ping
        )

    async def handle_peer_list_message(self, message):
        new_peers = message.get("payload", [])
        logger.info(f"Received peer list: {new_peers}")

        valid_new_peers = {}
        invalid_peers = []

        for peer_info in new_peers:
            if not self.is_valid_peer(peer_info):
                invalid_peers.append(peer_info)
                continue  # Skip invalid peers

            if peer_info == f"{self.host}:{self.p2p_port}":
                logger.info(f"Skipping self-peer: {peer_info}")
                continue  # Skip adding self-peer to valid_new_peers

            valid_new_peers[peer_info] = int(time.time())

        # Log invalid peers if debugging
        if self.debug:
            for invalid_peer in invalid_peers:
                logger.warning(f"Invalid peer received: {invalid_peer}")

        # Update peers and schedule file rewrite
        if valid_new_peers:
            self.peers.update(valid_new_peers)
            self.peers_changed = True
            await self.schedule_rewrite()

        # Try connecting to valid new peers, excluding the self-peer
        for peer_info in valid_new_peers:
            async with self.connection_attempts_lock, self.peers_connecting_lock:
                if peer_info in self.connections:
                    if self.debug:
                        logger.info(f"Already connected to {peer_info}, skipping additional connection attempt.")
                    continue

                if peer_info in self.peers_connecting:
                    if self.debug:
                        logger.info(f"Already connecting to {peer_info}, skipping additional connection attempt.")
                    continue

            host, port = peer_info.split(':')
            port = int(port)
            asyncio.create_task(self.connect_to_peer(host, port, 5))
            self.background_tasks.append(
                asyncio.create_task(self.connect_to_peer(host, port, 5))
            )

        # Update active peers after connecting
        self.update_active_peers()

    async def respond_to_heartbeat(self, writer, message):
        peer_server_id = message.get('server_id', 'unknown')

        # Create a heartbeat acknowledgment message
        heartbeat_ack = {
            "type": "heartbeat_ack",
            "server_id": self.server_id,
            "timestamp": time.time() + self.ntp_offset
        }

        try:
            # Send the heartbeat acknowledgment to the peer
            await self.send_message(writer, heartbeat_ack)
            logger.info(f"Sent heartbeat acknowledgment to peer {peer_server_id}.")
        except Exception as e:
            logger.error(f"Failed to send heartbeat acknowledgment to peer {peer_server_id}: {e}")

    async def send_peer_list(self, writer):
        logger.info("Attempting to send peer list...")

        if known_peers := list(self.peers.keys()):
            peer_list_message = {
                "type": "peer_list",
                "payload": known_peers,
                "server_id": self.server_id,
                "version": self.version,
                "timestamp": time.time() + self.ntp_offset
            }

            await self.send_message(writer, peer_list_message)
            logger.info("Sent peer list.")
        else:
            logger.warning("No known peers to send.")

    async def send_heartbeat(self, writer, server_id=None):
        try:
            while not writer.is_closing():
                if self.shutdown_flag:
                    logger.info(f"Shutdown in progress, stopping heartbeat to {server_id}.")
                    return  # Exit the heartbeat loop

                # Generate a nonce (current timestamp)
                nonce = str(time.time() + self.ntp_offset)

                # Create heartbeat message
                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id,
                    "version": self.version,
                    "timestamp": nonce,
                    "uptime": self.block_reward.total_uptime
                }

                # Send the heartbeat message
                try:
                    await self.send_message(writer, heartbeat_msg)
                    logger.debug(f"Heartbeat sent to {server_id}")

                    # Sleep in small increments to allow for quick shutdown
                    total_sleep = 0
                    heartbeat_interval = 60  # seconds
                    while total_sleep < heartbeat_interval:
                        if self.shutdown_flag:
                            logger.info(f"Shutdown in progress, stopping heartbeat to {server_id}.")
                            return
                        await asyncio.sleep(1)
                        total_sleep += 1

                except ConnectionError as e:
                    logger.warning(f"Error sending heartbeat to {server_id}: {e}")
                    writer.close()
                    await writer.wait_closed()
                    return

                except asyncio.CancelledError:
                    logger.info(f"Heartbeat task for {server_id} was cancelled.")
                    return

                except Exception as e:
                    logger.error(f"Unexpected error during heartbeat to {server_id}: {e}")
                    return

        except asyncio.CancelledError:
            logger.info(f"Heartbeat sending task was cancelled for {server_id}.")
            # Optionally close the writer if necessary
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logger.error(f"Error in heartbeat task for {server_id}: {e}")

    async def process_message(self, message, reader, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        if self.debug:
            logger.info(f"Received message from {peer_info}: {message}")

        message_type = message.get("type")
        if message_type == "hello":
            await self.handle_hello_message(message, reader, writer)
        elif message_type == "ack":
            await self.handle_ack_message(message, reader, writer)
        elif message_type == "request_peer_list":
            await self.send_peer_list(writer)
        elif message_type == "heartbeat":
            await self.handle_heartbeat(message, writer)
        elif message_type == "heartbeat_ack":
            await self.handle_heartbeat_ack(message)
        elif message_type == "peer_list":
            await self.handle_peer_list_message(message)
        elif message_type in ["direct_message", "broadcast_message"]:
            await self.handle_incoming_chat_message(message)
        elif message_type == "transaction":
            await self.handle_incoming_transaction(message)
        elif message_type == "revocation":
            await self.handle_revocation_message(message)
        elif message_type == "request_heartbeat_records":
            await self.handle_request_heartbeat_records(message, writer)
        elif message_type == "heartbeat_records":
            await self.handle_heartbeat_records(message)
        else:
            logger.warning(f"Unknown message type received: {message_type}")

    async def handle_heartbeat(self, message, writer):
        peer_server_id = message.get('server_id')
        uptime = message.get('uptime')

        # Record the heartbeat
        current_time = time.time()
        self.record_heartbeat(peer_server_id, uptime, current_time)

        # Update lastseen
        if peer_server_id in self.active_peers:
            self.active_peers[peer_server_id]['lastseen'] = int(current_time)
            self.peers_changed = True  # Mark peers as changed for later rewriting

        # Send heartbeat acknowledgment
        await self.respond_to_heartbeat(writer, message)

    def record_heartbeat(self, peer_server_id, uptime, timestamp):
        if peer_server_id not in self.active_peers:
            self.active_peers[peer_server_id] = {}
        if 'heartbeat_records' not in self.active_peers[peer_server_id]:
            self.active_peers[peer_server_id]['heartbeat_records'] = []

        self.active_peers[peer_server_id]['heartbeat_records'].append({
            'uptime': uptime,
            'timestamp': timestamp
        })

        # Limit the size of the heartbeat_records list to prevent memory issues
        self.active_peers[peer_server_id]['heartbeat_records'] = self.active_peers[peer_server_id]['heartbeat_records'][-100:]

    def verify_uptime(self, peer_server_id):
        if peer_server_id == self.server_id:
            # Self-verification can be considered true
            return True

        if peer_server_id not in self.active_peers:
            logger.warning(f"No data for peer {peer_server_id}")
            return False

        heartbeat_records = self.active_peers[peer_server_id].get('heartbeat_records', [])
        if not heartbeat_records:
            logger.warning(f"No heartbeat records for {peer_server_id}")
            return False

        # Verify that uptime increases over time and matches the timestamps
        previous_uptime = None
        previous_timestamp = None
        for record in heartbeat_records:
            uptime = record['uptime']
            timestamp = record['timestamp']

            if previous_uptime is not None:
                # Uptime should not decrease
                if uptime < previous_uptime:
                    logger.warning(f"Uptime decreased for {peer_server_id}")
                    return False
                # Uptime difference should correspond to time difference
                expected_uptime = previous_uptime + (timestamp - previous_timestamp)
                if abs(uptime - expected_uptime) > 120:  # Allow small discrepancy (e.g., 2 minutes)
                    logger.warning(f"Uptime discrepancy detected for {peer_server_id}")
                    return False

            previous_uptime = uptime
            previous_timestamp = timestamp

        return True

    async def listen_for_messages(self, reader, writer, server_id):
        addr = writer.get_extra_info('peername')
        host, port = addr[0], addr[1]
        peer_info = f"{host}:{port}"
        if self.debug:
            logger.info(f"Listening for messages from {peer_info}")
        try:
            while True:
                data_buffer = await reader.readuntil(separator=b'\n')
                if not data_buffer:
                    logger.info("Connection closed by peer.")
                    break
                message = json.loads(data_buffer.decode().strip())
                await self.process_message(message, reader, writer)
        except asyncio.IncompleteReadError as e:
            logger.error(f"IncompleteReadError with {peer_info}: {e}")
            await self.handle_disconnection(server_id)
            asyncio.create_task(self.reconnect_to_peer_by_server_id(server_id))
        except ConnectionResetError as e:
            logger.error(f"Connection reset error with {peer_info}: {e}")
            await self.handle_disconnection(server_id)
            asyncio.create_task(self.reconnect_to_peer_by_server_id(server_id))
        except Exception as e:
            logger.error(f"Error during communication with {peer_info}: {e}")
            await self.handle_disconnection(server_id)
        finally:
            logger.info(f"Closing connection with {peer_info}")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def register_peer_connection(self, reader, writer, peer_info, server_id, local_addr, local_port, bound_port, version, ping):
        """Register a successfully established peer connection."""
        self.connections[server_id] = (reader, writer)
        self.active_peers[server_id] = {
            'addr': peer_info,
            'addrlocal': f"{local_addr}:{local_port}",
            'addrbind': f"{self.external_ip}:{bound_port}",
            'server_id': server_id,
            'version': version,
            'lastseen': int(time.time()),
            'ping': round(ping, 3),
        }
        self.peers_changed = True
        await self.schedule_rewrite()
        listen_task = asyncio.create_task(self.listen_for_messages(reader, writer, server_id))
        self.background_tasks.append(listen_task)
        heartbeat_task = asyncio.create_task(self.send_heartbeat(writer, server_id))
        self.heartbeat_tasks[server_id] = heartbeat_task
        self.background_tasks.append(heartbeat_task)

        # Request peer list from the connected peer
        await self.request_peer_list(writer, peer_info)

    async def establish_peer_connection(self, reader, writer, host, port, peer_info):
        """Establish the secure connection."""
        try:
            send_time = await self.send_hello_message(writer)
            ack_message = await self.receive_message(reader)

            if ack_message.get("type") != "ack":
                raise ValueError("Failed handshake: did not receive acknowledgment.")
            server_id = ack_message.get("server_id")

            # Check for existing connection to this server_id
            if server_id in self.active_peers:
                logger.info(f"Already connected to server_id {server_id}. Closing new connection.")
                writer.close()
                await writer.wait_closed()
                return False

            # Proceed to register the connection
            receive_time = time.time()
            ping = receive_time - send_time
            version = ack_message.get("version", "unknown")

            # Register the peer connection with all the required arguments
            await self.register_peer_connection(
                reader, writer, peer_info, server_id, host, port, port, version, ping
            )

            return True
        except asyncio.IncompleteReadError as e:
            logger.error(f"IncompleteReadError while connecting to {peer_info}: {e}")
            writer.close()
            await writer.wait_closed()
            return False
        except ssl.SSLError as e:
            logger.error(f"SSL error during connection with {peer_info}: {e}")
            writer.close()
            await writer.wait_closed()
            return False
        except Exception as e:
            logger.error(f"Error establishing connection to {peer_info}: {e}")
            writer.close()
            await writer.wait_closed()
            return False

    async def connect_to_peer(self, host, port, max_retries=3):
        peer_info = f"{host}:{port}"

        async with self.peers_connecting_lock:
            # Check if already connected
            if any(peer_info == peer['addr'] for peer in self.active_peers.values()):
                logger.debug(f"Already connected to {peer_info}, skipping connection attempt.")
                return

            # Check if we are already trying to connect
            if peer_info in self.peers_connecting:
                logger.info(f"Already connecting to {peer_info}, skipping.")
                return

            self.peers_connecting.add(peer_info)

        # Initialize connection attempts
        async with self.connection_attempts_lock:
            self.connection_attempts[peer_info] = self.connection_attempts.get(peer_info, 0)

        try:
            while True:
                if self.shutdown_flag:
                    logger.info(f"Shutdown in progress, cancelling connection attempts to {peer_info}.")
                    return

                async with self.connection_attempts_lock:
                    attempts = self.connection_attempts.get(peer_info, 0)

                if attempts >= max_retries:
                    logger.warning(f"Failed to connect to {host}:{port} after {max_retries} attempts. Skipping peer.")
                    # Removed blacklisting
                    async with self.connection_attempts_lock:
                        self.connection_attempts.pop(peer_info, None)
                    return

                try:
                    logger.info(f"Attempting to secure connection to peer: {peer_info}")
                    timeout_duration = 10  # Adjust the timeout as needed
                    connect_coro = asyncio.open_connection(host, port, ssl=self.client_ssl_context)
                    reader, writer = await asyncio.wait_for(connect_coro, timeout=timeout_duration)
                    if await self.establish_peer_connection(reader, writer, host, port, peer_info):
                        logger.info(f"Successfully connected to peer: {peer_info}")
                        async with self.connection_attempts_lock:
                            self.connection_attempts.pop(peer_info, None)
                        return
                except ssl.SSLError as e:
                    logger.warning(f"SSL error while connecting to {peer_info}: {e}")
                except asyncio.TimeoutError:
                    logger.warning(f"Connection attempt to {peer_info} timed out after {timeout_duration} seconds.")
                except OSError as e:
                    logger.warning(f"Failed to connect to peer {peer_info}: {e.strerror} (Errno {e.errno})")
                except Exception as e:
                    logger.error(f"Unexpected error connecting to {peer_info}: {e}", exc_info=True)
                finally:
                    async with self.connection_attempts_lock:
                        self.connection_attempts[peer_info] = self.connection_attempts.get(peer_info, 0) + 1
                        attempts = self.connection_attempts[peer_info]
                    logger.info(f"Connection attempt {attempts} for {peer_info} failed.")
                    await asyncio.sleep(attempts * 60)
        finally:
            async with self.peers_connecting_lock:
                self.peers_connecting.discard(peer_info)

    async def connect_to_known_peers(self):
        """Attempt to connect to known peers, skipping already connected peers."""
        async with self.peers_connecting_lock:
            # Extract all connected peer_info strings from active_peers
            connected_peer_infos = {peer['addr'] for peer in self.active_peers.values()}

            # Now, filter out peers that are already connected or currently connecting
            available_peers = [
                peer for peer in self.peers
                if peer not in self.peers_connecting and
                peer not in connected_peer_infos  # Correctly skip already connected peers
            ]

        logger.debug(f"Available peers to connect: {available_peers}")

        if not available_peers:
            logger.debug("No available peers to connect.")
            return

        peers_to_connect = random.sample(available_peers, min(self.max_peers, len(available_peers)))

        for peer_info in peers_to_connect:
            host, port = peer_info.split(':')
            port = int(port)
            task = asyncio.create_task(self.connect_to_peer(host, port))
            self.background_tasks.append(task)

        # Update active peers after connection attempts
        self.update_active_peers()

    async def handle_disconnection(self, server_id):
        if self.shutdown_flag:
            return

        if server_id in self.active_peers:
            del self.active_peers[server_id]
            logger.info(f"Removed server_id {server_id} from active peers due to disconnection or error.")

        if server_id in self.connections:
            del self.connections[server_id]

        if server_id in self.heartbeat_tasks:
            task = self.heartbeat_tasks.pop(server_id)
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self.update_active_peers()
        self.peers_changed = True
        await self.schedule_rewrite()

    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"

        if self.debug:
            logger.info(f"Incoming connection from {peer_info}")

        try:
            # Receive the first message
            message = await self.receive_message(reader)
            if message.get("type") == "hello":
                peer_server_id = message.get("server_id")
                # Check for existing connection to this server_id
                if peer_server_id in self.active_peers:
                    logger.info(f"Already connected to server_id {peer_server_id}. Closing new incoming connection.")
                    writer.close()
                    await writer.wait_closed()
                    return
                # Proceed to handle the hello message
                await self.handle_hello_message(message, reader, writer)
            else:
                logger.warning("First message was not a hello message. Closing connection.")
                writer.close()
                await writer.wait_closed()
        except ConnectionResetError as e:
            logger.error(f"Connection reset by peer during handling: {e}")
            # Attempt to close the writer gracefully
            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except ConnectionResetError:
                    logger.debug("Writer was already closed by peer.")
        except Exception as e:
            logger.error(f"Unexpected error handling peer connection: {e}")
            # Attempt to close the writer gracefully
            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except ConnectionResetError:
                    logger.debug("Writer was already closed by peer.")

    async def close_p2p_server(self):
        if self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logger.info("P2P server closed.")
        else:
            logger.error("P2P server was not initialized.")

    async def start_p2p_server(self):
        try:
            server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port, ssl=self.server_ssl_context, reuse_address=True
            )
            self.p2p_server = server
            logger.info(f"P2P server version {self.version} with ID {self.server_id} listening on {self.host}:{self.p2p_port} with SSL")

            # Schedule periodic tasks before blocking
            peer_save_task = asyncio.create_task(self.schedule_periodic_peer_save())
            self.background_tasks.append(peer_save_task)

            async with server:
                await server.serve_forever()
        except OSError as e:
            logger.error(f"Failed to start server on {self.host}:{self.p2p_port}: {e}")
            if e.errno == errno.EADDRINUSE:
                logger.error(f"Port {self.p2p_port} is already in use. Please ensure the port is free and try again.")
            await self.close_p2p_server()
        except Exception as e:
            logger.error(f"Error starting P2P server: {e}")
            await self.close_p2p_server()

    async def start(self):
        try:
            if not os.path.exists("peers.dat"):
                self.addnode = self.config.get('addnode', [])
                for peer_info in self.addnode:
                    if self.is_valid_peer(peer_info):
                        self.peers[peer_info] = int(time.time())

            await self.load_peers()

            # Schedule connect_and_maintain before starting the server
            connect_task = asyncio.create_task(self.connect_and_maintain())
            self.background_tasks.append(connect_task)

            # Start BlockReward module
            await self.block_reward.start()

            # Removed leader election scheduling

            await self.start_p2p_server()
        finally:
            self.shutdown_flag = True
            await self.block_reward.shutdown()
            await self.cleanup_and_rewrite_peers()
            await self.close_p2p_server()

    async def connect_and_maintain(self):
        while not self.shutdown_flag:
            logger.debug("Attempting to connect to known peers...")
            await self.connect_to_known_peers()
            await asyncio.sleep(60)

    async def load_peers(self):
        """Load peers from peers.dat file or seed list."""
        loaded_peers_count = 0
        skipped_peers_count = 0
        addnode_set = set(self.config.get('addnode', [])) if self.config else set()

        if os.path.exists("peers.dat"):
            try:
                async with aiofiles.open("peers.dat", "r") as f:
                    async for line in f:
                        try:
                            parts = line.strip().split(":")
                            if len(parts) != 3:
                                raise ValueError("Line format incorrect, expected 3 parts.")

                            peer, port, last_seen_str = parts
                            peer_info = f"{peer}:{port}"

                            # Skip peers that are revoked
                            if peer_info in self.revoked_peers:
                                logger.info(f"Skipping revoked peer: {peer_info}")
                                skipped_peers_count += 1
                                continue

                            last_seen = int(last_seen_str)

                            if self.is_valid_peer(peer_info):
                                self.peers[peer_info] = last_seen
                                loaded_peers_count += 1
                                addnode_set.discard(peer_info)
                            else:
                                skipped_peers_count += 1
                        except ValueError as e:
                            logger.warning(f"Invalid line in peers.dat: {line} - Error: {e}")
                            skipped_peers_count += 1
            except Exception as e:
                logger.error(f"Error reading peers.dat: {e}")
        else:
            self.peers_changed = True

        for peer_info in addnode_set:
            # Skip revoked peers from seed nodes as well
            if peer_info in self.revoked_peers:
                logger.info(f"Skipping revoked seed node: {peer_info}")
                continue

            if self.is_valid_peer(peer_info):
                self.peers[peer_info] = int(time.time())
                loaded_peers_count += 1

        await self.schedule_rewrite()
        logger.info(f"Peers loaded from file. Loaded: {loaded_peers_count}, Skipped: {skipped_peers_count}.")

    async def schedule_rewrite(self):
        if not self.file_write_scheduled:
            self.file_write_scheduled = True
            rewrite_task = asyncio.create_task(self._rewrite_after_delay())
            self.background_tasks.append(rewrite_task)

    async def _rewrite_after_delay(self):
        await asyncio.sleep(self.file_write_delay)
        if self.peers_changed:
            async with self.file_lock:
                try:
                    valid_peers = {
                        peer_info: last_seen
                        for peer_info, last_seen in self.peers.items()
                        if self.is_valid_peer(peer_info) and last_seen != 0
                    }
                    async with aiofiles.open("peers.dat", "w") as f:
                        for peer_info, last_seen in valid_peers.items():
                            await f.write(f"{peer_info}:{last_seen}\n")
                    self.peers_changed = False
                    if self.debug:
                        logger.info("Peers file rewritten successfully.")
                except OSError as e:
                    logger.error(f"Failed to open peers.dat: {e}")
                except Exception as e:
                    logger.error(f"Failed to rewrite peers.dat: {e}")
        self.file_write_scheduled = False

    async def schedule_periodic_peer_save(self):
        while not self.shutdown_flag:
            await asyncio.sleep(60)
            await self.rewrite_peers_file()

    async def rewrite_peers_file(self):
        """Write the peer data to the peers.dat file, sorted by lastseen in descending order."""
        if not self.peers_changed:
            return

        async with self.file_lock:
            try:
                # Filter valid peers and sort them by lastseen in descending order
                sorted_peers = {
                    peer_info: last_seen
                    for peer_info, last_seen in sorted(
                        self.peers.items(),
                        key=lambda item: item[1],  # Sort by lastseen
                        reverse=True  # Descending order
                    )
                    if self.is_valid_peer(peer_info) and last_seen != 0
                }

                # Write sorted peers to the file
                async with aiofiles.open("peers.dat", "w") as f:
                    for peer_info, last_seen in sorted_peers.items():
                        await f.write(f"{peer_info}:{last_seen}\n")

                self.peers_changed = False
                if self.debug:
                    logger.info("Peers file rewritten successfully with sorted peers based on last seen.")
            except OSError as e:
                logger.error(f"Failed to open peers.dat: {e}")
            except Exception as e:
                logger.error(f"Failed to rewrite peers.dat: {e}")

    async def cleanup_and_rewrite_peers(self):
        self.peers = {peer_info: last_seen for peer_info, last_seen in self.peers.items() if last_seen != 0}
        self.peers_changed = True
        await self.rewrite_peers_file()
        await asyncio.gather(*[self.close_connection_by_server_id(server_id) for server_id in list(self.connections.keys())])

    async def close_connection_by_server_id(self, server_id):
        if server_id in self.connections:
            _, writer = self.connections.pop(server_id)
            if writer and not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"Successfully closed connection to server_id {server_id}")

    async def reconnect_to_peer_by_server_id(self, server_id):
        if peer_info := self.active_peers.get(server_id, {}).get('addr'):
            host, port = peer_info.split(':')
            await self._attempt_reconnect(peer_info)
        else:
            logger.info(f"No peer info found for server_id {server_id}. Cannot reconnect.")

    async def _attempt_reconnect(self, peer_info):
        logger.info(f"Attempting to reconnect to {peer_info}")

        retry_intervals = [60, 3600, 86400, 2592000]  # Retry intervals: 1 min, 1 hour, 1 day, 1 month
        attempt = 0

        while attempt < len(retry_intervals):
            if self.shutdown_flag:
                logger.info(f"Shutdown in progress, cancelling reconnection attempts for {peer_info}.")
                return

            async with self.peers_connecting_lock:
                if peer_info not in self.active_peers and peer_info not in self.peers_connecting:
                    try:
                        # Apply jitter to avoid simultaneous retries across multiple peers
                        jitter = random.uniform(0.8, 1.2)
                        backoff_time = retry_intervals[attempt] * jitter
                        logger.info(f"Scheduling reconnection attempt for {peer_info} in {backoff_time:.2f} seconds.")
                        await asyncio.sleep(backoff_time)

                        host, port = peer_info.split(':')
                        await self.connect_to_peer(host, int(port))

                        # If successfully reconnected, break out of the loop
                        if peer_info in self.active_peers:
                            logger.info(f"Successfully reconnected to {peer_info}.")
                            return  # Exit the function after a successful reconnection
                    except Exception as e:
                        logger.error(f"Reconnection attempt {attempt + 1} to {peer_info} failed: {e}")
                        attempt += 1  # Increment attempt on failure

                else:
                    logger.info(f"{peer_info} is already active or attempting to reconnect. Skipping reconnection.")
                    return  # Exit if already connected or connecting

            # Increment attempt if peer is not already active and connection failed
            attempt += 1

        logger.warning(f"Exhausted all reconnection attempts for {peer_info}.")

    async def request_peer_list(self, writer, peer_info):
        current_time = time.time()
        if peer_info in self.last_peer_list_request:
            last_request_time = self.last_peer_list_request[peer_info]
            cooldown_period = 300

            if current_time - last_request_time < cooldown_period:
                logger.info(f"Skipping peer list request to {peer_info} (cooldown period not yet passed)")
                return

        self.last_peer_list_request[peer_info] = current_time
        request_message = {
            "type": "request_peer_list",
            "server_id": self.server_id,
            "timestamp": time.time() + self.ntp_offset
        }
        await self.send_message(writer, request_message)
        logger.info(f"Request for peer list sent to {peer_info}.")

    async def update_last_seen(self, server_id):
        async with self.rewrite_lock:
            if server_id in self.active_peers:
                self.active_peers[server_id]["lastseen"] = int(time.time())
                self.peers_changed = True
                await self.schedule_rewrite()

    async def handle_incoming_chat_message(self, message):
        sender_id = message.get('sender_id', 'unknown')
        content = message.get('content', '')
        timestamp = message.get('timestamp', time.time())
        message_id = message.get('message_id')  # Retrieve the message ID

        if not message_id:
            logger.warning("Received a broadcast message without a message_id. Ignoring.")
            return

        async with self.seen_message_lock:
            if message_id in self.seen_message_ids:
                logger.debug(f"Already processed message_id {message_id}. Ignoring to prevent rebroadcast.")
                return  # Skip processing to prevent infinite rebroadcasting
            self.seen_message_ids.append(message_id)  # Mark the message as seen

        # Optional: Implement a mechanism to purge old message IDs to prevent memory bloat
        # For example, using a timestamped approach or limiting the size of the deque

        msg = {
            'sender_id': sender_id,
            'content': content,
            'timestamp': timestamp
        }

        # Store the message
        async with self.message_lock:
            self.received_messages.append(msg)
            logger.info(f"Stored message from {sender_id}: {content}")
            logger.debug(f"Current messages: {self.received_messages}")

        # Print the received message to the terminal
        print(f"\nNew message from {sender_id}: {content}\n")

        if message.get("type") == "broadcast_message":
            # Forward the broadcast message to other peers, excluding the original sender
            await self.forward_broadcast_message(message, exclude_sender=sender_id)

    async def forward_broadcast_message(self, message, exclude_sender=None):
        """Forward a broadcast message to all connected peers except the sender."""
        for server_id, (reader, writer) in self.connections.items():
            if server_id == exclude_sender:
                continue
            try:
                await self.send_message(writer, message)
                logger.debug(f"Forwarded broadcast message to {server_id}")
            except Exception as e:
                logger.error(f"Failed to forward broadcast message to {server_id}: {e}")

    async def send_message_to_peer(self, recipient_id, content):
        """Send a direct message to a specific peer."""
        logger.debug(f"Attempting to send message to {recipient_id}: {content}")
        if recipient_id not in self.connections:
            logger.warning(f"Cannot send message. Peer {recipient_id} is not connected.")
            return False
        try:
            writer = self.connections[recipient_id][1]
            message = {
                "type": "direct_message",
                "sender_id": self.server_id,
                "recipient_id": recipient_id,
                "content": content,
                "timestamp": time.time() + self.ntp_offset
            }
            await self.send_message(writer, message)
            logger.info(f"Sent message to {recipient_id}: {content}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message to {recipient_id}: {e}", exc_info=True)
            return False

    async def broadcast_message(self, content):
        """Broadcast a message to all connected peers."""
        message_id = str(uuid.uuid4())  # Generate a unique message ID
        message = {
            "type": "broadcast_message",
            "sender_id": self.server_id,
            "content": content,
            "message_id": message_id,  # Include the message ID
            "timestamp": time.time() + self.ntp_offset
        }
        logger.debug(f"Preparing to broadcast message: {message}")

        if not self.connections:
            logger.warning("No active connections to broadcast the message.")
            return

        for server_id, (reader, writer) in self.connections.items():
            try:
                await self.send_message(writer, message)
                logger.info(f"Broadcasted message to {server_id}: {content}")
            except Exception as e:
                logger.error(f"Failed to broadcast message to {server_id}: {e}", exc_info=True)

    async def handle_incoming_transaction(self, message):
        transaction = message.get('transaction')
        if self.crypto:
            await self.crypto.receive_transaction(transaction)
        else:
            logger.warning("Received transaction but Crypto module is not available.")

    async def broadcast_transaction(self, transaction):
        """Broadcast a transaction to all connected peers."""
        message = {
            'type': 'transaction',
            'transaction': transaction,
            'server_id': self.server_id,
            'timestamp': time.time() + self.ntp_offset
        }
        for server_id, (reader, writer) in self.connections.items():
            try:
                await self.send_message(writer, message)
                logger.info(f"Broadcasted transaction to {server_id}")
            except Exception as e:
                logger.error(f"Failed to broadcast transaction to {server_id}: {e}")

    async def shutdown(self):
        logger.info("Shutting down Peer...")
        self.shutdown_flag = True

        # Shut down BlockReward
        await self.block_reward.shutdown()

        # Cancel heartbeat tasks
        for server_id, task in self.heartbeat_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                    logger.info(f"Heartbeat task for {server_id} cancelled.")
                except asyncio.CancelledError:
                    logger.info(f"Heartbeat task for {server_id} cancelled via exception.")
                except Exception as e:
                    logger.error(f"Error cancelling heartbeat task for {server_id}: {e}")
        self.heartbeat_tasks.clear()

        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()

        # Close all peer connections
        for server_id, (reader, writer) in self.connections.items():
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
                logger.info(f"Connection with server_id {server_id} closed during shutdown.")
        self.connections.clear()

        # Close the P2P server
        await self.close_p2p_server()
        logger.info("Peer shutdown complete.")

    def save_revoked_peers(self):
        """Save the revoked peers list to disk."""
        try:
            with open("revoked_peers.json", "w") as f:
                json.dump(list(self.revoked_peers), f, indent=4)
            logger.info("Revoked peers list saved to disk.")
        except Exception as e:
            logger.error(f"Failed to save revoked peers list: {e}")

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

    async def handle_revocation_message(self, message):
        """Handle incoming revocation messages to revoke a peer."""
        revoked_server_id = message.get('revoked_server_id')
        reason = message.get('reason', 'No reason provided')

        if not revoked_server_id:
            logger.warning("Received revocation message without revoked_server_id. Ignoring.")
            return

        # Add to revoked peers
        self.revoked_peers.add(revoked_server_id)
        logger.info(f"Peer {revoked_server_id} has been revoked for: {reason}")
        self.save_revoked_peers()

        # Disconnect if currently connected
        if revoked_server_id in self.connections:
            reader, writer = self.connections.pop(revoked_server_id)
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"Disconnected from revoked peer {revoked_server_id}")

        # Broadcast the revocation to other peers
        await self.broadcast_revocation(revoked_server_id, reason)

    async def broadcast_revocation(self, revoked_server_id, reason):
        """Broadcast a revocation message to all connected peers."""
        revocation_message = {
            "type": "revocation",
            "revoked_server_id": revoked_server_id,
            "reason": reason,
            "timestamp": time.time() + self.ntp_offset
        }
        for server_id, (reader, writer) in self.connections.items():
            try:
                await self.send_message(writer, revocation_message)
                logger.info(f"Broadcasted revocation of {revoked_server_id} to {server_id}")
            except Exception as e:
                logger.error(f"Failed to broadcast revocation to {server_id}: {e}")

    async def revoke_peer(self, server_id, reason="No reason provided"):
        """Revoke a peer by their server_id."""
        if server_id in self.revoked_peers:
            logger.info(f"Peer {server_id} is already revoked.")
            return

        self.revoked_peers.add(server_id)
        logger.info(f"Revoking peer {server_id} for: {reason}")
        self.save_revoked_peers()

        # Disconnect if currently connected
        if server_id in self.connections:
            reader, writer = self.connections.pop(server_id)
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"Disconnected from revoked peer {server_id}")

        # Broadcast the revocation to all connected peers
        await self.broadcast_revocation(server_id, reason)

    async def handle_heartbeat_ack(self, message):
        peer_server_id = message.get('server_id', 'unknown')
        timestamp = message.get('timestamp', time.time())
        logger.debug(f"Received heartbeat acknowledgment from {peer_server_id} at {timestamp}")

        # Update the lastseen time for the peer
        if peer_server_id in self.active_peers:
            self.active_peers[peer_server_id]['lastseen'] = int(timestamp)
            logger.debug(f"Updated last seen for {peer_server_id} to {timestamp}")
        else:
            logger.warning(f"Received heartbeat_ack from unknown peer {peer_server_id}")

    async def handle_request_heartbeat_records(self, message, writer):
        """Handle incoming requests for heartbeat records."""
        # Send heartbeat records to the requesting peer
        heartbeat_records = self.active_peers.get(self.server_id, {}).get('heartbeat_records', [])
        response = {
            "type": "heartbeat_records",
            "server_id": self.server_id,
            "heartbeat_records": heartbeat_records,
            "timestamp": time.time() + self.ntp_offset
        }
        await self.send_message(writer, response)

    async def handle_heartbeat_records(self, message):
        """Handle received heartbeat records from peers."""
        peer_server_id = message.get('server_id')
        received_records = message.get('heartbeat_records', [])

        # Store or merge the received heartbeat records
        if peer_server_id not in self.active_peers:
            self.active_peers[peer_server_id] = {}

        if 'heartbeat_records' not in self.active_peers[peer_server_id]:
            self.active_peers[peer_server_id]['heartbeat_records'] = []

        existing_records = self.active_peers[peer_server_id]['heartbeat_records']
        # Merge records, avoiding duplicates
        combined_records = existing_records + received_records
        unique_records = { (rec['timestamp'], rec['uptime']): rec for rec in combined_records }
        sorted_records = sorted(unique_records.values(), key=lambda x: x['timestamp'])
        self.active_peers[peer_server_id]['heartbeat_records'] = sorted_records[-100:]  # Limit size

    def is_valid_peer(self, peer_info):
        """Validate peer information, e.g., correct format and reachable IP."""
        try:
            host, port = peer_info.split(':')
            port = int(port)
            # Validate IP address format
            socket.inet_aton(host)
            if not (1 <= port <= 65535):
                raise ValueError
            return True
        except Exception:
            logger.warning(f"Invalid peer format or unreachable: {peer_info}")
            return False

    def save_wallet(self, data):
        """Save wallet data in plaintext."""
        try:
            with open("wallet.dat", "w") as f:
                json.dump(data, f, indent=4)  # Assuming data is JSON-serializable
            logger.info("Wallet data saved in plaintext.")
        except Exception as e:
            logger.error(f"Failed to save wallet data: {e}")

    # Original encrypted load_wallet method removed

    def load_wallet(self):
        """Load wallet data from plaintext."""
        if not os.path.exists("wallet.dat"):
            logger.info("wallet.dat does not exist. Starting with an empty wallet.")
            return {}
        try:
            with open("wallet.dat", "r") as f:
                data = json.load(f)
            logger.info("Wallet data loaded from plaintext.")
            return data
        except Exception as e:
            logger.error(f"Failed to load wallet data: {e}")
            return {}
