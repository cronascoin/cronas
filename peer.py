# Copyright 2023 Cronas.org
# peer.py

import asyncio
import contextlib
import datetime
import errno
import json
import logging
import os
import socket
import time
import ipaddress
import aiofiles
import random
import ntplib
import stun
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import base64

logging.basicConfig(level=logging.INFO)

def get_stun_info():
    try:
        nat_type, external_ip, external_port = stun.get_ip_info(stun_host='stun.l.google.com', stun_port=19302)
        logging.info(f"NAT Type: {nat_type}, External IP: {external_ip}, External Port: {external_port}")
        return external_ip, external_port
    except Exception as e:
        logging.error(f"Failed to get IP info via STUN: {e}")
        return None, None

def get_out_ip():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
        logging.info(f"External IP via socket: {ip}")
        return ip
    except Exception as e:
        logging.error(f"Failed to detect external IP address via socket: {e}")
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
        self.trust_store = {}
        self.certificates = {}
        self.blacklist = set()
        self.key_pair = self.generate_key_pair()
        self.certificate = self.generate_self_signed_certificate()
        self.external_ip, self.external_p2p_port = get_external_ip()
        self.hello_seq = 0
        self.version = version
        self.file_lock = asyncio.Lock()
        self.peers_changed = False
        self.connections = {}  # Key: server_id, Value: (reader, writer)
        self.shutdown_flag = False
        self.heartbeat_tasks = {}  # Key: server_id, Value: task
        self.file_write_scheduled = False
        self.pending_file_write = False
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
        self.start_time = datetime.datetime.now()
        self.ntp_offset = self.get_ntp_offset()
        self.load_key_pair()
        self.load_certificate()  # Load or generate own certificate
        self.load_trust_store()  # Load the trust store

        # Set the logging level based on the configuration
        log_level = self.config.get('log_level', 'INFO').upper()
        logging.getLogger().setLevel(log_level)

    # RSA key generation for Web of Trust
    def generate_key_pair(self):
        """Generates an RSA key pair for signing and encryption."""
        return RSA.generate(2048)

    def generate_self_signed_certificate(self):
        """Generates a self-signed certificate."""
        public_key_pem = self.key_pair.publickey().export_key().decode()
        cert_data = json.dumps({
            'server_id': self.server_id,
            'public_key': public_key_pem,
        }).encode()
        h = SHA256.new(cert_data)
        signature = pkcs1_15.new(self.key_pair).sign(h)
        return {
            'server_id': self.server_id,
            'public_key': public_key_pem,
            'signatures': {self.server_id: base64.b64encode(signature).decode()},
        }

    def get_ntp_offset(self):
        try:
            client = ntplib.NTPClient()
            response = client.request('pool.ntp.org')
            ntp_time = datetime.datetime.fromtimestamp(response.tx_time)
            system_time = datetime.datetime.now()
            offset = (ntp_time - system_time).total_seconds()
            logging.info(f"NTP time: {ntp_time}, System time: {system_time}, Offset: {offset}")

            if abs(offset) >= 1:
                logging.warning(f"Significant time discrepancy detected: {offset} seconds between system time and NTP time.")

            return offset
        except Exception as e:
            logging.error(f"Failed to get NTP time: {e}")
            return 0

    def is_private_ip(self, ip):
        return ipaddress.ip_address(ip).is_private

    def detect_ip_address(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                ip = s.getsockname()[0]
            return ip
        except Exception as e:
            logging.info(f"Failed to detect external IP address: {e}")
            return '127.0.0.1'

    def get_uptime(self):
        current_time = datetime.datetime.now()
        uptime = current_time - self.start_time
        return uptime.total_seconds()

    def is_valid_peer(self, peer_info):
        try:
            ip, port = peer_info.split(':')
            ipaddress.ip_address(ip)

            if ip in [self.host, self.external_ip, "127.0.0.1", "localhost"]:
                logging.info(f"Skipping self-peer with IP: {ip}")
                return False

            port = int(port)
            if not (1 <= port <= 65535):
                logging.warning(f"Invalid port number: {port}")
                return False

            return True
        except ValueError as e:
            logging.error(f"Invalid peer address '{peer_info}': {e}")
            return False

    def mark_peer_changed(self):
        self.peers_changed = True
        logging.debug("Peers data marked as changed.")

    def update_active_peers(self):
        self.active_peers = dict(sorted(
            self.active_peers.items(),
            key=lambda x: float(x[1]['ping']) if x[1]['ping'] is not None else float('inf')
        )[:self.max_peers])

    # Sign and verify certificates
    def sign_certificate(self, peer_certificate):
        """Sign a peer's certificate using the local private key."""
        cert_data = json.dumps(peer_certificate['public_key']).encode()
        h = SHA256.new(cert_data)
        signature = pkcs1_15.new(self.key_pair).sign(h)
        return base64.b64encode(signature).decode()

    def verify_certificate(self, peer_certificate, signature, public_key):
        """Verifies a peer's certificate using the public key."""
        cert_data = json.dumps(peer_certificate['public_key']).encode()
        h = SHA256.new(cert_data)
        try:
            pkcs1_15.new(RSA.import_key(public_key)).verify(h, base64.b64decode(signature))
            return True
        except (ValueError, TypeError):
            return False

    def verify_trust(self, peer_certificate):
        """Verifies the peer's certificate based on known trusted peers."""
        for trusted_peer_id, trusted_peer in self.trust_store.items():
            if 'signature' in trusted_peer['certificate']:
                signature = trusted_peer['certificate']['signature']
                if self.verify_certificate(peer_certificate, signature, trusted_peer['certificate']['public_key']):
                    return True
        return False

    async def send_message(self, writer, message):
        message_data = json.dumps(message).encode('utf-8')
        writer.write(message_data + b'\n')
        await writer.drain()

    async def send_hello_message(self, writer):
        """Send hello message along with the self-signed certificate."""
        hello_message = {
            'type': 'hello',
            'version': self.version,  # Include version
            'server_id': self.server_id,
            'certificate': self.certificate,  # Include your certificate
            'timestamp': time.time() + self.ntp_offset
        }
        send_time = time.time()
        await self.send_message(writer, hello_message)
        logging.debug(f"Sent hello message: {hello_message}")
        return send_time

    async def receive_message(self, reader):
        try:
            data = await reader.readuntil(separator=b'\n')
            message = json.loads(data.decode())
            logging.debug(f"Received message: {message}")
            return message
        except asyncio.IncompleteReadError as e:
            logging.error(f"IncompleteReadError: {e}")
            raise
        except Exception as e:
            logging.error(f"Error receiving message: {e}")
            raise

    async def send_ack_message(self, writer, version, server_id):
        ack_message = {
            "type": "ack",
            "version": version,
            "server_id": server_id,
            "certificate": self.certificate,  # Include your certificate
            "timestamp": time.time() + self.ntp_offset
        }
        await self.send_message(writer, ack_message)
        logging.info(f"Sent ack message to peer with server_id: {server_id}")

    async def handle_hello_message(self, message, reader, writer):
        """Handle hello message and verify the peer's certificate."""
        receive_time = time.time()
        peer_certificate = message['certificate']
        peer_server_id = message['server_id']
        peer_version = message.get('version', 'unknown')
        peer_timestamp = message.get('timestamp', receive_time)
        peer_info = writer.get_extra_info('peername')
        peer_info_str = f"{peer_info[0]}:{peer_info[1]}"

        # Calculate ping time
        ping = receive_time - peer_timestamp

        # Verify the peer's certificate
        if self.verify_peer_certificate(peer_certificate):
            # Add the peer's certificate to the trust store
            self.trust_store[peer_server_id] = {'certificate': peer_certificate, 'trusted': True}
            logging.info(f"Trusted peer {peer_server_id} connection established.")
            self.save_trust_store()  # Save the updated trust store
        else:
            logging.warning(f"Untrusted peer {peer_server_id} attempting connection. Blacklisting.")
            async with self.peers_connecting_lock:
                self.blacklist.add(peer_info_str)
            writer.close()
            await writer.wait_closed()
            return  # Terminate the connection

        # Check for existing connection to this server_id
        if peer_server_id in self.active_peers:
            logging.info(f"Already connected to server_id {peer_server_id}. Closing new incoming connection.")
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
        """Handle ack message and verify the peer's certificate."""
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        remote_version = message.get("version", "unknown")
        remote_server_id = message.get("server_id", "unknown")
        ack_certificate = message.get('certificate')

        local_addr, local_port = writer.get_extra_info('sockname')
        _, bound_port = writer.get_extra_info('peername')  # Only bound_port is used

        # Verify the peer's certificate
        if ack_certificate and self.verify_peer_certificate(ack_certificate):
            # Add the peer's certificate to the trust store
            self.trust_store[remote_server_id] = {'certificate': ack_certificate, 'trusted': True}
            logging.info(f"Trusted peer {remote_server_id} added to trust store from ack message.")
            self.save_trust_store()  # Save the updated trust store
        else:
            logging.warning(f"Failed to verify certificate from ack message of peer {remote_server_id}.")
            writer.close()
            await writer.wait_closed()
            return  # Terminate the connection

        # Check for existing connection to this server_id
        if remote_server_id in self.active_peers:
            logging.info(f"Already connected to server_id {remote_server_id}. Closing new connection.")
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
        logging.info(f"Received peer list: {new_peers}")

        valid_new_peers = {
            peer_info: self.peers.get(peer_info, 0)  # Do not update lastseen, retain previous timestamp
            for peer_info in new_peers
            if self.is_valid_peer(peer_info)
        }

        invalid_peers = [
            peer_info for peer_info in new_peers
            if not self.is_valid_peer(peer_info)
        ]

        for invalid_peer in invalid_peers:
            if self.debug:
                logging.warning(f"Invalid peer received: {invalid_peer}")

        # Update peers list without changing lastseen
        self.peers.update(valid_new_peers)
        if valid_new_peers:
            self.peers_changed = True
            await self.schedule_rewrite()

        # Attempt to connect to valid new peers
        for peer_info in valid_new_peers:
            if peer_info == f"{self.host}:{self.p2p_port}":
                logging.info(f"Skipping self-peer: {peer_info}")
                continue

            async with self.connection_attempts_lock, self.peers_connecting_lock:
                if peer_info in self.connections:
                    if self.debug:
                        logging.info(f"Already connected to {peer_info}, skipping additional connection attempt.")
                    continue

                if peer_info in self.peers_connecting:
                    if self.debug:
                        logging.info(f"Already connecting to {peer_info}, skipping additional connection attempt.")
                    continue

            host, port = peer_info.split(':')
            port = int(port)
            asyncio.create_task(self.connect_to_peer(host, port, 5))

        self.update_active_peers()


    async def respond_to_heartbeat(self, writer, message):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        peer_server_id = message.get('server_id')

        # Update the lastseen for the peer in active peers
        if peer_server_id in self.active_peers:
            self.active_peers[peer_server_id]['lastseen'] = int(time.time())
            self.peers_changed = True  # Mark peers as changed for later rewriting

            # Also update lastseen in the self.peers dictionary to be saved to peers.dat
            if peer_info in self.peers:
                self.peers[peer_info] = int(time.time())
                logging.debug(f"Updated last seen for {peer_server_id} in peers.dat.")

            # Schedule a rewrite to save peers.dat with updated lastseen
            await self.schedule_rewrite()

        # Send back an acknowledgment for the heartbeat
        ack_message = {
            "type": "heartbeat_ack",
            "payload": "pong",
            "server_id": self.server_id,
            "timestamp": time.time() + self.ntp_offset
        }
        await self.send_message(writer, ack_message)
        if self.debug:
            logging.info(f"Sent heartbeat acknowledgment to {peer_server_id}.")


    async def send_peer_list(self, writer):
        logging.info("Attempting to send peer list...")

        if known_peers := list(self.peers.keys()):
            peer_list_message = {
                "type": "peer_list",
                "payload": known_peers,
                "server_id": self.server_id,
                "version": self.version,
                "timestamp": time.time() + self.ntp_offset
            }

            await self.send_message(writer, peer_list_message)
            logging.info("Sent peer list.")
        else:
            logging.warning("No known peers to send.")

    async def send_heartbeat(self, writer, server_id=None):
        try:
            while not writer.is_closing():
                if self.shutdown_flag:
                    logging.info(f"Shutdown in progress, stopping heartbeat to {server_id}.")
                    return  # Exit the heartbeat loop

                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id,
                    "version": self.version,
                    "timestamp": time.time() + self.ntp_offset
                }

                try:
                    await self.send_message(writer, heartbeat_msg)
                    logging.debug(f"Heartbeat sent to {server_id}")

                    # Sleep in small increments to allow for quick shutdown
                    total_sleep = 0
                    heartbeat_interval = 60  # seconds
                    while total_sleep < heartbeat_interval:
                        if self.shutdown_flag:
                            logging.info(f"Shutdown in progress, stopping heartbeat to {server_id}.")
                            return
                        await asyncio.sleep(1)
                        total_sleep += 1

                except ConnectionError as e:
                    logging.warning(f"Error sending heartbeat to {server_id}: {e}")
                    writer.close()
                    await writer.wait_closed()
                    return

                except asyncio.CancelledError:
                    logging.info(f"Heartbeat task for {server_id} was cancelled.")
                    return

                except Exception as e:
                    logging.error(f"Unexpected error during heartbeat to {server_id}: {e}")
                    return

        except asyncio.CancelledError:
            logging.info(f"Heartbeat sending task was cancelled for {server_id}.")
            # Optionally close the writer if necessary
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logging.error(f"Error in heartbeat task for {server_id}: {e}")

    async def process_message(self, message, reader, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        if self.debug:
            logging.info(f"Received message from {peer_info}: {message}")

        message_type = message.get("type")
        if message_type == "hello":
            await self.handle_hello_message(message, reader, writer)
        elif message_type == "ack":
            await self.handle_ack_message(message, reader, writer)
        elif message_type == "request_peer_list":
            await self.send_peer_list(writer)
        elif message_type == "heartbeat":
            await self.respond_to_heartbeat(writer, message)
        elif message_type == "peer_list":
            await self.handle_peer_list_message(message)
        elif message_type == "certificate_signature":
            await self.handle_certificate_signature(message)

    async def listen_for_messages(self, reader, writer, server_id):
        addr = writer.get_extra_info('peername')
        host, port = addr[0], addr[1]
        peer_info = f"{host}:{port}"
        if self.debug:
            logging.info(f"Listening for messages from {peer_info}")
        try:
            while True:
                data_buffer = await reader.readuntil(separator=b'\n')
                if not data_buffer:
                    logging.info("Connection closed by peer.")
                    break
                message = json.loads(data_buffer.decode().strip())
                await self.process_message(message, reader, writer)
        except asyncio.IncompleteReadError as e:
            logging.error(f"IncompleteReadError with {peer_info}: {e}")
            await self.handle_disconnection(server_id)
            asyncio.create_task(self.reconnect_to_peer_by_server_id(server_id))
        except ConnectionResetError as e:
            logging.error(f"Connection reset error with {peer_info}: {e}")
            await self.handle_disconnection(server_id)
            asyncio.create_task(self.reconnect_to_peer_by_server_id(server_id))
        except Exception as e:
            logging.error(f"Error during communication with {peer_info}: {e}")
            await self.handle_disconnection(server_id)
        finally:
            logging.info(f"Closing connection with {peer_info}")
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
            'lastseen': int(time.time()),  # Only update lastseen upon actual connection
            'ping': round(ping, 3),
        }
        self.peers_changed = True
        await self.schedule_rewrite()
        asyncio.create_task(self.listen_for_messages(reader, writer, server_id))
        heartbeat_task = asyncio.create_task(self.send_heartbeat(writer, server_id))
        self.heartbeat_tasks[server_id] = heartbeat_task

        # Request peer list from the connected peer
        await self.request_peer_list(writer, peer_info)


    async def establish_peer_connection(self, reader, writer, host, port, peer_info):
        """Establish the connection and verify trust using certificates."""
        async with self.peers_connecting_lock:
            if peer_info in self.blacklist:
                logging.warning(f"Peer {peer_info} is blacklisted. Aborting connection.")
                writer.close()
                await writer.wait_closed()
                return False

        try:
            # Get the local and bound ports for the connection
            local_addr, local_port = writer.get_extra_info('sockname')
            _, bound_port = writer.get_extra_info('peername')  # Only bound_port is used

            send_time = await self.send_hello_message(writer)
            ack_message = await self.receive_message(reader)

            if ack_message.get("type") != "ack":
                raise ValueError("Failed handshake: did not receive acknowledgment.")
            server_id = ack_message.get("server_id")
            ack_certificate = ack_message.get('certificate')

            # Check for existing connection to this server_id
            if server_id in self.active_peers:
                logging.info(f"Already connected to server_id {server_id}. Closing new connection.")
                writer.close()
                await writer.wait_closed()
                return False

            # Verify the peer's certificate
            if ack_certificate and self.verify_peer_certificate(ack_certificate):
                # Add the peer's certificate to the trust store
                self.trust_store[server_id] = {'certificate': ack_certificate, 'trusted': True}
                logging.info(f"Trusted peer {server_id} added to trust store from ack message.")
                self.save_trust_store()
            else:
                logging.warning(f"Failed to verify certificate from ack message of peer {server_id}. Closing connection.")
                writer.close()
                await writer.wait_closed()
                async with self.peers_connecting_lock:
                    self.blacklist.add(peer_info)
                return False

            # Sign the peer's certificate and send your signature
            peer_signature = self.sign_peer_certificate(ack_certificate)
            signature_message = {
                "type": "certificate_signature",
                "server_id": self.server_id,
                "signature": peer_signature,
                "timestamp": time.time() + self.ntp_offset
            }
            await self.send_message(writer, signature_message)

            # After receiving ack_message
            receive_time = time.time()
            ping = receive_time - send_time
            version = ack_message.get("version", "unknown")

            # Register the peer connection with all the required arguments
            await self.register_peer_connection(
                reader, writer, peer_info, server_id, local_addr, local_port, bound_port, version, ping
            )

            return True
        except asyncio.IncompleteReadError as e:
            logging.error(f"IncompleteReadError while connecting to {peer_info}: {e}")
            writer.close()
            await writer.wait_closed()
            return False
        except Exception as e:
            logging.error(f"Error establishing connection to {peer_info}: {e}")
            writer.close()
            await writer.wait_closed()
            return False

    async def connect_to_peer(self, host, port, max_retries=5):
        peer_info = f"{host}:{port}"

        async with self.peers_connecting_lock:
            # Check if already connected
            if any(peer_info == peer['addr'] for peer in self.active_peers.values()):
                logging.debug(f"Already connected to {peer_info}, skipping connection attempt.")
                return

            # Check if we are already trying to connect
            if peer_info in self.peers_connecting:
                logging.info(f"Already connecting to {peer_info}, skipping.")
                return

            self.peers_connecting.add(peer_info)

        # Initialize connection attempts
        async with self.connection_attempts_lock:
            self.connection_attempts[peer_info] = self.connection_attempts.get(peer_info, 0)

        try:
            while True:
                if self.shutdown_flag:
                    logging.info(f"Shutdown in progress, cancelling connection attempts to {peer_info}.")
                    return

                async with self.connection_attempts_lock:
                    attempts = self.connection_attempts.get(peer_info, 0)

                if attempts >= max_retries:
                    logging.warning(f"Failed to connect to {host}:{port} after {max_retries} attempts. Blacklisting peer.")
                    async with self.peers_connecting_lock:
                        self.blacklist.add(peer_info)
                    async with self.connection_attempts_lock:
                        self.connection_attempts.pop(peer_info, None)
                    return

                try:
                    logging.info(f"Attempting to connect to peer: {peer_info}")
                    timeout_duration = 10  # Adjust the timeout as needed
                    connect_coro = asyncio.open_connection(host, port)
                    reader, writer = await asyncio.wait_for(connect_coro, timeout=timeout_duration)
                    if await self.establish_peer_connection(reader, writer, host, port, peer_info):
                        logging.info(f"Successfully connected to peer: {peer_info}")
                        async with self.connection_attempts_lock:
                            self.connection_attempts.pop(peer_info, None)
                        return
                except asyncio.TimeoutError:
                    logging.warning(f"Connection attempt to {peer_info} timed out after {timeout_duration} seconds.")
                except OSError as e:
                    logging.warning(f"Failed to connect to peer {peer_info}: {e.strerror} (Errno {e.errno})")
                except Exception as e:
                    logging.error(f"Unexpected error connecting to {peer_info}: {e}", exc_info=True)
                finally:
                    async with self.connection_attempts_lock:
                        self.connection_attempts[peer_info] = self.connection_attempts.get(peer_info, 0) + 1
                        attempts = self.connection_attempts[peer_info]
                    logging.info(f"Connection attempt {attempts} for {peer_info} failed.")
                    await asyncio.sleep(attempts * 60)
        finally:
            async with self.peers_connecting_lock:
                self.peers_connecting.discard(peer_info)


    async def connect_to_known_peers(self):
        """Attempt to connect to known peers, skipping blacklisted or already connected peers."""
        async with self.peers_connecting_lock:
            available_peers = [
                peer for peer in self.peers
                if peer not in self.peers_connecting and 
                peer not in self.blacklist and 
                peer not in self.active_peers  # Skip already connected peers
            ]

        logging.debug(f"Available peers to connect: {available_peers}")

        if not available_peers:
            logging.info("No available peers to connect.")
            return

        peers_to_connect = random.sample(available_peers, min(self.max_peers, len(available_peers)))

        tasks = [asyncio.create_task(self.connect_to_peer(host, int(port)))
                for host, port in (peer.split(':') for peer in peers_to_connect)]

        await asyncio.gather(*tasks, return_exceptions=True)

        # Update active peers after connection attempts
        self.update_active_peers()


    async def handle_disconnection(self, server_id):
        if self.shutdown_flag:
            return

        if server_id in self.active_peers:
            del self.active_peers[server_id]
            logging.info(f"Removed server_id {server_id} from active peers due to disconnection or error.")

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
        try:
            # Receive the first message
            message = await self.receive_message(reader)
            if message.get("type") == "hello":
                peer_server_id = message.get("server_id")
                # Check for existing connection to this server_id
                if peer_server_id in self.active_peers:
                    logging.info(f"Already connected to server_id {peer_server_id}. Closing new incoming connection.")
                    writer.close()
                    await writer.wait_closed()
                    return
                # Proceed to handle the hello message
                await self.handle_hello_message(message, reader, writer)
            else:
                logging.warning("First message was not a hello message. Closing connection.")
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logging.error(f"Error handling peer connection: {e}")
            writer.close()
            await writer.wait_closed()

    async def close_p2p_server(self):
        if self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logging.info("P2P server closed.")
        else:
            logging.error("P2P server was not initialized.")

    async def start_p2p_server(self):
        try:
            server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port, reuse_address=True
            )
            self.p2p_server = server
            logging.info(f"P2P server version {self.version} with ID {self.server_id} listening on {self.host}:{self.p2p_port}")

            # Schedule periodic tasks before blocking
            asyncio.create_task(self.schedule_periodic_peer_save())

            async with server:
                await server.serve_forever()
        except OSError as e:
            logging.error(f"Failed to start server on {self.host}:{self.p2p_port}: {e}")
            if e.errno == errno.EADDRINUSE:
                logging.error(f"Port {self.p2p_port} is already in use. Please ensure the port is free and try again.")
            await self.close_p2p_server()
        except Exception as e:
            logging.error(f"Error starting P2P server: {e}")
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
            asyncio.create_task(self.connect_and_maintain())

            await self.start_p2p_server()
        finally:
            self.shutdown_flag = True
            await self.cleanup_and_rewrite_peers()
            await self.close_p2p_server()

    async def connect_and_maintain(self):
        while not self.shutdown_flag:
            logging.debug("Attempting to connect to known peers...")
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

                            # Skip peers that are blacklisted
                            if peer_info in self.blacklist:
                                logging.info(f"Skipping blacklisted peer: {peer_info}")
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
                            logging.warning(f"Invalid line in peers.dat: {line} - Error: {e}")
                            skipped_peers_count += 1
            except Exception as e:
                logging.error(f"Error reading peers.dat: {e}")
        else:
            self.peers_changed = True

        for peer_info in addnode_set:
            # Skip blacklisted peers from seed nodes as well
            if peer_info in self.blacklist:
                logging.info(f"Skipping blacklisted seed node: {peer_info}")
                continue

            if self.is_valid_peer(peer_info):
                self.peers[peer_info] = int(time.time())
                loaded_peers_count += 1

        await self.schedule_rewrite()
        logging.info(f"Peers loaded from file. Loaded: {loaded_peers_count}, Skipped: {skipped_peers_count}.")

    async def schedule_rewrite(self):
        if not self.file_write_scheduled:
            self.file_write_scheduled = True
            asyncio.create_task(self._rewrite_after_delay())

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
                        logging.info("Peers file rewritten successfully.")
                except OSError as e:
                    logging.error(f"Failed to open peers.dat: {e}")
                except Exception as e:
                    logging.error(f"Failed to rewrite peers.dat: {e}")
        self.file_write_scheduled = False

    async def schedule_periodic_peer_save(self):
        while True:
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
                    logging.info("Peers file rewritten successfully with sorted peers based on last seen.")
            except OSError as e:
                logging.error(f"Failed to open peers.dat: {e}")
            except Exception as e:
                logging.error(f"Failed to rewrite peers.dat: {e}")


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
            logging.info(f"Successfully closed connection to server_id {server_id}")

    async def reconnect_to_peer_by_server_id(self, server_id):
        if peer_info := self.active_peers.get(server_id, {}).get('addr'):
            host, port = peer_info.split(':')
            await self._attempt_reconnect(peer_info)
        else:
            logging.info(f"No peer info found for server_id {server_id}. Cannot reconnect.")

    async def _attempt_reconnect(self, peer_info):
        logging.info(f"Attempting to reconnect to {peer_info}")

        retry_intervals = [60, 3600, 86400, 2592000]  # Retry intervals: 1 min, 1 hour, 1 day, 1 month
        attempt = 0

        while attempt < len(retry_intervals):
            if self.shutdown_flag:
                logging.info(f"Shutdown in progress, cancelling reconnection attempts for {peer_info}.")
                return

            async with self.peers_connecting_lock:
                if peer_info not in self.active_peers and peer_info not in self.peers_connecting:
                    try:
                        # Apply jitter to avoid simultaneous retries across multiple peers
                        jitter = random.uniform(0.8, 1.2)
                        backoff_time = retry_intervals[attempt] * jitter
                        logging.info(f"Scheduling reconnection attempt for {peer_info} in {backoff_time:.2f} seconds.")
                        await asyncio.sleep(backoff_time)

                        host, port = peer_info.split(':')
                        await self.connect_to_peer(host, int(port))

                        # If successfully reconnected, break out of the loop
                        if peer_info in self.active_peers:
                            logging.info(f"Successfully reconnected to {peer_info}.")
                            return  # Exit the function after a successful reconnection
                    except Exception as e:
                        logging.error(f"Reconnection attempt {attempt + 1} to {peer_info} failed: {e}")
                        attempt += 1  # Increment attempt on failure

                else:
                    logging.info(f"{peer_info} is already active or attempting to reconnect. Skipping reconnection.")
                    return  # Exit if already connected or connecting

            # Increment attempt if peer is not already active and connection failed
            attempt += 1

        logging.warning(f"Exhausted all reconnection attempts for {peer_info}.")


    async def request_peer_list(self, writer, peer_info):
        current_time = time.time()
        if peer_info in self.last_peer_list_request:
            last_request_time = self.last_peer_list_request[peer_info]
            cooldown_period = 300

            if current_time - last_request_time < cooldown_period:
                logging.info(f"Skipping peer list request to {peer_info} (cooldown period not yet passed)")
                return

        self.last_peer_list_request[peer_info] = current_time
        request_message = {
            "type": "request_peer_list",
            "server_id": self.server_id,
            "timestamp": time.time() + self.ntp_offset
        }
        await self.send_message(writer, request_message)
        logging.info(f"Request for peer list sent to {peer_info}.")

    async def update_last_seen(self, server_id):
        async with self.rewrite_lock:
            if server_id in self.active_peers:
                self.active_peers[server_id]["lastseen"] = int(time.time())
                self.peers_changed = True
                await self.schedule_rewrite()

    def verify_peer_certificate(self, peer_certificate):
        """Verify the peer's certificate based on the Web of Trust."""
        peer_server_id = peer_certificate['server_id']
        peer_public_key_pem = peer_certificate['public_key']
        peer_public_key = RSA.import_key(peer_public_key_pem)
        signatures = peer_certificate.get('signatures', {})

        # Verify the self-signature
        cert_data = json.dumps({
            'server_id': peer_server_id,
            'public_key': peer_public_key_pem,
        }).encode()
        h = SHA256.new(cert_data)

        # Check if the peer has signed their own certificate
        self_signature_b64 = signatures.get(peer_server_id)
        if not self_signature_b64:
            logging.warning(f"Peer {peer_server_id} has no self-signature.")
            return False

        try:
            self_signature = base64.b64decode(self_signature_b64)
            pkcs1_15.new(peer_public_key).verify(h, self_signature)
        except (ValueError, TypeError):
            logging.warning(f"Invalid self-signature for peer {peer_server_id}.")
            return False

        # Check if any trusted peer has signed this certificate
        for trusted_peer_id, trusted_peer in self.trust_store.items():
            if trusted_peer_id == peer_server_id:
                continue  # Skip self
            trusted_certificate = trusted_peer['certificate']
            trusted_public_key_pem = trusted_certificate['public_key']
            trusted_public_key = RSA.import_key(trusted_public_key_pem)
            if signature_b64 := signatures.get(trusted_peer_id):
                try:
                    signature = base64.b64decode(signature_b64)
                    pkcs1_15.new(trusted_public_key).verify(h, signature)
                    logging.info(f"Peer {peer_server_id} is trusted via {trusted_peer_id}.")
                    return True
                except (ValueError, TypeError):
                    continue  # Invalid signature, try next

        # For initial trust, accept valid self-signed certificates
        logging.info(f"Peer {peer_server_id} has a valid self-signature. Accepting for initial trust.")
        return True

    def sign_peer_certificate(self, peer_certificate):
        """Sign a peer's certificate with the local private key."""
        peer_server_id = peer_certificate['server_id']
        peer_public_key_pem = peer_certificate['public_key']
        cert_data = json.dumps({
            'server_id': peer_server_id,
            'public_key': peer_public_key_pem,
        }).encode()
        h = SHA256.new(cert_data)
        signature = pkcs1_15.new(self.key_pair).sign(h)
        return base64.b64encode(signature).decode()

    async def handle_certificate_signature(self, message):
        """Handle incoming certificate signatures from peers."""
        peer_server_id = message['server_id']
        signature_b64 = message['signature']

        # Add the signature to our certificate
        if peer_server_id not in self.certificate['signatures']:
            self.certificate['signatures'][peer_server_id] = signature_b64
            logging.info(f"Added signature from peer {peer_server_id} to our certificate.")

            # Save your updated certificate to disk
            self.save_certificate()
        else:
            logging.info(f"Already have signature from peer {peer_server_id}.")

    def save_certificate(self):
        """Save the peer's own certificate to disk."""
        try:
            with open("my_certificate.json", "w") as f:
                json.dump(self.certificate, f)
            logging.info("Own certificate saved to disk.")
        except Exception as e:
            logging.error(f"Failed to save own certificate: {e}")

    def load_certificate(self):
        """Load the peer's own certificate from disk."""
        if os.path.exists("my_certificate.json"):
            try:
                with open("my_certificate.json", "r") as f:
                    self.certificate = json.load(f)
                logging.info("Own certificate loaded from disk.")
            except Exception as e:
                logging.error(f"Failed to load own certificate: {e}")
                # If loading fails, generate a new certificate
                self.certificate = self.generate_self_signed_certificate()
                self.save_certificate()
        else:
            # If the certificate file doesn't exist, generate and save a new one
            self.certificate = self.generate_self_signed_certificate()
            self.save_certificate()

    def save_trust_store(self):
        """Save the trust store to disk."""
        try:
            with open("trust_store.json", "w") as f:
                json.dump(self.trust_store, f)
            logging.info("Trust store saved to disk.")
        except Exception as e:
            logging.error(f"Failed to save trust store: {e}")

    def load_trust_store(self):
        """Load the trust store from disk."""
        if os.path.exists("trust_store.json"):
            try:
                with open("trust_store.json", "r") as f:
                    self.trust_store = json.load(f)
                logging.info("Trust store loaded from disk.")
            except Exception as e:
                logging.error(f"Failed to load trust store: {e}")
                self.trust_store = {}
        else:
            self.trust_store = {}
            logging.info("No existing trust store found. Starting with an empty trust store.")

    def save_key_pair(self):
        """Save the RSA key pair to disk."""
        try:
            private_key_pem = self.key_pair.export_key(format='PEM').decode()
            with open("private_key.pem", "w") as f:
                f.write(private_key_pem)
            logging.info("Private key saved to disk.")
        except Exception as e:
            logging.error(f"Failed to save private key: {e}")

    def load_key_pair(self):
        """Load the RSA key pair from disk or generate a new one."""
        if os.path.exists("private_key.pem"):
            try:
                with open("private_key.pem", "r") as f:
                    private_key_pem = f.read()
                self.key_pair = RSA.import_key(private_key_pem)
                logging.info("Private key loaded from disk.")
            except Exception as e:
                logging.error(f"Failed to load private key: {e}")
                # If loading fails, generate a new key pair
                self.key_pair = self.generate_key_pair()
                self.save_key_pair()
        else:
            # If the private key file doesn't exist, generate and save a new one
            self.key_pair = self.generate_key_pair()
            self.save_key_pair()

    async def shutdown(self):
        logging.info("Shutting down Peer...")
        self.shutdown_flag = True

        # Cancel heartbeat tasks
        for server_id, task in self.heartbeat_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                    logging.info(f"Heartbeat task for {server_id} cancelled.")
                except asyncio.CancelledError:
                    logging.info(f"Heartbeat task for {server_id} cancelled via exception.")
                except Exception as e:
                    logging.error(f"Error cancelling heartbeat task for {server_id}: {e}")
        self.heartbeat_tasks.clear()

        # Close all peer connections
        for server_id, (reader, writer) in self.connections.items():
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
                logging.info(f"Connection with server_id {server_id} closed during shutdown.")
        self.connections.clear()

        # Close the P2P server
        await self.close_p2p_server()
        logging.info("Peer shutdown complete.")
