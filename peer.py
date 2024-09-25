# Copyright 2024 Cronas.org
# peer.py

import asyncio
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



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

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
        self.active_peers = {}
        self.trust_store = {}
        self.certificates = {}
        self.blacklist = set()
        self.key_pair = self.generate_key_pair()
        self.certificate = self.generate_self_signed_certificate()
        self.external_ip, self.external_p2p_port = get_external_ip()
        self.hello_seq = 0
        self.version = version
        self.file_lock = asyncio.Lock()  # File lock for peer file operations
        self.peers_changed = False
        self.connections = {}  # Initialize connections dictionary
        self.shutdown_flag = False
        self.heartbeat_tasks = {}
        self.file_write_scheduled = False
        self.pending_file_write = False
        self.file_write_delay = 10
        self.max_peers = max_peers
        self.rewrite_lock = asyncio.Lock()
        self.connection_attempts = {}
        self.peers_connecting = set()
        self.config = config or {}
        self.debug = self.config.get('debug', 'false').lower() == 'true'  # Ensure debug is initialized
        self.p2p_server = None  # Initialize p2p_server to None
        self.last_peer_list_request = {}
        self.start_time = datetime.datetime.now()
        self.ntp_offset = self.get_ntp_offset()

    # Example of starting the P2P server
    async def start_p2p_server(self):
        try:
            server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port, reuse_address=True
            )
            self.p2p_server = server  # Ensure server is assigned to p2p_server
            logging.info(f"P2P server version {self.version} with ID {self.server_id} listening on {self.host}:{self.p2p_port}")
            await self.connect_to_known_peers()
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

    # Example of closing the P2P server
    async def close_p2p_server(self):
        if self.p2p_server:
            self.p2p_server.close()  # Properly close the server
            await self.p2p_server.wait_closed()  # Wait for the server to fully close
            logging.info("P2P server closed.")
        else:
            logging.error("P2P server was not initialized.")


    async def connect_to_known_peers(self):
        """Attempt to connect to known peers, skipping blacklisted peers."""
        available_peers = [
            peer for peer in self.peers if peer not in self.active_peers
            and peer not in self.peers_connecting and peer not in self.blacklist
        ]
        
        if not available_peers:
            logging.info("No available peers to connect.")
            return

        peers_to_connect = random.sample(available_peers, min(self.max_peers, len(available_peers)))
        
        # Remove peers from peers_connecting after the attempt
        self.peers_connecting.update(peers_to_connect)

        tasks = [asyncio.create_task(self.connect_to_peer(host, int(port)))
                for host, port in (peer.split(':') for peer in peers_to_connect)]
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.peers_connecting.difference_update(peers_to_connect)

        # Update active peers after connection attempts
        self.update_active_peers()


    async def connect_to_peer(self, host, port, max_retries=5):
        """Connect to a peer, skipping if they are blacklisted."""
        peer_info = f"{host}:{port}"

        if peer_info in self.blacklist:
            logging.warning(f"Peer {peer_info} is blacklisted. Aborting connection.")
            return  # Do not attempt to connect if peer is blacklisted

        if peer_info in self.connection_attempts or peer_info in self.peers_connecting:
            logging.info(f"Skipping already connecting peer: {peer_info}")
            return

        self.connection_attempts[peer_info] = 0

        while self.connection_attempts[peer_info] < max_retries:
            if self.shutdown_flag:
                logging.info(f"Shutdown in progress, cancelling connection attempts to {peer_info}.")
                return

            if len(self.active_peers) >= self.max_peers:
                logging.info(f"Maximum number of peers ({self.max_peers}) already connected. Skipping connection to {peer_info}.")
                return

            try:
                reader, writer = await asyncio.open_connection(host, port)
                if await self.establish_peer_connection(reader, writer, host, port, peer_info):
                    return  # Connection successful
            except Exception as e:
                logging.error(f"Error connecting to {host}:{port}: {e}")
                self.connection_attempts[peer_info] += 1
                await asyncio.sleep(self.connection_attempts[peer_info] * 5)

        # Blacklist peer after failing max retries
        logging.warning(f"Failed to connect to {host}:{port} after {max_retries} attempts. Blacklisting peer.")
        self.blacklist.add(peer_info)
        self.connection_attempts.pop(peer_info, None)


    async def handle_hello_message(self, message, writer):
        """Handle hello message, including verifying the peer's certificate."""
        peer_certificate = message['certificate']
        peer_server_id = message['server_id']
        peer_info = f"{writer.get_extra_info('peername')[0]}:{writer.get_extra_info('peername')[1]}"

        # Verify if the certificate is trusted (signed by known peers)
        if peer_server_id in self.trust_store:
            peer_trusted = self.trust_store[peer_server_id]['trusted']
        else:
            peer_trusted = self.verify_trust(peer_certificate)

        if peer_trusted:
            self.trust_store[peer_server_id] = {'certificate': peer_certificate, 'trusted': True}
            logging.info(f"Trusted peer {peer_server_id} connection established.")
        else:
            if peer_info not in self.blacklist:
                logging.warning(f"Untrusted peer {peer_server_id} attempting connection. Blacklisting.")
                self.blacklist.add(peer_info)  # Blacklist the untrusted peer
            writer.close()
            await writer.wait_closed()
            return  # Terminate the connection

        # Continue with the rest of the handshake process if trusted
        await self.send_ack_message(writer, self.version, self.server_id)


    async def establish_peer_connection(self, reader, writer, host, port, peer_info):
        """Establish the connection and verify trust using certificates."""
        if peer_info in self.blacklist:
            logging.warning(f"Peer {peer_info} is blacklisted. Aborting connection.")
            writer.close()
            await writer.wait_closed()
            return False

        try:
            # Get the local and bound ports for the connection
            local_addr, local_port = writer.get_extra_info('sockname')  # Local (outgoing) address and port
            bound_addr, bound_port = writer.get_extra_info('peername')  # Bound (peer) address and port

            send_time = await self.send_hello_message(writer)
            ack_message = await self.receive_message(reader)

            # If the handshake is successful (received ack), process the connection
            if ack_message.get("type") == "ack":
                server_id = ack_message.get("server_id")

                # Check if the peer is trusted before proceeding
                if server_id not in self.trust_store or not self.trust_store[server_id]['trusted']:
                    logging.warning(f"Untrusted peer {server_id}. Closing connection.")
                    writer.close()
                    await writer.wait_closed()
                    self.blacklist.add(peer_info)  # Blacklist the untrusted peer
                    return False

                # Calculate ping time
                receive_time = time.time()
                ping = receive_time - send_time

                # Register the peer connection with all the required arguments
                await self.register_peer_connection(
                    reader, writer, peer_info, server_id, local_addr, local_port, bound_port, ack_message, ping
                )

                # Start listening for messages and sending heartbeat
                asyncio.create_task(self.listen_for_messages(reader, writer))
                asyncio.create_task(self.send_heartbeat(writer, server_id))
                return True
            else:
                raise ValueError("Failed handshake: did not receive acknowledgment.")
        except Exception as e:
            logging.error(f"Error establishing connection to {peer_info}: {e}")
            return False

        

    # RSA key generation for Web of Trust
    def generate_key_pair(self):
        """Generates an RSA key pair for signing and encryption."""
        return RSA.generate(2048)

    def generate_self_signed_certificate(self):
        """Generates a self-signed certificate."""
        return {
            'server_id': self.server_id,
            'public_key': self.key_pair.publickey().export_key().decode(),
            'signature': None,  # No signature, self-signed
        }

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

    async def send_hello_message(self, writer):
        """Send hello message along with the self-signed certificate."""
        hello_message = {
            'type': 'hello',
            'server_id': self.server_id,
            'public_key': self.key_pair.publickey().export_key().decode(),  # Send public key
            'certificate': self.certificate,
            'timestamp': time.time() + self.ntp_offset
        }
        send_time = time.time()
        await self.send_message(writer, hello_message)
        return send_time

    def verify_trust(self, peer_certificate):
        """Verifies the peer's certificate based on known trusted peers."""
        #public_key = peer_certificate['public_key']
        for trusted_peer_id, trusted_peer in self.trust_store.items():
            if 'signature' in trusted_peer['certificate']:
                signature = trusted_peer['certificate']['signature']
                if self.verify_certificate(peer_certificate, signature, trusted_peer['certificate']['public_key']):
                    return True
        return False


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

    async def close_connection(self, host, port):
        if (host, port) in self.connections:
            _, writer = self.connections.pop((host, port))
            if writer and not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.info(f"Successfully closed connection to {host}:{port}")

    async def connect_and_maintain(self):
        while not self.shutdown_flag:
            await self.connect_to_known_peers()
            await asyncio.sleep(60)

    async def connect_to_new_peers(self, new_peers):
        for peer_info in new_peers:
            try:
                host, port = peer_info.split(':')
                port = int(port)

                if not self.is_valid_peer(peer_info) or peer_info in self.connections or peer_info in self.connection_attempts:
                    continue

                while len(self.connection_attempts) >= self.max_peers:
                    await asyncio.sleep(1)

                self.connection_attempts[peer_info] = 0
                asyncio.create_task(self.connect_to_peer(host, port, 5))
            except ValueError:
                logging.warning(f"Invalid peer address format: {peer_info}")


    async def register_peer_connection(self, reader, writer, peer_info, server_id, local_addr, local_port, bound_port, ack_message, ping):
        """Register a successfully established peer connection."""
        self.connections[peer_info] = (reader, writer)
        self.active_peers[server_id] = {
            'addr': peer_info,
            'addrlocal': f"{local_addr}:{local_port}",  # Local IP and outgoing (ephemeral) port
            'addrbind': f"{self.external_ip}:{bound_port}",  # Bound IP and bound port for incoming connections
            'server_id': server_id,
            'version': ack_message.get("version", "unknown"),
            'lastseen': int(time.time()),
            'ping': round(ping, 3),
        }
        self.peers_changed = True
        await self.schedule_rewrite()



    async def receive_message(self, reader):
        data = await reader.readuntil(separator=b'\n')
        return json.loads(data.decode())

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

    async def handle_ack_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        remote_version = message.get("version", "unknown")
        remote_server_id = message.get("server_id", "unknown")
        local_addr, local_port = writer.get_extra_info('sockname')

        addrlocal = f"{self.external_ip}:{local_port}"
        addrbind = f"{self.external_ip}:{local_port}"

        if remote_server_id in self.active_peers:
            logging.info(f"Duplicate connection detected for server_id {remote_server_id}. Closing new connection.")
            writer.close()
            await writer.wait_closed()
            return

        receive_time = time.time()
        send_time = self.active_peers.get(remote_server_id, {}).get('send_time', receive_time)
        ping = receive_time - send_time

        self.active_peers[remote_server_id] = {
            "addr": peer_info,
            "addrlocal": addrlocal,
            "addrbind": addrbind,
            "server_id": remote_server_id,
            "version": remote_version,
            "lastseen": int(time.time()),
            "ping": round(ping, 3)
        }

        logging.info(f"Connection fully established with {peer_info}, version {remote_version}, server_id {remote_server_id}")
        self.update_active_peers()
        self.mark_peer_changed()
        await self.schedule_rewrite()
        asyncio.create_task(self.send_heartbeat(writer))


    async def handle_disconnection(self, peer_info):
        if self.shutdown_flag:
            return

        for server_id, peer_data in self.active_peers.items():
            if peer_data["addr"] == peer_info:
                del self.active_peers[server_id]
                logging.info(f"Removed {peer_info} from active peers due to disconnection or error.")
                break

        self.update_active_peers()
        self.peers_changed = True
        await self.schedule_rewrite()

    async def handle_peer_connection(self, reader, writer):
        peer_address = writer.get_extra_info('peername')
        peer_info = f"{peer_address[0]}:{peer_address[1]}"

        if peer_info in self.connections:
            logging.info(f"Duplicate connection attempt to {peer_info}. Closing new connection.")
            writer.close()
            await writer.wait_closed()
            return

        self.connections[peer_info] = (reader, writer)

        try:
            buffer = b''
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                buffer += data
                while b'\n' in buffer:
                    message, buffer = buffer.split(b'\n', 1)
                    try:
                        if message:
                            message_obj = json.loads(message.decode('utf-8').strip())
                            await self.process_message(message_obj, writer)
                    except UnicodeDecodeError as e:
                        logging.error(f"Failed to decode message from {peer_info}: {e}")
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse JSON message from {peer_info}: {e}")

        except asyncio.CancelledError:
            logging.info(f"Connection task with {peer_address} cancelled")
        except ConnectionResetError:
            logging.warning(f"Connection reset by peer {peer_address}.")
        except (asyncio.IncompleteReadError, json.JSONDecodeError):
            logging.warning(f"Connection with {peer_address} closed abruptly or received malformed data.")
        except Exception as e:
            logging.exception(f"General error during P2P communication with {peer_address}: {e}")
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.info(f"Connection with {peer_address} closed.")
            await self.handle_disconnection(peer_info)


    async def handle_peer_list_message(self, message):
        new_peers = message.get("payload", [])
        logging.info("Processing new peer list...")

        valid_new_peers = {
            peer_info: self.peers.get(peer_info, 0) for peer_info in new_peers
            if self.is_valid_peer(peer_info)
        }

        invalid_peers = [
            peer_info for peer_info in new_peers
            if not self.is_valid_peer(peer_info)
        ]

        for invalid_peer in invalid_peers:
            if self.debug:
                logging.warning(f"Invalid peer received: {invalid_peer}")

        self.peers.update(valid_new_peers)
        if valid_new_peers:
            self.peers_changed = True
            await self.schedule_rewrite()

        for peer_info in valid_new_peers:
            try:
                host, port = peer_info.split(':')
                port = int(port)

                if peer_info in self.connections:
                    if self.debug:
                        logging.info(f"Already connected to {peer_info}, skipping additional connection attempt.")
                    continue

                while len(self.connection_attempts) >= self.max_peers:
                    await asyncio.sleep(1)

                self.connection_attempts[peer_info] = 0
                asyncio.create_task(self.connect_to_peer(host, port, 5))
            except ValueError:
                logging.warning(f"Invalid peer address format: {peer_info}")
        self.update_active_peers()

    def is_valid_peer(self, peer_info):
        try:
            ip, port = peer_info.split(':')
            ipaddress.ip_address(ip)

            if ip in [self.host, self.external_ip, "127.0.0.1", "localhost"]:
                logging.info(f"Skipping self-peer with IP: {ip}")
                return False

            port = int(port)
            return not (32768 <= port <= 65535)
        except ValueError as e:
            logging.error(f"Invalid peer address '{peer_info}': {e}")
            return False

    async def listen_for_messages(self, reader, writer):
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
                await self.process_message(message, writer)
        except asyncio.IncompleteReadError:
            logging.info("Incomplete read error, attempting to reconnect...")
            await self.handle_disconnection(peer_info)
            asyncio.create_task(self.reconnect_to_peer(host, int(port)))
        except ConnectionResetError as e:
            logging.error(f"Connection reset error with {peer_info}: {e}")
            await self.handle_disconnection(peer_info)
            asyncio.create_task(self.reconnect_to_peer(host, int(port)))
        except Exception as e:
            logging.error(f"Error during communication with {peer_info}: {e}")
            await self.handle_disconnection(peer_info)
        finally:
            logging.info(f"Closing connection with {peer_info}")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

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


    def mark_peer_changed(self):
        self.peers_changed = True
        logging.debug("Peers data marked as changed.")

    async def process_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        if self.debug:
            logging.info(f"Received message from {peer_info}: {message}")

        message_type = message.get("type")
        if message_type == "hello":
            await self.handle_hello_message(message, writer)
        elif message_type == "ack":
            await self.handle_ack_message(message, writer)
        elif message_type == "request_peer_list":
            await self.send_peer_list(writer)
        elif message_type == "heartbeat":
            await self.respond_to_heartbeat(writer, message)
        elif message_type == "peer_list":
            await self.handle_peer_list_message(message)

    async def reconnect_to_peer(self, host, port):
        peer_info = f"{host}:{port}"
        await self._attempt_reconnect(peer_info)

    async def schedule_reconnect(self, peer_info):
        await self._attempt_reconnect(peer_info)

    async def _attempt_reconnect(self, peer_info):
        logging.info(f"Attempting to reconnect to {peer_info}")

        retry_intervals = [60, 3600, 86400, 2592000]
        attempt = 0

        while attempt < len(retry_intervals):
            if self.shutdown_flag:
                logging.info(f"Shutdown in progress, cancelling reconnection attempts for {peer_info}.")
                return

            if peer_info not in self.active_peers and peer_info not in self.peers_connecting:
                try:
                    jitter = random.uniform(0.8, 1.2)
                    backoff_time = retry_intervals[attempt] * jitter
                    logging.info(f"Scheduling reconnection attempt for {peer_info} in {backoff_time:.2f} seconds.")
                    await asyncio.sleep(backoff_time)

                    host, port = peer_info.split(':')
                    await self.connect_to_peer(host, int(port))

                    if peer_info in self.active_peers:
                        logging.info(f"Reconnected to {peer_info} successfully.")
                        break
                except Exception as e:
                    logging.error(f"Reconnection attempt {attempt} to {peer_info} failed: {e}")
                    attempt += 1
            else:
                logging.info(f"{peer_info} is already active or attempting to reconnect.")
                break

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
        writer.write(json.dumps(request_message).encode() + b'\n')
        await writer.drain()
        logging.info(f"Request for peer list sent to {peer_info}.")

    async def respond_to_heartbeat(self, writer, message):
        peer_info = writer.get_extra_info('peername')
        peer_info_str = f"{peer_info[0]}:{peer_info[1]}"

        if peer_info_str in self.active_peers:
            self.active_peers[peer_info_str]['lastseen'] = int(time.time())
            self.peers_changed = True
            if self.debug:
                logging.info(f"Updated last seen for {peer_info_str}")

        ack_message = {
            "type": "heartbeat_ack",
            "payload": "pong",
            "server_id": self.server_id,
            "timestamp": time.time() + self.ntp_offset
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        await writer.drain()
        if self.debug:
            logging.info(f"Sent heartbeat acknowledgment to {peer_info_str}.")

    async def send_ack_message(self, writer, version, server_id):
        ack_message = {
            "type": "ack",
            "version": version,
            "server_id": server_id,
            "timestamp": time.time() + self.ntp_offset
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        await writer.drain()
        logging.info(f"Sent ack message to peer with server_id: {server_id}")

    async def schedule_periodic_peer_save(self):
        while True:
            await asyncio.sleep(60)
            await self.rewrite_peers_file()

    async def schedule_rewrite(self):
        if not self.file_write_scheduled:
            self.file_write_scheduled = True
            asyncio.create_task(self._rewrite_after_delay())

    async def send_heartbeat(self, writer, server_id=None):
        retry_count = 0
        max_retries = 3  # Maximum number of retry attempts
        retry_delay = 5  # Delay in seconds between retry attempts

        try:
            await asyncio.sleep(60)  # Initial delay before sending heartbeat
            while not writer.is_closing():
                if self.shutdown_flag:
                    logging.info("Shutdown in progress, stopping heartbeat.")
                    return

                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id,
                    "version": self.version,
                    "timestamp": time.time() + self.ntp_offset
                }

                try:
                    writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
                    await writer.drain()
                    logging.info(f"Heartbeat sent to {server_id}")
                    
                    # Reset retry count after a successful send
                    retry_count = 0

                    # Sleep for 60 seconds before sending the next heartbeat
                    await asyncio.sleep(60)

                except (ConnectionResetError, asyncio.CancelledError) as e:
                    logging.warning(f"Error sending heartbeat to {server_id}: {e}. Retrying...")

                    retry_count += 1
                    if retry_count >= max_retries:
                        logging.error(f"Max retries reached. Closing connection to {server_id}.")
                        writer.close()
                        await writer.wait_closed()
                        return  # Stop sending heartbeats and close connection

                    # Wait before retrying
                    await asyncio.sleep(retry_delay)

                except Exception as e:
                    logging.error(f"Unexpected error during heartbeat: {e}")
                    return

        except asyncio.CancelledError:
            logging.info("Heartbeat sending task was cancelled.")
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")


    async def send_message(self, writer, message):
        message_data = json.dumps(message).encode('utf-8')
        writer.write(message_data + b'\n')
        await writer.drain()

    async def send_peer_list(self, writer):
        logging.info("Attempting to send peer list...")

        if connecting_ports := [
            peer_data["addr"] for peer_data in self.active_peers.values() if self.is_valid_peer(peer_data["addr"])
        ]:
            peer_list_message = {
                "type": "peer_list",
                "payload": connecting_ports,
                "server_id": self.server_id,
                "version": self.version,
                "timestamp": time.time() + self.ntp_offset
            }

            writer.write(json.dumps(peer_list_message).encode() + b'\n')
            await writer.drain()
            logging.info("Sent active peer list.")
        else:
            logging.warning("No valid active peers to send.")

    async def start(self):
        try:
            if not os.path.exists("peers.dat"):
                self.addnode = self.config.get('addnode', [])
                for peer_info in self.addnode:
                    if self.is_valid_peer(peer_info):
                        self.peers[peer_info] = int(time.time())

            await self.load_peers()
            await self.start_p2p_server()
            asyncio.create_task(self.connect_and_maintain())
        finally:
            self.shutdown_flag = True
            await self.cleanup_and_rewrite_peers()
            await self.close_p2p_server()

    async def update_last_seen(self, server_id):
        async with self.rewrite_lock:
            if server_id in self.active_peers:
                self.active_peers[server_id]["lastseen"] = int(time.time())
                self.peers_changed = True
                await self.schedule_rewrite()

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

    async def cleanup_and_rewrite_peers(self):
        self.peers = {peer_info: last_seen for peer_info, last_seen in self.peers.items() if last_seen != 0}
        self.peers_changed = True
        await self.rewrite_peers_file()

        await asyncio.gather(*[self.close_connection(*peer.split(':')) for peer in self.connections.keys()])

    def update_active_peers(self):
        self.active_peers = dict(sorted(
            self.active_peers.items(),
            key=lambda x: float(x[1]['ping']) if x[1]['ping'] is not None else float('inf')
        )[:self.max_peers])