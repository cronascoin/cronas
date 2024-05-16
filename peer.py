# Copyright 2024 cronas.org
# peer.py

import asyncio
import errno
import json
import logging
import os
import socket
import time
import ipaddress
import aiofiles
import requests
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

class Peer:
    def __init__(self, host, p2p_port, server_id, version, max_peers=10):  # Add max_peers as a parameter
        self.server_id = server_id
        self.host = host
        self.p2p_port = p2p_port
        self.peers = {}
        self.active_peers = {}
        self.external_ip = self.detect_ip_address()
        self.out_ip = self.detect_out_ip()
        self.hello_seq = 0
        self.version = version
        self.file_lock = asyncio.Lock()
        self.peers_changed = False
        self.connections = {}
        self.shutdown_flag = False
        self.heartbeat_tasks = {}  # Dictionary to store heartbeat tasks per peer
        self.file_write_scheduled = False
        self.pending_file_write = False  # New attribute to track pending writes
        self.file_write_delay = 10      # Delay in seconds before writing to file
        self.max_peers = max_peers  # Store max_peers as an instance variable
        
    def is_private_ip(self, ip):
        return ipaddress.ip_address(ip).is_private

    async def close_connection(self, host, port):
        if (host, port) in self.connections:
            _, writer = self.connections.pop((host, port))
            if writer and not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.info(f"Successfully closed connection to {host}:{port}")
        else:
            logging.info(f"No active connection found for {host}:{port} to close.")

    async def close_p2p_server(self):
        if self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logging.info("P2P server closed.")

    async def connect_and_maintain(self):
        """Main loop for connecting and maintaining peer connections."""
        while not self.shutdown_flag:
            await self.load_peers()
            await self.connect_to_known_peers()
            await asyncio.sleep(60)  # Wait for 60 seconds before checking again

    async def connect_to_known_peers(self):
        """Asynchronously connects to a random subset of known peers, up to max_peers."""
        available_peers = [peer for peer in self.peers if peer not in self.active_peers]
        peers_to_connect = random.sample(available_peers, min(self.max_peers, len(available_peers)))
        tasks = [asyncio.create_task(self.connect_to_peer(host, int(port)))
        for host, port in (peer.split(':') for peer in peers_to_connect)]
        await asyncio.gather(*tasks, return_exceptions=True)
                
    async def connect_to_new_peers(self, new_peers):
        """Attempts to connect to new peers that are not already connected."""

        for peer_info in new_peers:
            try:
                host, port = peer_info.split(':')
                port = int(port)

                # Check if already connected or attempting to connect to self
                if host in [self.host, self.external_ip, self.out_ip, "127.0.0.1"] or peer_info in self.connections:
                    continue

                # Ensure we don't exceed the maximum number of peers
                if len(self.active_peers) >= self.max_peers:
                    logging.info("Maximum number of peers reached. Skipping connection to new peer.")
                    break

                # Attempt to connect to the peer
                logging.info(f"Attempting to connect to new peer: {peer_info}")
                asyncio.create_task(self.connect_to_peer(host, port))
            except ValueError:
                logging.warning(f"Invalid peer address format: {peer_info}")

    async def connect_to_peer(self, host, port, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                local_addr, local_port = writer.get_extra_info('sockname')  # Get local_port here

                # Construct handshake message
                handshake_msg = {
                    "type": "hello",
                    "payload": f"Hello from {self.host}",
                    "version": self.version,
                    "seq": self.hello_seq + 1,
                    "server_id": self.server_id,
                    "listening_port": self.p2p_port
                }
                # Send the handshake message
                writer.write(json.dumps(handshake_msg).encode() + b'\n')
                await writer.drain()

                # Send the request for peers message
                await self.request_peer_list(writer)

                # Define peer_info after successful connection
                peer_info = f"{host}:{port}"
                self.connections[peer_info] = (reader, writer)
                self.active_peers[peer_info] = {
                    'addr': peer_info,
                    'addrlocal': f"{self.out_ip}:{local_port}",
                    'addrbind': f"{self.external_ip}:{local_port}",
                    'server_id': self.server_id,
                    'version': self.version
                }  
                asyncio.create_task(self.listen_for_messages(reader, writer))
                asyncio.create_task(self.send_heartbeat(writer, peer_info))
                return  # Successful connection, exit loop
            except Exception as e:
                logging.error(f"Error connecting to {host}:{port}: {e}")
                retry_count += 1
                await asyncio.sleep(retry_count * 5)  # Exponential backoff
            logging.warning(f"Failed to connect to {host}:{port} after {max_retries} attempts.")


    def detect_ip_address(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                ip = s.getsockname()[0]
            return ip
        except Exception as e:
            logging.info(f"Failed to detect external IP address: {e}")
            return '127.0.0.1'

    def detect_out_ip(self):
        try:
            response = requests.get('https://api.ipify.org?format=json')
            response.raise_for_status()
            return response.json()['ip']
        except requests.RequestException as e:
            logging.error(f"Failed to detect external IP address: {e}")
            return '127.0.0.1'

    async def handle_disconnection(self, peer_info):
        if peer_info in self.peers:
            self.peers[peer_info] = int(time.time())
            await self.rewrite_peers_file()
        if peer_info in self.active_peers:
            del self.active_peers[peer_info]
            logging.info(f"Removed {peer_info} from active peers due to disconnection or error.")

    async def handle_peer_connection(self, reader, writer):
        peer_address = writer.get_extra_info('peername')

        try:
            buffer = ''
            while True:
                try:
                    data = await reader.read(1024)
                    if not data:
                        break

                    buffer += data.decode()
                    while '\n' in buffer:
                        message, buffer = buffer.split('\n', 1)
                        if message:
                            message_obj = json.loads(message)
                            await self.process_message(message_obj, writer)

                            if message_obj.get('type') == 'hello' and 'listening_port' in message_obj:
                                host, port = peer_address
                                peer_info = f"{host}:{message_obj['listening_port']}"
                                if peer_info not in self.peers:
                                    self.peers[peer_info] = int(time.time())
                                    await self.rewrite_peers_file()

                except ConnectionResetError:
                    logging.warning(f"Connection reset by peer {peer_address}. Attempting to handle gracefully.")
                    break

        except asyncio.CancelledError:
            logging.info(f"Connection task with {peer_address} cancelled")
        except (asyncio.IncompleteReadError, json.JSONDecodeError):
            logging.warning(f"Connection with {peer_address} closed abruptly or received malformed data.")
        except Exception as e:
            logging.exception(f"General error during P2P communication with {peer_address}: {e}")
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.info(f"Connection with {peer_address} closed.")

    async def listen_for_messages(self, reader, writer):
        addr = writer.get_extra_info('peername')
        host, port = addr[0], addr[1]
        peer_info = f"{host}:{port}"
        logging.info(f"Listening for messages from {peer_info}")
        try:
            while True:
                data_buffer = await reader.readuntil(separator=b'\n')
                if not data_buffer:
                    logging.info("Connection closed by peer.")
                    break
                message = json.loads(data_buffer.decode().strip())
                self.peers[peer_info] = int(time.time())
                await self.process_message(message, writer)
                await self.rewrite_peers_file()
        except asyncio.IncompleteReadError:
            logging.info("Incomplete read error, attempting to reconnect...")
            await self.handle_disconnection(peer_info)
            asyncio.create_task(self.reconnect_to_peer(host, int(port)))
        except Exception as e:
            logging.error(f"Error during communication with {peer_info}: {e}")
            await self.handle_disconnection(peer_info)
        finally:
            logging.info(f"Closing connection with {peer_info}")
            await self.handle_disconnection(peer_info)
            writer.close()
            await writer.wait_closed()

    async def load_peers(self):
        loaded_peers_count = 0
        skipped_peers_count = 0

        if os.path.exists("peers.dat"):
            async with aiofiles.open("peers.dat", "r") as f:
                async for line in f:
                    try:
                        parts = line.strip().split(":")
                        if len(parts) != 3:
                            raise ValueError("Line format incorrect, expected 3 parts.")

                        peer, port, last_seen_str = parts
                        peer_info = f"{peer}:{port}"
                        last_seen = int(last_seen_str)

                        self.peers[peer_info] = last_seen
                        loaded_peers_count += 1
                    except ValueError as e:
                        logging.warning(f"Invalid line in peers.dat: {line} - Error: {e}")
                        skipped_peers_count += 1

        await self.rewrite_peers_file()
        logging.info(f"Peers loaded from file. Loaded: {loaded_peers_count}, Skipped: {skipped_peers_count}.")

    def mark_peer_changed(self):
        self.peers_changed = True
        logging.debug("Peers data marked as changed.")

    async def process_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        await self.update_last_seen(peer_info)
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
        elif message_type == "heartbeat_ack":
            logging.info(f"Heartbeat acknowledgment from {peer_info}")
        else:
            logging.info(f"Unhandled message type from {peer_info}: {message_type}")

    def get_local_addr_and_port(self, addr):
        """Determine the local IP address and ephemeral port for a given address."""
        if self.is_private_ip(self.out_ip):
            return self.out_ip, 49152  # Example ephemeral port for private IP
        else:
            return self.external_ip, 49152  # Example ephemeral port for public IP


    async def handle_hello_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        
        if 'listening_port' in message and isinstance(message['listening_port'], int):
            peer_port = message['listening_port']
            peer_info = f"{addr[0]}:{peer_port}"
            if peer_info not in self.peers:
                self.peers[peer_info] = int(time.time())
                logging.info(f"Added new peer {peer_info}.")
                self.mark_peer_changed()
                await self.rewrite_peers_file()
            
            ack_message = {
                "type": "ack",
                "payload": "Handshake acknowledged",
                "version": self.version, 
                "server_id": self.server_id
            }
            writer.write(json.dumps(ack_message).encode() + b'\n')
            await writer.drain()
            logging.info(f"Handshake acknowledged to {peer_info}")
            # Start sending heartbeats
            asyncio.create_task(self.send_heartbeat(writer))
        else:
            logging.error("Invalid or missing listening port in handshake message.")

    async def handle_ack_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        remote_version = message.get("version", "unknown")
        remote_server_id = message.get("server_id", "unknown")

        local_addr, local_port = writer.get_extra_info('sockname')


        addrlocal = f"{self.out_ip}:{local_port}"
        addrbind = f"{self.external_ip}:{local_port}"


        self.active_peers[peer_info] = {
            "addr": peer_info,
            "addrlocal": addrlocal,
            "addrbind": addrbind,
            "server_id": remote_server_id,
            "version": remote_version
        }
        logging.info(f"Connection fully established with {peer_info}, version {remote_version}")
        self.mark_peer_changed()
        await self.rewrite_peers_file()
        asyncio.create_task(self.send_heartbeat(writer))

    async def handle_peer_list_message(self, message):
        if new_peers := message.get("payload", []):
            logging.info("Processing new peer list...")

            # Filter out invalid and already connected peers
            valid_new_peers = [
                peer_info
                for peer_info in new_peers
                if self.is_valid_peer(peer_info) and peer_info not in self.active_peers
            ]

            # Limit the number of new connections to maintain 'max_peers'
            if len(self.active_peers) + len(valid_new_peers) > self.max_peers:
                valid_new_peers = valid_new_peers[:(self.max_peers - len(self.active_peers))]

            # Connect to the filtered new peers
            await self.connect_to_new_peers(valid_new_peers)
        else:
            logging.warning("Received empty peer list.")
            
    def is_valid_peer(self, peer_info):
        try:
            ip, port = peer_info.split(':')
            # Validate IP address
            ipaddress.ip_address(ip)

            # Ensure the IP address is not one of the local/server addresses
            if ip in [self.host, self.external_ip, self.out_ip, "127.0.0.1", "localhost"]:
                logging.info(f"Skipping self-peer with IP: {ip}")
                return False

            port = int(port)  # Validate that port is an integer
            # Ephemeral ports typically range from 32768 to 65535; adjust if your range differs
            return not (32768 <= port <= 65535)
        except ValueError as e:
            logging.error(f"Invalid peer address '{peer_info}': {e}")
            return False


    async def reconnect_to_peer(self, host, port):
        peer_identifier = f"{host}:{port}"
        logging.info(f"Attempting to reconnect to {peer_identifier}")

        retry_intervals = [60, 3600, 86400, 2592000]  # Seconds for minute, hour, day, month
        max_attempts = len(retry_intervals)

        for attempt, base_delay in enumerate(retry_intervals):
            if (host, str(port)) not in self.active_peers:
                try:
                    # Calculate the backoff time with jitter
                    jitter = random.uniform(0.8, 1.2)  # More moderate jitter
                    backoff_time = base_delay * jitter
                    await asyncio.sleep(backoff_time)  

                    # Attempt to reconnect
                    await self.connect_to_peer(host, port)

                    logging.info(f"Reconnected to {peer_identifier} successfully.")
                    
                    # Attempt to reconnect
                    await self.connect_to_peer(host, port)

                    logging.info(f"Reconnected to {peer_identifier} successfully.")

                    # Update peer information (assuming successful reconnect)
                    peer_info = f"{host}:{port}"
                    self.peers[peer_info] = int(time.time())

                    # Add or update active peer information
                    reader, writer = self.connections[peer_info]
                    local_addr, local_port = writer.get_extra_info('sockname')
                    self.active_peers[peer_info] = {
                        'addr': peer_info,
                        'addrlocal': f"{self.out_ip}:{local_port}",
                        'addrbind': f"{self.external_ip}:{local_port}",
                        'server_id': self.server_id,
                        'version': self.version
                    }
                    break  # Exit the loop if successful

                except Exception as e:
                    logging.error(f"Reconnection attempt {attempt} to {peer_identifier} failed: {e}")
            else:
                logging.info(f"Already connected to {peer_identifier}. No need to reconnect.")
                break

        if attempt == max_attempts:  # Check if max attempts were reached
            logging.info(f"Max reconnection attempts reached for {peer_identifier}.")


    async def request_peer_list(self, writer):
        request_message = {
            "type": "request_peer_list",
            "server_id": self.server_id
        }
        writer.write(json.dumps(request_message).encode() + b'\n')
        await writer.drain()
        logging.info("Request for peer list sent.")

    async def respond_to_heartbeat(self, writer, message):
        peer_info = writer.get_extra_info('peername')
        peer_info_str = f"{peer_info[0]}:{peer_info[1]}"
        
        if peer_info_str in self.active_peers:
            self.active_peers[peer_info_str]['last_seen'] = int(time.time())
            logging.info(f"Updated last seen for {peer_info_str}")

        ack_message = {
            "type": "heartbeat_ack",
            "payload": "pong",
            "server_id": self.server_id
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        await writer.drain()
        logging.info(f"Sent heartbeat acknowledgment to {peer_info_str}.")

    async def rewrite_peers_file(self):
        if not self.peers_changed:
            return

        async with self.file_lock:
            try:
                valid_peers = {peer_info: last_seen for peer_info, last_seen in self.peers.items() if self.is_valid_peer(peer_info)}
                async with aiofiles.open("peers.dat", "w") as f:
                    for peer_info, last_seen in valid_peers.items():
                        await f.write(f"{peer_info}:{last_seen}\n")
                self.peers_changed = False
                logging.info("Peers file rewritten successfully.")
            except OSError as e:
                logging.error(f"Failed to open peers.dat: {e}")
            except Exception as e:
                logging.error(f"Failed to rewrite peers.dat: {e}")

    async def schedule_periodic_peer_save(self):
        while True:
            await asyncio.sleep(60)
            await self.rewrite_peers_file()

    async def schedule_rewrite(self):
        """Schedules a rewrite of the peers.dat file if not already scheduled."""
        if not self.file_write_scheduled:
            self.file_write_scheduled = True
            asyncio.create_task(self._rewrite_after_delay())  # Create task for delayed rewrite

    async def send_heartbeat(self, writer, peer_info=None):  # Make peer_info optional
        await asyncio.sleep(60)
        try:
            while not writer.is_closing():
                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id,
                    "version": self.version
                }
                writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
                await writer.drain()
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            logging.info("Heartbeat sending cancelled.")
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")
            # No need to call self.restart_heartbeat(writer) anymore


    async def send_peer_list(self, writer):
        logging.info("Attempting to send peer list...")
        peer_list = []

        try:
            async with aiofiles.open("peers.dat", "r") as f:
                async for line in f:
                    if line.strip():
                        parts = line.strip().split(':')
                        if len(parts) >= 2:
                            ip_port = f"{parts[0]}:{parts[1]}"
                            peer_list.append(ip_port)
        except Exception as e:
            logging.error(f"Failed to read peers.dat: {e}")
            return

        if peer_list:
            peer_list_message = {
                "type": "peer_list",
                "payload": peer_list,
                "server_id": self.server_id,
                "version": self.version
            }

            writer.write(json.dumps(peer_list_message).encode() + b'\n')
            await writer.drain()
            logging.info("Sent sanitized peer list from peers.dat to a peer.")
        else:
            logging.warning("No peers found to send.")

    async def start(self):
        """Start the peer server and main loop."""
        try:
            asyncio.create_task(self.connect_and_maintain())  # Start main loop in the background
            await self.start_p2p_server()
        finally:
            self.shutdown_flag = True
            # Wait for connections to close gracefully
            await asyncio.gather(*[self.close_connection(*peer) for peer in self.connections])
            await self.close_p2p_server()            
    async def start_p2p_server(self):
        try:
            server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port,
                reuse_address=True
            )
            self.p2p_server = server
            logging.info(f"P2P server version {self.version} with ID {self.server_id} listening on {self.host}:{self.p2p_port}")
            await self.load_peers()
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

    async def update_last_seen(self, peer_info):
        self.peers[peer_info] = int(time.time())
        await self.schedule_rewrite() 

    async def _rewrite_after_delay(self):
        """Helper function to rewrite the file after a delay."""
        await asyncio.sleep(self.file_write_delay)
        if self.peers_changed:  # Only rewrite if changes occurred
            async with self.file_lock:
                try:
                    valid_peers = {peer_info: last_seen 
                                    for peer_info, last_seen in self.peers.items() 
                                    if self.is_valid_peer(peer_info)}
                    async with aiofiles.open("peers.dat", "w") as f:
                        for peer_info, last_seen in valid_peers.items():
                            await f.write(f"{peer_info}:{last_seen}\n")
                    self.peers_changed = False
                    logging.info("Peers file rewritten successfully.")
                except OSError as e:
                    logging.error(f"Failed to open peers.dat: {e}")
                except Exception as e:
                    logging.error(f"Failed to rewrite peers.dat: {e}")
            self.file_write_scheduled = False  # Reset the flag after rewrite
