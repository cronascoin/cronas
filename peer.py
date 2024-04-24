#Copyright 2024 cronas.org
#peer.py

import asyncio
import contextlib
import json
import logging
import os
import socket
import uuid
import random
import aiofiles
import traceback
import time
import re

logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.p2p_port = p2p_port
        self.peers = {}  # Initialize as a dictionary
        self.active_peers = set()  # Initialize active_peers
        self.seeds = seeds
        self.external_ip = self.detect_ip_address()
        self.hello_seq = 0  # Initialize the hello_seq attribute here
        self.reconnect_delay = 5  # seconds
        self.retry_attempts = {}  # Map: peer_identifier -> (last_attempt_time, attempt_count)
        self.cooldown_period = 60  # Cooldown period in seconds before retrying a connection
        self.heartbeat_tasks = []  # Add this line to track heartbeat tasks
        self.known_ips = set()
        self.connected_peers = set()
        self.new_peers = []
        

    async def async_init(self):
        await self.load_peers()

    def calculate_backoff(self, attempt):
        """Calculates the backoff time with jitter."""
        return min(2 ** attempt + random.uniform(0, 1), 60)


    async def cancel_heartbeat_tasks(self):
        """Cancels all heartbeat tasks."""
        for task in self.heartbeat_tasks:
            task.cancel()  # Request cancellation of the task
            with contextlib.suppress(asyncio.CancelledError):
                await task  # Wait for the task to be cancelled
        self.heartbeat_tasks.clear()  # Clear the list of tasks after cancellation
        logging.info("All heartbeat tasks cancelled.")


    async def close_p2p_server(self):
        if self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logging.info("P2P server closed.")


    async def connect_to_known_peers(self):
        """Asynchronously attempts to connect to all known peers, avoiding self-connection."""
        logging.info(f"Attempting to connect to known peers: {list(self.peers.keys())}")
        tasks = []
        for peer_address in self.peers.keys():
            host, port = peer_address.split(':')  # Assuming peer_address is in the form 'host:port'
            port = int(port)  # Convert port to an integer

            # Check if the peer is the local machine by comparing host and port
            if host in [self.host, "127.0.0.1"] and port == self.p2p_port:
                logging.info(f"Skipping connection to self at {host}:{port}")
                continue

            # Proceed with the connection attempt
            task = asyncio.create_task(self.connect_to_peer(host, port))
            tasks.append(task)
            logging.info(f"Initiated connection to {host}:{port}")

        # Optionally wait for all tasks to complete, handling exceptions
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Error connecting to a peer: {result}")
            else:
                logging.info("Connected successfully to a peer.")

        logging.info("Completed attempts to connect to all known peers.")



    async def connect_to_new_peers(self, new_peers):
        """Attempt to connect to new peers not currently being connected to."""
        for peer_info in new_peers:
            host, port = peer_info.split(':')  # Splitting into host and port
            port = int(port)  # Ensuring port is an integer

            # Check if the peer is the node itself or if it's already in the list of active or connecting peers
            if (host == self.external_ip and port == self.p2p_port) or (peer_info in self.peers):
                logging.info(f"Skipping connection to {host}:{port} as it is self or already connected.")
                continue

            # Now attempt to connect to the peer
            logging.info(f"Attempting to connect to new peer: {host}:{port}")
            if peer_info not in self.peers:
                self.peers[peer_info] = None  # Optionally consider storing more useful information here
            asyncio.create_task(self.connect_to_peer(host, port))



    async def connect_to_peer(self, host, port):
        peer_info = f"{host}:{port}"
        if host in [self.host, self.external_ip, "127.0.0.1"]:
            return

        logging.info(f"Attempting to connect to {peer_info}")
        writer = None
        attempt = 0
        while attempt < 5:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                handshake_msg = {
                    "type": "hello",
                    "payload": f"Hello from {self.host}",
                    "seq": self.hello_seq + 1,
                    "server_id": "your-server-id",  # Replace with actual server ID
                    "listening_port": self.p2p_port
                }
                writer.write(json.dumps(handshake_msg).encode() + b'\n')
                await writer.drain()

                data = await reader.readline()
                if not data:
                    logging.error("No data received in response to handshake.")
                    break
                ack_message = json.loads(data.decode())
                if ack_message.get("type") == "ack":
                    self.peers[peer_info] = int(time.time())  # Update or add new peer
                    logging.info(f"Connected and acknowledged by peer: {peer_info}")
                    self.active_peers.add((host, port))

                    asyncio.create_task(self.send_heartbeat(writer))
                    request_msg = {"type": "request_peer_list", "server_id": "your-server-id"}
                    writer.write(json.dumps(request_msg).encode() + b'\n')
                    await writer.drain()

                    await self.listen_for_messages(reader, writer)

                    self.hello_seq += 1
                    logging.info(f"Successfully connected to {host}:{port}")
                    break

            except asyncio.TimeoutError as e:
                logging.error(f"Timeout error while connecting to {peer_info}: {e}")
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error while processing handshake from {peer_info}: {e}")
            except Exception as e:
                logging.error(f"General error while connecting to {peer_info}: {e}")
            finally:
                if writer and not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()

                attempt += 1
                if attempt < 5:
                    await asyncio.sleep(self.calculate_backoff(attempt))

        if attempt == 5:
            logging.info(f"Max connection attempts reached for {peer_info}.")


    def detect_ip_address(self):
        """
        Attempt to find the best IP address representing this machine on the Internet.
        This does not actually make a connection to the external server.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                # Use Google's Public DNS server to find the best external IP address
                s.connect(('8.8.8.8', 80))
                # Get the socket's own address
                ip = s.getsockname()[0]
            return ip
        except Exception as e:
            logging.info(f"Failed to detect external IP address: {e}")
            return '127.0.0.1'
        
        
    async def add_new_peers(self, new_peers):
        updated = False
        for ip in new_peers:
            if ip not in self.peers and ip != self.external_ip:
                self.peers[ip] = None  # No last seen time yet
                logging.info(f"New peer {ip} added to the list.")
                updated = True
        if updated:
            await self.rewrite_peers_file()


    async def handle_client(self, writer, peer):
        await peer.listen_for_messages(self, writer)
        writer.close()
        await writer.wait_closed()


    async def handle_peer_connection(self, reader, writer):
        peer_address = writer.get_extra_info('peername')

        try:
            buffer = ''
            while True:
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

        except asyncio.CancelledError:
            logging.info(f"Connection task with {peer_address} cancelled")
        except (asyncio.IncompleteReadError, json.JSONDecodeError):
            pass  # Most likely a connection closed before finishing a message
        except Exception:
            logging.exception(f"General error during P2P communication with {peer_address}")
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.info(f"Connection with {peer_address} closed")


    async def handle_peer_list(self, peer_data):
        """Processes received peer list, updating internal data structures."""
        logging.info("Processing new peer list...")

        for peer_info in peer_data:
            if ':' not in peer_info:
                logging.error(f"Invalid peer info format (missing port): {peer_info}")
                continue

            ip, port = peer_info.split(':', 1)  # Only split on the first colon
            if not ip or not port.isdigit():
                logging.error(f"Invalid peer info format: {peer_info}")
                continue

            peer_address = f"{ip}:{port}"
            if peer_address not in self.peers:
                logging.info(f"Adding new peer: {peer_address}")
            else:
                logging.info(f"Updating last seen time for known peer: {peer_address}")
            self.peers[peer_address] = int(time.time())
        # After processing the peer list, rewrite the peers file with updated information
        try:
            await self.rewrite_peers_file()
            logging.info("Peer list processed and peers file updated.")
        except Exception as e:
            logging.error(f"Failed to rewrite peers file: {e}")



    async def listen_for_messages(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Listening for messages from {addr}")
        try:
            while True:
                data_buffer = await reader.readuntil(separator=b'\n')
                if not data_buffer:
                    logging.info("Connection closed by peer.")
                    break
                message = json.loads(data_buffer.decode().strip())
                await self.process_message(message, writer)
        except asyncio.IncompleteReadError:
            logging.info("Incomplete read error, reconnecting...")
            asyncio.create_task(self.reconnect_to_peer(writer.get_extra_info('peername')))
        except Exception as e:
            logging.error(f"Error during communication with {addr}: {e}")
        finally:
            logging.info(f"Closing connection with {addr}")
            writer.close()
            await writer.wait_closed()


    async def load_peers(self):
        if os.path.exists("peers.dat"):
            async with aiofiles.open("peers.dat", "r") as f:
                async for line in f:
                    try:
                        peer, port, last_seen_str = line.strip().split(":")  # Split into IP:port and timestamp
                        peer_info = str(f"{peer}:{port}")
                        last_seen = int(last_seen_str) if last_seen_str != 'None' else None
                        self.peers[peer_info] = last_seen
                    except ValueError as e:
                        logging.warning(f"Invalid line in peers.dat: {line} - Error: {e}")
        else:
            # Initialize peers from seeds and save to file
            for seed in self.seeds:
                self.peers[seed] = int(time.time()) 
            await self.rewrite_peers_file()  # Save seeds to peers.dat
        logging.info("Peers loaded from file or initialized from seeds.")


    def parse_address(self):
        if match := re.match(r"^(?:(\[[\d:a-fA-F]+\])|([\d\.]+)):(\d+)$", self):
            ip = match[1] or match[2]
            port = int(match[3])
            return ip.strip('[]'), port
        else:
            raise ValueError("Invalid address format")
        

    async def process_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Received message from {addr}: {message}")

        if message.get("type") == "hello":
            if 'listening_port' in message and isinstance(message['listening_port'], int):
                peer_port = message['listening_port']
                peer_info = f"{addr[0]}:{peer_port}"  # addr[0] is the peer's IP
                if peer_info not in self.peers:
                    self.peers[peer_info] = int(time.time())  # Record the time of addition/update
                    logging.info(f"Added new peer {peer_info}.")
                    await self.rewrite_peers_file()
                # Acknowledge the handshake
                ack_message = {
                    "type": "ack",
                    "payload": "Handshake acknowledged",
                    "server_id": self.server_id
                }
                writer.write(json.dumps(ack_message).encode() + b'\n')
                await writer.drain()
                logging.info(f"Handshake acknowledged to {addr}")
            else:
                logging.error("Invalid or missing listening port in handshake message.")

        elif message.get("type") == "request_peer_list":
            logging.info(f"Peer list requested by {addr}")
            # Send the peer list
            await self.send_peer_list(writer)

        elif message.get("type") == "heartbeat":
            await self.respond_to_heartbeat(writer, message)

        elif message.get("type") == "peer_list":
            logging.info(f"Received peer list from {addr}")
            if new_peers := message.get("payload", []):
                logging.info("Processing and updating with new peer list...")
                await self.update_peers(new_peers)
            else:
                logging.warning("Received empty peer list.")

        elif message.get("type") == "heartbeat_ack":
            logging.info(f"Heartbeat acknowledgment from {addr}")

        else:
            logging.info(f"Unhandled message type from {addr}: {message['type']}")


    async def reconnect_to_peer(self, host, port):
        peer_identifier = f"{host}:{port}"
        logging.info(f"Attempting to reconnect to {peer_identifier}")

        attempt = 0
        max_attempts = 5  # Define a maximum number of reconnection attempts

        while attempt < max_attempts:
            if (host, port) not in self.peers:  # Assuming active_peers stores (host, port) tuples
                try:
                    await self.connect_to_peer(host, port)
                    logging.info(f"Reconnected to {host} successfully.")
                    peer_info = f"{host}:{port}"  # Or (host, port) if you use tuples as keys
                    self.peers[peer_info] = int(time.time())  # Update last_seen
                    break
                except Exception as e:
                    logging.error(f"Reconnection attempt to {host} failed: {e}")
            else:
                logging.info(f"Already connected to {peer_identifier}. No need to reconnect.")
                break

            delay = self.calculate_backoff(attempt)
            await asyncio.sleep(delay)
            logging.info(f"Waiting for {delay} seconds before next reconnection attempt to {peer_identifier}.")
            await asyncio.sleep(delay)
            attempt += 1

        if attempt == max_attempts:
            logging.info(f"Max reconnection attempts reached for {peer_identifier}.")


    async def respond_to_heartbeat(self, writer, message):
        """Send a heartbeat_ack in response to a heartbeat and update last_seen."""
        logging.info(f"Heartbeat received with message: {message}")

        peer_info = writer.get_extra_info('peername')  # Extract peer_info

        # Update last_seen in your peers dictionary
        if peer_info in self.peers:
            self.peers[peer_info] = int(time.time())  # Update with current timestamp

        ack_message = {
            "type": "heartbeat_ack",
            "payload": "pong",
            "server_id": self.server_id
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        await writer.drain()
        await self.rewrite_peers_file(peer_info) # Save the change


    async def rewrite_peers_file(self, include_new_peers=False):
        """Rewrites the peers.dat file, ensuring uniqueness and optionally batching updates."""
        try:
            peers_to_write = self.peers.copy()  
            
            if include_new_peers:
                peers_to_write.update(self.new_peers)  
                self.new_peers.clear()  
            
            async with aiofiles.open("peers.dat", "w") as f:
                for peer_info, last_seen in peers_to_write.items():  
                    await f.write(f"{peer_info}:{last_seen or 'None'}\n") 

            logging.info("Peers file rewritten successfully.")  
        except IOError as e:
            logging.error(f"Failed to write to peers.dat: {e}")
        except Exception as e:
            logging.error(f"Failed to rewrite peers.dat: {e}\n{traceback.format_exc()}")


    async def send_heartbeat(self, writer):
        """Sends a heartbeat message to the connected peer every 60 seconds."""
        try:
            while not writer.is_closing():
                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id
                }
                logging.info("Sending heartbeat to peer.")
                writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
                await writer.drain()
                await asyncio.sleep(60)  # Send a heartbeat every 60 seconds.
        except asyncio.CancelledError:
            logging.info("Heartbeat sending cancelled.")
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")
            # Add backoff or delay before retrying to avoid spamming in case of persistent error
            await asyncio.sleep(10)
            if not writer.is_closing():
                logging.info("Attempting to restart heartbeat.")
                self.restart_heartbeat(writer)

    def restart_heartbeat(self, writer):
        if writer and not writer.is_closing():
            task = asyncio.create_task(self.send_heartbeat(writer))
            self.heartbeat_tasks.append(task)  # Track the task
            logging.info("Heartbeat task restarted.")
        else:
            logging.info("Writer is closing, not restarting heartbeat.")


    async def send_peer_list(self, writer):
        peer_list_message = {
            "type": "peer_list",
            "payload": list(self.peers),
            "server_id": self.server_id,
        }
        writer.write(json.dumps(peer_list_message).encode() + b'\n')
        await writer.drain()
        logging.info("Sent peer list to a peer.")
        # Do not close the writer or break the loop here


    async def start_p2p_server(self):
        """Start the P2P server and handle any startup errors."""
        try:
            # Check if the port is available (additional pre-check)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.host, self.p2p_port))
                s.close()
            
            self.p2p_server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port)
            logging.info(f"P2P server {self.server_id} listening on {self.host}:{self.p2p_port}")
            async with self.p2p_server:
                await self.p2p_server.serve_forever()

        except OSError as e:
            logging.error(f"Failed to start server on {self.host}:{self.p2p_port}: {e}")
            if e.errno == socket.EADDRINUSE:
                logging.error("Port is already in use. Please ensure the port is free and try again.")
            await self.close_p2p_server()

        except Exception as e:
            logging.error(f"Error starting P2P server: {e}")
            await self.close_p2p_server()



    async def update_peers(self, new_peers):
        updated = False
        for peer in new_peers:
            if peer not in self.peers and peer != self.external_ip:
                self.peers[peer] = None 
                logging.info(f"Peer {peer} added to the list.")
                updated = True
        if updated:
            await self.rewrite_peers_file()
            logging.info("Peers file updated successfully.")
