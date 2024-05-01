#Copyright 2024 cronas.org
#peer.py

import asyncio
import contextlib
import errno
import json
import logging
import os
import socket
import aiofiles
import time
import re
import requests
import ipaddress

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

class Peer:
    def __init__(self, host, p2p_port, seeds, server_id, version):
        self.server_id = server_id
        self.host = host
        self.p2p_port = p2p_port
        self.peers = {}  # Initialize as a dictionary
        self.active_peers = {}  # Initialize as a dictionary
        self.seeds = seeds
        self.external_ip = self.detect_ip_address()
        self.out_ip = self.detect_out_ip()
        self.hello_seq = 0  # Initialize the hello_seq attribute here
        self.heartbeat_tasks = []  # Add this line to track heartbeat tasks
        self.new_peers = []
        self.version = version
        self.file_lock = asyncio.Lock()  # Initialize the asyncio lock
        self.file_write_scheduled = False
        self.pending_file_write = False
        self.file_write_delay = 10  # Delay in seconds before 
        self.peers_changed = False
        self.connections = {}  # Dictionary to track connections


    async def add_peer(self, peer_info):
        """Adds a peer or updates an existing one without additional validation, and writes to file."""
        if peer_info in self.peers:
            logging.info(f"Updated last seen for peer: {peer_info}")
        else:
            logging.info(f"Added new peer: {peer_info}")
        self.peers[peer_info] = int(time.time())  # Add/update with current timestamp as last seen
        await self.rewrite_peers_file()  # Directly rewrite the file after updating peers


    def calculate_backoff(self, attempt):
        """Calculates the backoff time with escalating delays."""
        if attempt == 1:
            return 3600  # 1 hour
        elif attempt == 2:
            return 86400  # 1 day
        elif attempt == 3:
            return 604800  # 1 week
        elif attempt >= 4:
            return 2592000  # 1 month
        else:
            return 60  # Default backoff for any other cases, e.g., 0 attempts


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
            
            if host in [self.host, self.external_ip, self.out_ip, "127.0.0.1"]:
                logging.info(f"Skipping connection to self or local address: {host}:{port}")
                continue

            logging.info(f"Preparing to connect to {host}:{port}")
            task = asyncio.create_task(self.connect_to_peer(host, port))
            tasks.append(task)

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

            if host in [self.host, self.external_ip, self.out_ip, "127.0.0.1"]:
                continue
            
            # Now attempt to connect to the peer
            logging.info(f"Attempting to connect to new peer: {host}:{port}")
            if peer_info not in self.peers:
                self.peers[peer_info] = None  # Optionally consider storing more useful information here
            asyncio.create_task(self.connect_to_peer(host, port))


    async def connect_to_peer(self, host, port):
        # sourcery skip: low-code-quality
        peer_info = f"{host}:{port}"
        if host in [self.host, self.external_ip, self.out_ip, "127.0.0.1"]:
            return

        logging.info(f"Attempting to connect to {peer_info}")
        writer = None
        successful_connection = False
        attempt = 0
        while attempt < 5:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                handshake_msg = {
                    "type": "hello",
                    "payload": f"Hello from {self.host}",
                    "seq": self.hello_seq + 1,
                    "server_id": self.server_id,
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
                    remote_server_id = ack_message["server_id"]
                    if remote_server_id in self.active_peers:
                        existing_host, existing_port = self.active_peers[remote_server_id]
                        if (existing_host, existing_port) != (host, port):
                            logging.info(f"Replacing old connection {existing_host}:{existing_port} with new connection {host}:{port} for server_id {remote_server_id}.")
                            await self.close_connection(existing_host, existing_port)
                    
                    self.peers[peer_info] = int(time.time())
                    self.active_peers[remote_server_id] = (host, port)
                    logging.info(f"Connected and acknowledged by peer with server_id {remote_server_id}: {peer_info}")
                    
                    asyncio.create_task(self.send_heartbeat(writer))
                    request_msg = {"type": "request_peer_list", "server_id": self.server_id}
                    writer.write(json.dumps(request_msg).encode() + b'\n')
                    await writer.drain()

                    await self.listen_for_messages(reader, writer)

                    self.hello_seq += 1
                    logging.info(f"Successfully connected to {peer_info}")
                    successful_connection = True
                    break

            except (asyncio.TimeoutError, json.JSONDecodeError, Exception) as e:
                logging.error(f"Error while connecting to {peer_info}: {e}")

            finally:
                if not successful_connection and writer and not writer.is_closing():
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
        
        
    def detect_out_ip(self):
        try:
            response = requests.get('https://api.ipify.org?format=json')
            return response.json()['ip']
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
        """Processes received peer list, updating internal data structures and attempting to connect to new peers."""
        logging.info("Processing new peer list...")
        new_peers = []

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
                await self.add_peer(peer_address)  # Use add_peer to add new peers
                new_peers.append(peer_address)
            else:
                logging.info(f"Updating last seen time for known peer: {peer_address}")
                self.peers[peer_address] = int(time.time())  # Still directly updating time for known peers

        # After processing the peer list, rewrite the peers file with updated information
        await self.rewrite_peers_file()
        logging.info("Peer list processed and peers file updated.")

        # Attempt to connect to new peers if any
        if new_peers:
            await self.connect_to_new_peers(new_peers)


    def is_valid_peer(self, peer_info):
        try:
            ip, port = peer_info.split(':')
            ipaddress.ip_address(ip)  # Validate IP address
            port = int(port)
            # Ephemeral ports typically range from 32768 to 65535; adjust if your range differs
            return not 32768 <= port <= 65535
        except ValueError as e:
            logging.error(f"Invalid peer address '{peer_info}': {e}")
            return False


    async def listen_for_messages(self, reader, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"  # Format peer_info as 'host:port'
        logging.info(f"Listening for messages from {peer_info}")
        try:
            while True:
                data_buffer = await reader.readuntil(separator=b'\n')
                if not data_buffer:
                    logging.info("Connection closed by peer.")
                    break
                message = json.loads(data_buffer.decode().strip())
                self.peers[peer_info] = int(time.time())  # Update last_seen timestamp
                await self.process_message(message, writer)
                await self.rewrite_peers_file()  # Update peers.dat file after processing each message
        except asyncio.IncompleteReadError:
            logging.info("Incomplete read error, reconnecting...")
            asyncio.create_task(self.reconnect_to_peer(peer_info))
        except Exception as e:
            logging.error(f"Error during communication with {peer_info}: {e}")
        finally:
            logging.info(f"Closing connection with {peer_info}")
            if peer_info in self.peers:  # Confirm peer is still tracked
                self.peers[peer_info] = int(time.time())  # Final update on connection close
                await self.rewrite_peers_file()  # Final rewrite to ensure all data is up-to-date
            writer.close()
            await writer.wait_closed()


    async def load_peers(self):
        """Loads peers from the peers.dat file, merging with seeds, and creates the file if it doesn't exist."""
        loaded_peers_count = 0
        skipped_peers_count = 0

        # Load existing peers if file exists
        if os.path.exists("peers.dat"):
            async with aiofiles.open("peers.dat", "r") as f:
                async for line in f:
                    try:
                        parts = line.strip().split(":")
                        if len(parts) != 3:
                            raise ValueError("Line format incorrect, expected 3 parts.")

                        peer, port, last_seen_str = parts
                        peer_info = f"{peer}:{port}"
                        last_seen = int(last_seen_str)  # Convert last seen to integer

                        # Here you could include checks if necessary (e.g., validate_format(peer_info))
                        self.peers[peer_info] = last_seen
                        loaded_peers_count += 1
                    except ValueError as e:
                        logging.warning(f"Invalid line in peers.dat: {line} - Error: {e}")
                        skipped_peers_count += 1

        # Merge seeds
        for seed in self.seeds:
            if seed not in self.peers:
                self.peers[seed] = int(time.time())  # Use current time for last seen
                loaded_peers_count += 1  # Count seed as loaded if it was added
                self.mark_peer_changed()  # This marks the peers list as changed


        # Save the updated peers list back to the file, creating it if necessary
        await self.rewrite_peers_file()
        logging.info(f"Peers loaded from file or initialized from seeds. Loaded: {loaded_peers_count}, Skipped: {skipped_peers_count}.")


    def mark_peer_changed(self):
        self.peers_changed = True
        logging.debug("Peers data marked as changed.")


    def parse_address(self):
        if match := re.match(r"^(?:(\[[\d:a-fA-F]+\])|([\d\.]+)):(\d+)$", self):
            ip = match[1] or match[2]
            port = int(match[3])
            return ip.strip('[]'), port
        else:
            raise ValueError("Invalid address format")
        

    async def process_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        await self.update_last_seen(f"{addr[0]}:{addr[1]}")
        logging.info(f"Received message from {peer_info}: {message}")

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
                logging.info(f"Handshake acknowledged to {peer_info}")
            else:
                logging.error("Invalid or missing listening port in handshake message.")

        elif message.get("type") == "request_peer_list":
            logging.info(f"Peer list requested by {peer_info}")
            # Send the peer list
            await self.send_peer_list(writer)

        elif message.get("type") == "heartbeat":
            await self.respond_to_heartbeat(writer, message)

        elif message.get("type") == "peer_list":
            logging.info(f"Received peer list from {peer_info}")
            if new_peers := message.get("payload", []):
                logging.info("Processing and updating with new peer list...")
                self.mark_peer_changed()
                await self.update_peers(new_peers)
            else:
                logging.warning("Received empty peer list.")

        elif message.get("type") == "heartbeat_ack":
            logging.info(f"Heartbeat acknowledgment from {peer_info}")

        else:
            logging.info(f"Unhandled message type from {peer_info}: {message['type']}")


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
        await self.rewrite_peers_file()  # Correctly call the method without parameters


    def restart_heartbeat(self, writer):
        if writer and not writer.is_closing():
            task = asyncio.create_task(self.send_heartbeat(writer))
            self.heartbeat_tasks.append(task)  # Track the task
            logging.info("Heartbeat task restarted.")
        else:
            logging.info("Writer is closing, not restarting heartbeat.")
            
            
    async def rewrite_peers_file(self):
        """Rewrites the peers.dat file if there have been changes."""
        if not self.peers_changed:
            return  # Exit if no changes have been made

        async with self.file_lock:
            try:
                valid_peers = {peer_info: last_seen for peer_info, last_seen in self.peers.items() if self.is_valid_peer(peer_info)}
                async with aiofiles.open("peers.dat", "w") as f:
                    for peer_info, last_seen in valid_peers.items():
                        await f.write(f"{peer_info}:{last_seen}\n")
                self.peers_changed = False  # Reset change flag after writing
                logging.info("Peers file rewritten successfully.")
            except OSError as e:
                logging.error(f"Failed to open peers.dat: {e}")
            except Exception as e:
                logging.error(f"Failed to rewrite peers.dat: {e}")


    async def schedule_periodic_peer_save(self):
        """Periodically saves the peers list to a file."""
        while True:
            await asyncio.sleep(60)  # Wait for 60 seconds before each check
            await self.rewrite_peers_file()  # Attempt to rewrite the file if there were changes


    async def schedule_rewrite(self):
        """Schedule a delayed rewrite of the peers file."""
        if not self.file_write_scheduled:
            self.file_write_scheduled = True
            while self.pending_file_write:
                self.pending_file_write = False  # Reset pending flag
                await asyncio.sleep(self.file_write_delay)  # Delay before actually writing
            await self.rewrite_peers_file()  # Perform the file writing
            self.file_write_scheduled = False  # Reset the scheduled flag
        else:
            self.pending_file_write = True  # Set pending write flag if another write is scheduled


    async def send_heartbeat(self, writer):
        """Sends a heartbeat message to the connected peer every 60 seconds."""
        try:
            while not writer.is_closing():
                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id
                }
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


    async def send_peer_list(self, writer):
        # Initialize an empty list to store peer data
        peer_list = []
        
        # Read the peers from the peers.dat file
        async with aiofiles.open("peers.dat", "r") as f:
            async for line in f:
                if line.strip():  # Ensure the line is not empty
                    parts = line.strip().split(':')
                    if len(parts) >= 2:  # Basic validation to check format
                        ip_port = f"{parts[0]}:{parts[1]}"  # Recreate the IP:port format
                        peer_list.append(ip_port)

        # Prepare the message containing the peer list
        peer_list_message = {
            "type": "peer_list",
            "payload": peer_list,
            "server_id": self.server_id,
        }

        # Serialize the peer data to JSON and send it
        writer.write(json.dumps(peer_list_message).encode() + b'\n')
        await writer.drain()
        logging.info("Sent sanitized peer list from peers.dat to a peer.")



    async def start_p2p_server(self):
        try:
            await self.load_peers()
            await self.connect_to_known_peers()
            asyncio.create_task(self.schedule_periodic_peer_save())

            server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port,
                reuse_address=True
            )
            self.p2p_server = server
            logging.info(f"P2P server version {self.version} with ID {self.server_id} listening on {self.host}:{self.p2p_port}")

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
        """Update the last seen time for a given peer."""
        self.peers[peer_info] = int(time.time())
        await self.schedule_rewrite()  # Schedule a rewrite after updating


    async def update_peers(self, new_peers):
        """
        Updates the list of peers with new entries, ensuring that only valid peers are added.
        """
        updated = False
        for peer_info in new_peers:
            if self.is_valid_peer(peer_info) and ':' in peer_info:
                ip, port = peer_info.split(':')
                if ip == self.out_ip:
                    logging.info(f"Skipping addition of own external IP {peer_info} to peer list.")
                    continue
                if peer_info not in self.peers:
                    self.peers[peer_info] = int(time.time())  # Update last seen to current time
                    logging.info(f"Peer {peer_info} added to the list.")
                    updated = True

        if updated:
            self.mark_peer_changed()
            await self.rewrite_peers_file()  # Save changes if any valid new peers were added
            
    async def close_connection(self, host, port):
        if (host, port) in self.connections:
            _, writer = self.connections.pop((host, port))
            if writer and not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.info(f"Successfully closed connection to {host}:{port}")
        else:
            logging.info(f"No active connection found for {host}:{port} to close.")