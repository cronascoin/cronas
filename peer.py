#Copyright 2024 cronas.org
#peer.py

import asyncio
import errno
import json
import logging
import os
import socket
import aiofiles
import time
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
        self.shutdown_flag = False  # Initialize the shutdown flag
        

    async def add_peer(self, peer_info):
        host, port = peer_info.split(":")
        if host in [self.host, self.external_ip, self.out_ip, "127.0.0.1", "localhost"]:
            logging.info(f"Attempted to add self as peer: {peer_info}")
            return False

        if peer_info in self.peers:
            logging.info(f"Updated last seen for peer: {peer_info}")
        else:
            logging.info(f"Added new peer: {peer_info}")
        self.peers[peer_info] = int(time.time())
        self.mark_peer_changed()
        return True


    async def calculate_backoff(self, attempt):
        """Calculates the backoff time with escalating delays."""
        try:
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
        except asyncio.CancelledError:
            # Gracefully handle cancellation by raising it again
            raise

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
        peer_info = f"{host}:{port}"
        if host in [self.host, self.external_ip, self.out_ip, "127.0.0.1"]:
            return

        logging.info(f"Attempting to connect to {peer_info}")
        writer = None
        successful_connection = False
        attempt = 0
        while not self.shutdown_flag and attempt < 5:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                handshake_msg = {
                    "type": "hello",
                    "payload": f"Hello from {self.host}",
                    "version": self.version,
                    "seq": self.hello_seq + 1,
                    "server_id": self.server_id,
                    "listening_port": self.p2p_port
                }
                writer.write(json.dumps(handshake_msg).encode() + b'\n')
                await writer.drain()

                # Delegate to process_message to handle all incoming messages
                await self.listen_for_messages(reader, writer)

                successful_connection = True
                break

            except (asyncio.TimeoutError, json.JSONDecodeError, Exception) as e:
                logging.error(f"Error while connecting to {peer_info}: {e}")
                attempt += 1
                await asyncio.sleep(await self.calculate_backoff(attempt))

            finally:
                if not successful_connection and writer is not None and not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()

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
        """Detect the external IP address using an API service with specific error handling."""
        try:
            response = requests.get('https://api.ipify.org?format=json')
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return response.json()['ip']
        except requests.RequestException as e:
            logging.error(f"Failed to detect external IP address: {e}")
            return '127.0.0.1'

    async def handle_disconnection(self, peer_info):
        """Handles cleanup when a peer disconnects or an error occurs."""
        if peer_info in self.peers:  # Confirm peer is still tracked
            self.peers[peer_info] = int(time.time())  # Final update on connection close
            await self.rewrite_peers_file()  # Final rewrite to ensure all data is up-to-date
        if peer_info in self.active_peers:  # Remove from active_peers if present
            del self.active_peers[peer_info]
            logging.info(f"Removed {peer_info} from active peers due to disconnection or error.")


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

    async def listen_for_messages(self, reader, writer):
        addr = writer.get_extra_info('peername')
        host, port = addr[0], addr[1]  # Directly extract host and port
        peer_info = f"{host}:{port}"  # Format peer_info as 'host:port'
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
            logging.info("Incomplete read error, attempting to reconnect...")
            await self.handle_disconnection(peer_info)
            asyncio.create_task(self.reconnect_to_peer(host, int(port)))  # Correctly pass host and port
        except Exception as e:
            logging.error(f"Error during communication with {peer_info}: {e}")
            await self.handle_disconnection(peer_info)
        finally:
            logging.info(f"Closing connection with {peer_info}")
            await self.handle_disconnection(peer_info)
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

    async def process_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        await self.update_last_seen(peer_info)
        logging.info(f"Received message from {peer_info}: {message}")

        if message.get("type") == "hello":
            # Respond to hello with ack, update peers, start listening for further messages
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

        elif message.get("type") == "ack":
            # Initial post-handshake ack handling
            remote_version = message.get("version", "unknown")
            remote_server_id = message["server_id"]
            self.active_peers[remote_server_id] = {"host": addr[0], "port": addr[1], "version": remote_version}
            logging.info(f"Connection fully established with {peer_info}, version {remote_version}")
            self.mark_peer_changed()
            await self.rewrite_peers_file()
            # Start sending heartbeats
            asyncio.create_task(self.send_heartbeat(writer))

        elif message.get("type") == "request_peer_list":
            await self.send_peer_list(writer)

        elif message.get("type") == "heartbeat":
            await self.respond_to_heartbeat(writer, message)

        elif message.get("type") == "peer_list":
            if new_peers := message.get("payload", []):
                logging.info("Processing and updating with new peer list...")
                await self.update_peers(new_peers)
                self.mark_peer_changed()
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
            # Check if peer is already considered connected
            if (host, port) not in self.active_peers:  # Check against the appropriate collection
                try:
                    await self.connect_to_peer(host, port)
                    logging.info(f"Reconnected to {peer_identifier} successfully.")
                    peer_info = f"{host}:{port}"
                    self.peers[peer_info] = int(time.time())  # Update last_seen
                    self.active_peers[peer_info] = (host, port)  # Ensure it is marked as active
                    break
                except Exception as e:
                    logging.error(f"Reconnection attempt to {peer_identifier} failed: {e}")
                    attempt += 1
                    delay = self.calculate_backoff(attempt)
                    logging.info(f"Waiting for {delay} seconds before next reconnection attempt to {peer_identifier}.")
                    await asyncio.sleep(delay)
            else:
                logging.info(f"Already connected to {peer_identifier}. No need to reconnect.")
                break

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
            
