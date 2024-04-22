import asyncio
import json
import logging
import os
import socket
import uuid
import random
import aiofiles
import traceback
import time

logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.p2p_port = p2p_port
        self.peers = {}
        self.seeds = seeds
        self.external_ip = self.detect_ip_address()
        self.hello_seq = 0  # Initialize the hello_seq attribute here
        self.reconnect_delay = 5  # seconds
        self.connecting_peers = set()
        self.active_peers = set()  # Track active connections
        self.retry_attempts = {}  # Map: peer_identifier -> (last_attempt_time, attempt_count)
        self.cooldown_period = 60  # Cooldown period in seconds before retrying a connection
        self.heartbeat_tasks = []  # Add this line to track heartbeat tasks
 

    async def async_init(self):
        await self.load_peers()
        asyncio.create_task(self.periodic_flush_peers_to_file())


    async def load_peers(self):
        """Loads peers from the peers.dat file, focusing on IP addresses without ports."""
        if os.path.exists("peers.dat"):
            async with aiofiles.open("peers.dat", "r") as f:
                async for line in f:
                    parts = line.strip().split(',')
                    if len(parts) == 2:
                        ip, last_seen_str = parts
                        last_seen = None if last_seen_str == 'None' else int(last_seen_str)
                        self.peers[ip] = last_seen
                    elif len(parts) == 1:
                        # If there's only one part, it's an IP without last_seen time
                        ip = parts[0]
                        if ip not in self.peers:  # Check to avoid overwriting existing entries
                            self.peers[ip] = None
                    else:
                        logging.warning(f"Skipping malformed line: {line.strip()}")
        else:
            # Initialize peers from seeds (here assuming seeds are just IPs)
            for seed in self.seeds:
                self.peers[seed] = None
            await self.rewrite_peers_file()
        logging.info("Peers loaded from file or initialized from seeds.")



    async def start_p2p_server(self):
        """Start the P2P server and handle any startup errors."""
        try:
            assert self is not None, "Null pointer exception: self is null"

            self.p2p_server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port)
            assert self.p2p_server is not None, "Null pointer exception: p2p_server is null"

            logging.info(f"P2P server {self.server_id} listening on {self.host}:{self.p2p_port}")
            async with self.p2p_server:
                assert self.p2p_server is not None, "Null pointer exception: p2p_server is null"

                await self.p2p_server.serve_forever()

        except AssertionError as e:
            logging.error(f"Fatal error during P2P server startup: {e}")

        except OSError as e:
            if e.errno == socket.EADDRINUSE:  # Error code for address already in use, works across platforms
                logging.error(f"Port {self.p2p_port} is already in use. Please ensure the port is free and try again.")
            else:
                logging.error(f"Failed to start server: {e}")
            await self.close_p2p_server()

        except Exception as e:
            logging.error(f"Error starting P2P server: {e}")
            await self.close_p2p_server()


    async def connect_to_known_peers(self):
        """Asynchronously attempts to connect to all known peers."""
        default_port = self.p2p_port
        logging.info(f"Attempting to connect to known peers: {self.peers}")
        for peer_address in self.peers:
            # Directly include the logic to parse the peer address
            if ':' in peer_address:
                host, port_str = peer_address.split(':', 1)
                port = int(port_str)
            else:
                host = peer_address
                port = default_port

            # Now, host and port are defined, proceed with the connection
            if (host, port) not in self.active_peers and (host, port) not in self.connecting_peers:
                asyncio.create_task(self.connect_to_peer(host, port))


    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        assert self is not None, "Null pointer exception: self is null"
        assert addr is not None, "Null pointer exception: addr is null"

        self.active_peers.add(addr)  # Keep track of active connections
        logging.info(f"Connected to peer {addr}")
        try:
            data_buffer = ""
            while True:
                data = await reader.read(1024)
                if not data:
                    if reader.at_eof():
                        logging.info("Peer disconnected gracefully. Exiting listen loop.")
                        break  # Only break if peer disconnected gracefully
                    else:
                        logging.info("No data received. Waiting for more data...")
                        await asyncio.sleep(1)
                        continue

                data_buffer += data.decode()
                while '\n' in data_buffer:
                    message, data_buffer = data_buffer.split('\n', 1)
                    if message:
                        await self.process_message(json.loads(message), writer)
                    # Do not break after processing; wait for more messages

        except AssertionError as e:
            logging.error(f"Fatal error during P2P communication with {addr}: {e}")
        except Exception as e:
            logging.error(f"Error during P2P communication with {addr}: {e}")
        finally:
            logging.info(f"Closing connection with {addr}")
            self.active_peers.remove(addr)  # Clean up after disconnection
            writer.close()
            await writer.wait_closed()



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


    async def connect_to_peer(self, host, port):
        if host in [self.host, self.external_ip, "127.0.0.1"]:
            return

        peer_tuple = (host, port)  # Use a tuple for consistency

        if peer_tuple in self.active_peers or peer_tuple in self.connecting_peers:
            return

        self.connecting_peers.add(peer_tuple)
        logging.info(f"Attempting to connect to {host}:{port}")

        attempt = 0
        writer = None

        while attempt < 5:
            try:
                reader, writer = await asyncio.open_connection(host, port)

                seq = self.hello_seq + 1
                handshake_msg = {
                    "type": "hello",
                    "payload": f"Hello from {self.host}",
                    "seq": seq,
                    "server_id": self.server_id
                }
                writer.write(json.dumps(handshake_msg).encode() + b'\n')
                await writer.drain()

                data = await reader.readline()
                ack_message = json.loads(data.decode())
                if ack_message["type"] == "ack":
                    self.active_peers.add(peer_tuple)
                    self.hello_seq = seq

                    # Update the last seen time for this peer in the peers dictionary
                    self.peers[host] = int(time.time())

                    asyncio.create_task(self.send_heartbeat(writer))
                    request_message = {"type": "request_peer_list", "server_id": self.server_id}
                    writer.write(json.dumps(request_message).encode() + b'\n')
                    await writer.drain()

                    await self.listen_for_messages(reader, writer)
                    
                    break  # Exit the loop after successful connection

            except Exception as e:
                logging.error(f"Failed to connect to {host}:{port}: {e}")

            finally:
                attempt += 1
                if attempt < 5:
                    await asyncio.sleep(self.calculate_backoff(attempt))

        if attempt == 5:
            logging.info(f"Max connection attempts reached for {host}:{port}.")

        if writer is not None and not writer.is_closing():
            writer.close()
            await writer.wait_closed()

        self.connecting_peers.remove(peer_tuple)


    def calculate_backoff(self, attempt):
        """Calculates the backoff time with jitter."""
        return min(2 ** attempt + random.uniform(0, 1), 60)


    async def periodic_flush_peers_to_file(self, interval_seconds=60):
        """Periodically saves the peers data to the file."""
        while True:
            await asyncio.sleep(interval_seconds)  # Wait for the specified interval
            await self.rewrite_peers_file()  # Save the current state of peers to the file
            logging.info("Peers file updated in background.")


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


    async def handle_client(self, writer, peer):
        await peer.listen_for_messages(self, writer)
        writer.close()
        await writer.wait_closed()


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


    async def rewrite_peers_file(self):
        """Rewrites the peers.dat file with IP and last seen timestamp, excluding ports."""
        try:
            async with aiofiles.open("peers.dat", "w") as f:
                for ip, last_seen in self.peers.items():
                    # Prepare last_seen string, 'None' if not available
                    last_seen_str = 'None' if last_seen is None else str(last_seen)
                    # Write the peer IP and last_seen timestamp to the file
                    await f.write(f"{ip},{last_seen_str}\n")

            logging.info("Peers file rewritten successfully with current data, excluding ports.")
        except IOError as e:
            logging.error(f"Failed to write to peers.dat: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while updating peers.dat: {e}\n{traceback.format_exc()}")


    async def add_peer(self, peer_info):
        # Directly use peer_info without splitting to handle IP:Port format
        if ":" in peer_info:
            ip, port = peer_info.split(":")
        else:
            ip = peer_info
            port = self.p2p_port
            peer_info = f"{ip}:{port}"  # Reformat peer_info with default port if not specified

        if ip in [self.host, self.external_ip, "127.0.0.1"]:
            logging.info(f"Skipping adding self or localhost to peers: {ip}")
            return

        # Check if ip is not already in self.peers
        # This assumes the peer identification is based on IP address only for simplicity
        if ip not in self.peers:
            self.peers[ip] = None  # Initialize last seen time as None
            await self.rewrite_peers_file()  # Persist peers information asynchronously
            logging.info(f"Added new peer: {peer_info}")
        else:
            logging.info(f"Peer {peer_info} already exists. No action taken.")


    async def update_peers(self, new_peers):
        updated = False
        for peer_info in new_peers:
            # Assuming peer_info comes in IP:Port format
            if ":" in peer_info:
                ip, port = peer_info.split(":", 1)
            else:
                ip = peer_info
                port = self.p2p_port  # Use default port if not specified
                peer_info = f"{ip}:{port}"  # Reformat peer_info with default port if not specified
            
            # Prevent adding the peer if it's the node itself or localhost
            if ip in [self.host, self.external_ip, "127.0.0.1"]:
                logging.info(f"Skipping adding self or localhost to peers: {ip}")
                continue

            # Update peer information or add new peer
            if ip not in self.peers:
                self.peers[ip] = None  # Initialize last seen time as None for new peers
                logging.info(f"Added new peer: {peer_info}")
                updated = True
            # Optional: Update last seen for existing peers if you're receiving a fresh list
            # This part depends on your application logic; you may choose to update last seen time here or elsewhere

        # Call rewrite_peers_file only once if there were any updates
        if updated:
            await self.rewrite_peers_file()
            logging.info("Peers file updated successfully.")



    async def connect_to_new_peers(self, new_peers):
        """Attempt to connect to new peers not currently being connected to."""
        for peer_info in new_peers:
            host = peer_info
            port = self.p2p_port

            # Check if the peer is the node itself or if it's already in the list of active or connecting peers
            if host == self.external_ip or \
                host in self.active_peers or \
                host in self.connecting_peers:
    
                logging.info(f"Skipping connection to {host}.")
                continue

            # Now attempt to connect to the peer
            logging.info(f"Attempting to connect to new peer: {host}")
            self.connecting_peers.add(host)
            asyncio.create_task(self.connect_to_peer(host, port))


    async def respond_to_heartbeat(self, writer, message):
        """Send a heartbeat_ack in response to a heartbeat."""
        logging.info(f"Heartbeat received with message: {message}")
        ack_message = {
            "type": "heartbeat_ack",
            "payload": "pong",
            "server_id": self.server_id
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        await writer.drain()


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
            logging.info("Closing heartbeat messages.")
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")

            # Example of starting a heartbeat task and tracking it
            task = asyncio.create_task(self.send_heartbeat(writer))
            self.heartbeat_tasks.append(task)  # Track the task


    async def handle_peer_list(self, peer_data):
        """Processes received peer list and updates internal data structures."""
        try:
            logging.info("Processing new peer list...")
            new_peers = set(peer_data) - self.peers - {self.external_ip}
            self.update_peers(new_peers)
            
            # Optionally, if you want to immediately attempt to connect to these new peers:
            await self.connect_to_new_peers(new_peers)
        except Exception as e:
            logging.error(f"Error in handle_peer_list: {e}")


    async def process_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Received message from {addr}: {message}")

        if message.get("type") == "hello":
            logging.info(f"Received handshake from {addr}")
            # Acknowledge the handshake
            ack_message = {
                "type": "ack",
                "payload": "Handshake acknowledged",
                "server_id": self.server_id
            }
            writer.write(json.dumps(ack_message).encode() + b'\n')
            await writer.drain()
            logging.info(f"Handshake acknowledged to {addr}")

        elif message.get("type") == "request_peer_list":
            logging.info(f"Peer list requested by {addr}")
            # Send the peer list
            await self.send_peer_list(writer)

        elif message.get("type") == "heartbeat":
            logging.info(f"Heartbeat received from {addr}")
            # Send a heartbeat acknowledgment
            ack_message = {
                "type": "heartbeat_ack",
                "payload": "pong",
                "server_id": self.server_id
            }
            writer.write(json.dumps(ack_message).encode() + b'\n')
            await writer.drain()
            logging.info(f"Heartbeat acknowledged to {addr}".format(addr))

        elif message.get("type") == "peer_list":
            logging.info(f"Received peer list from {addr}")
            if new_peers := message.get("payload", []):
                logging.info("Processing and updating with new peer list...")
                await self.update_peers(new_peers)  # Directly call update_peers here
            else:
                logging.warning("Received empty peer list.")

        elif message.get("type") == "heartbeat_ack":
            logging.info(f"Heartbeat acknowledgment from {addr}")

        else:
            logging.info(f"Unhandled message type from {addr}: {message['type']}")


    async def close_p2p_server(self):
        if self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logging.info("P2P server closed.")


    async def reconnect_to_peer(self, host, port):
        peer_identifier = f"{host}:{port}"
        logging.info(f"Attempting to reconnect to {peer_identifier}")

        attempt = 0
        max_attempts = 5  # Define a maximum number of reconnection attempts

        while attempt < max_attempts:
            if (host, port) not in self.active_peers:  # Assuming active_peers stores (host, port) tuples
                try:
                    await self.connect_to_peer(host, port)
                    logging.info(f"Reconnected to {host} successfully.")
                    self.active_peers.add((host, port))  # Update active_peers appropriately
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

                
    async def cancel_heartbeat_tasks(self):
        """Cancels all heartbeat tasks."""
        for task in self.heartbeat_tasks:
            task.cancel()  # Request cancellation of the task
            try:
                await task  # Wait for the task to be cancelled
            except asyncio.CancelledError:
                pass  # Expected, as we requested cancellation
        self.heartbeat_tasks.clear()  # Clear the list of tasks after cancellation
        logging.info("All heartbeat tasks cancelled.")
