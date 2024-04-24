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

    async def load_peers(self):
        if os.path.exists("peers.dat"):
            async with aiofiles.open("peers.dat", "r") as f:
                async for line in f:
                    try:
                        peer, port, last_seen_str = line.strip().split(":")
                        last_seen = int(last_seen_str)
                        logging.debug(f"Loaded timestamp: {last_seen}, type: {type(last_seen)}")
                        self.peers[peer] = last_seen
                    except ValueError as e:
                        logging.info(f"error:{e}")
        else:
            # Initialize peers from seeds and save to file
            for seed in self.seeds:
                self.peers[seed] = int(time.time()) 
            await self.rewrite_peers_file()  # Save seeds to peers.dat
        logging.info("Peers loaded from file or initialized from seeds.")


    async def start_p2p_server(self):
        """Start the P2P server and handle any startup errors."""
        try:
            self.p2p_server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port)
            logging.info(f"P2P server {self.server_id} listening on {self.host}:{self.p2p_port}")
            async with self.p2p_server:
                await self.p2p_server.serve_forever()

        except OSError as e:
            if e.errno == socket.EADDRINUSE:
                logging.error(f"Port {self.p2p_port} is already in use. Please ensure the port is free and try again.")
            else:
                logging.error(f"Failed to start server: {e}")
            await self.close_p2p_server()

        except Exception as e:
            logging.error(f"Error starting P2P server: {e}")
            await self.close_p2p_server()



    async def connect_to_known_peers(self):
        """Asynchronously attempts to connect to all known peers using the default p2p_port, avoiding self-connection."""
        logging.info(f"Attempting to connect to known peers: {list(self.peers.keys())}")
        for peer_address in self.peers.keys():
            host = peer_address.split(':')[0]  # Extract only the host part

            # Prevent connecting to itself by comparing the host and port
            if host in [self.host, self.external_ip, "127.0.0.1"] or self.p2p_port == self.p2p_port:
                logging.info(f"Skipping connection to self: {host}")
                continue

            # Proceed with the connection attempt using the default P2P port
            asyncio.create_task(self.connect_to_peer(host, self.p2p_port))


    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        assert self is not None, "Null pointer exception: self is null"  # Debugging assertion
        assert addr is not None, "Null pointer exception: addr is null"  # Debugging assertion

        self.active_peers.add(addr)  # Assuming you have initialized self.active_peers as a set 
        logging.info(f"Connected to peer {addr}")

        try:
            data_buffer = ""
            while True:
                try:
                    data = await reader.read(1024)
                    if not data:
                        if reader.at_eof():
                            logging.info("Peer disconnected gracefully. Exiting listen loop.")
                            break  
                        else:
                            logging.info("No data received. Waiting for more data...")
                            await asyncio.sleep(1)
                            continue

                    data_buffer += data.decode()
                    while '\n' in data_buffer:
                        message, data_buffer = data_buffer.split('\n', 1)
                        if message:
                            logging.info(f"Received message from {addr}: {message}")
                            await self.process_message(json.loads(message), writer)
                            await self.rewrite_peers_file() # Save the change

                except asyncio.CancelledError:
                    logging.info(f"Connection task with {addr} cancelled (likely due to shutdown)")
                    break  # Exit the while loop if shutdown is in progress

        except Exception as e:
            logging.error(f"Error during P2P communication with {addr}: {e}")

        finally:
            logging.info(f"Closing connection with {addr}")
            self.active_peers.remove(addr)  # Remove the peer from active_peers
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
        peer_address = (host, port)
        peer_info = f"{host}:{port}"  # Or (host, port) if you use tuples

        if host in [self.host, self.external_ip, "127.0.0.1"]:
            return

        if host not in self.known_ips:
            self.known_ips.add(host)

        if peer_address in self.peers:  # Assuming peers contain connected + potential
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
                    "server_id": self.server_id
                }
                writer.write(json.dumps(handshake_msg).encode() + b'\n')
                await writer.drain()

                data = await reader.readline()
                ack_message = json.loads(data.decode())

                if ack_message["type"] == "ack":
                    if peer_info not in self.peers:  # New peer
                        self.peers[peer_info] = int(time.time()) 
                        logging.info(f"New peer discovered: {peer_info}") 

                    self.active_peers.add(peer_address)
                    await self.rewrite_peers_file()  
                    self.hello_seq += 1

                    # ... rest of connection logic (heartbeat, message handling) ...

                    logging.info(f"Successfully connected to {peer_info}")
                    break

            except Exception as e:
                logging.error(f"Failed to connect to {peer_info}. Error: {e}")

            finally:
                attempt += 1
                if attempt < 5:
                    await asyncio.sleep(self.calculate_backoff(attempt))

        if attempt == 5:
            logging.info(f"Max connection attempts reached for {peer_info}.")

        if writer and not writer.is_closing(): 
            writer.close()
            await writer.wait_closed()


    def calculate_backoff(self, attempt):
        """Calculates the backoff time with jitter."""
        return min(2 ** attempt + random.uniform(0, 1), 60)


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
        
    async def add_new_peers(self, new_peers):
        updated = False
        for ip in new_peers:
            if ip not in self.peers and ip != self.external_ip:
                self.peers[ip] = None  # No last seen time yet
                logging.info(f"New peer {ip} added to the list.")
                updated = True
        if updated:
            await self.rewrite_peers_file()


    async def rewrite_peers_file(self, include_new_peers=False):
        """Rewrites the peers.dat file, ensuring uniqueness and optionally batching updates."""
        try:
            peers_to_write = self.peers.copy()  # Start with a copy of existing peers

            if include_new_peers:
                peers_to_write.update(self.new_peers)  # Add new peers, maintaining uniqueness
                self.new_peers.clear()  

                async with aiofiles.open("peers.dat", "w") as f:
                    if include_new_peers:
                        self.new_peers.update(self.peers)  # Update in-place
                        peers_to_write = self.new_peers  
                        self.new_peers.clear()  
                    else:
                        peers_to_write = self.peers

                    for peer, last_seen in peers_to_write.items():  
                        await f.write(f"{peer}:{last_seen}\n")
                        
            logging.info("Peers file rewritten successfully.")  
        except IOError as e:
            logging.error(f"Failed to write to peers.dat: {e}")
        except Exception as e:
            logging.error(f"Failed to rewrite peers.dat: {e}\n{traceback.format_exc()}")


    async def rewrite_peers_file(self, include_new_peers=False):
        """Rewrites the peers.dat file, ensuring uniqueness and optionally batching updates."""
        try:
            peers_to_write = set(self.peers.keys())  # Start with unique peers

            if include_new_peers:
                peers_to_write.update(self.new_peers)  # Add new peers, maintaining uniqueness
                self.new_peers.clear()  

            async with aiofiles.open("peers.dat", "w") as f:
                for peer in peers_to_write:
                    last_seen = self.peers.get(peer, 0)  
                    await f.write(f"{peer}:{last_seen}\n")

            logging.info("Peers file rewritten successfully.")  
        except IOError as e:
            logging.error(f"Failed to write to peers.dat: {e}")
        except Exception as e:
            logging.error(f"Failed to rewrite peers.dat: {e}\n{traceback.format_exc()}")


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



    async def connect_to_new_peers(self, new_peers):
        """Attempt to connect to new peers not currently being connected to."""
        for peer_info in new_peers:
            host = peer_info
            port = self.p2p_port

            # Check if the peer is the node itself or if it's already in the list of active or connecting peers
            if host == self.external_ip or \
                host in self.peers:
    
                logging.info(f"Skipping connection to {host}.")
                continue

            # Now attempt to connect to the peer
            logging.info(f"Attempting to connect to new peer: {host}")
            self.peers[peer_info] = None 
            asyncio.create_task(self.connect_to_peer(host, port))


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
        """Processes received peer list, removing port info and updating internal data structures."""
        logging.info("Processing new peer list...")
        
        # Extract and process each peer in the received list
        for peer_info in peer_data:
            # Extract the IP address, discarding the port
            ip = peer_info.split(':')[0]
            
            # Update or add the peer with last seen time if it's a new connection
            if ip not in self.peers:
                logging.info(f"Adding new peer: {ip}")
                self.peers[ip] = None  # Newly discovered peer, last seen is None initially
            else:
                # Optionally update the last seen time for known peers here, if relevant
                logging.info(f"Peer {ip} already known, potentially updating last seen time.")
                # self.peers[ip] = int(time.time())  # Uncomment to update last seen time for existing peers
            
        # After processing the peer list, rewrite the peers file with updated information
        await self.rewrite_peers_file()
        logging.info("Peer list processed and peers file updated.")


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
            await self.respond_to_heartbeat(writer, message) 

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

                
    async def cancel_heartbeat_tasks(self):
        """Cancels all heartbeat tasks."""
        for task in self.heartbeat_tasks:
            task.cancel()  # Request cancellation of the task
            with contextlib.suppress(asyncio.CancelledError):
                await task  # Wait for the task to be cancelled
        self.heartbeat_tasks.clear()  # Clear the list of tasks after cancellation
        logging.info("All heartbeat tasks cancelled.")
