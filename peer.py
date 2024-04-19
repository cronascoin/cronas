import asyncio
import json
import logging
import os
import socket
import uuid
import random
import time


logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.p2p_port = p2p_port
        self.peers = set(seeds if seeds else [])
        self.seeds = seeds
        self.external_ip = self.detect_ip_address()
        self.load_peers()
        self.rewrite_peers_file()
        self.hello_seq = 0  # Initialize the hello_seq attribute here
        self.reconnect_delay = 5  # seconds
        self.connecting_peers = set()
        self.active_peers = set()  # Track active connections
        self.retry_attempts = {}  # Map: peer_identifier -> (last_attempt_time, attempt_count)
        self.cooldown_period = 60  # Cooldown period in seconds before retrying a connection


    async def start_p2p_server(self):
        try:
            self.p2p_server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port)
            logging.info(f"P2P server {self.server_id} listening on {self.host}:{self.p2p_port}")
            async with self.p2p_server:
                await self.p2p_server.serve_forever()
        except OSError as e:
            if e.errno == 10048:  # Windows specific error code for address already in use
                logging.error("Port 4333 is already in use. Please ensure the port is free and try again.")
            elif e.errno == socket.EADDRINUSE:  # Generic error code for address already in use (works across platforms)
                logging.error("Port 4333 is already in use. Please ensure the port is free and try again.")
            else:
                logging.error(f"Failed to start server: {e}")
                await self.close_p2p_server()
        except Exception as e:
            logging.error(f"Error starting P2P server: {e}")
            await self.close_p2p_server()
            
    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
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

        except Exception as e:
            logging.error(f"Error during communication with {addr}: {e}")
        finally:
            logging.info(f"Closing connection with {addr}")
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
            logging.info("Attempted to connect to self. Skipping.")
            return
        
        peer_identifier = f"{host}:{port}"
        self.connecting_peers.add(peer_identifier)
        
        if peer_identifier in self.active_peers:
            logging.info(f"Already connected to {peer_identifier}. Skipping.")
            return
        
        current_time = time.time()
        last_attempt, attempt_count = self.retry_attempts.get(peer_identifier, (0, 0))
        cooldown = self.calculate_backoff(attempt_count)

        if current_time - last_attempt < cooldown:
            logging.info(f"Cooldown period active for {peer_identifier}. Skipping attempt.")
            return

        self.retry_attempts[peer_identifier] = (current_time, attempt_count + 1)

        attempt = 0
        connected = False
        writer = None

        try:  # This try covers the connection attempts
            while attempt < max(attempt_count + 1, 5) and not connected:
                try:
                    logging.info(f"Attempt {attempt + 1} to connect to {host}:{port}")
                    reader, writer = await asyncio.open_connection(host, port)

                    self.hello_seq += 1
                    handshake_msg = {
                        "type": "hello",
                        "payload": f"Hello from {self.host}",
                        "seq": self.hello_seq,
                        "server_id": self.server_id
                    }
                    writer.write(json.dumps(handshake_msg).encode() + b'\n')
                    await writer.drain()

                    data = await reader.readline()
                    ack_message = json.loads(data.decode())
                    if ack_message.get("type") == "ack":
                        logging.info(f"Handshake acknowledged by {host}:{port}")
                        self.active_peers.add(peer_identifier)
                        logging.info(f"Successfully connected to {peer_identifier}.")
                        self.retry_attempts[peer_identifier] = (0, 0)  # Reset retry attempts after a successful connection
                        connected = True
                        asyncio.create_task(self.send_heartbeat(writer))
                        request_message = {"type": "request_peer_list", "server_id": self.server_id}
                        writer.write(json.dumps(request_message).encode() + b'\n')
                        await writer.drain()
                        logging.info("Requested peer list.")

                        # Transition into listening for messages without closing the connection.
                        await self.listen_for_messages(reader, writer)
                        # If listen_for_messages returns, it means the connection was closed.
                        return
                    else:
                        logging.info(f"Unexpected response from {host}:{port}")
                        writer.close()
                        await writer.wait_closed()
                        return

                except Exception as e:
                    logging.error(f"Failed to connect or communicate with {host}:{port}: {e}")

                finally:
                    attempt += 1
                    if not connected and attempt < max(attempt_count + 1, 5):
                        backoff = self.calculate_backoff(attempt)
                        logging.info(f"Waiting {backoff} seconds before next attempt.")
                        await asyncio.sleep(backoff)

        finally:  # This finally is now clearly associated with the try block
            self.connecting_peers.discard(peer_identifier)
            if writer is not None and not writer.is_closing():
                writer.close()
                await writer.wait_closed()

        if not connected:
            logging.info(f"Max attempts reached for {peer_identifier}. Will retry later.")



    def calculate_backoff(self, attempt):
            """Calculate the backoff time in seconds based on the attempt count."""
            return min(2 ** attempt + random.uniform(0, 1), 60)  # Cap the backoff at 60 seconds


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


    async def handle_client(reader, writer, peer):
        await peer.listen_for_messages(reader, writer)
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
            print(f"Failed to detect external IP address: {e}")
            return '127.0.0.1'


    def rewrite_peers_file(self):
        try:
            with open("peers.dat", "w") as f:
                for peer in self.peers:
                    f.write(f"{peer}\n")
            logging.info("Peers file updated successfully.")
        except IOError as e:
            logging.error(f"Failed to write to peers.dat: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while updating peers.dat: {e}")


    def load_peers(self):
        if os.path.exists("peers.dat"):
            with open("peers.dat", "r") as f:
                for line in f:
                    self.peers.add(line.strip())
        logging.info("Peers loaded from file.")


    def add_peer(self, ip):
        if ip in [self.host, self.external_ip, "127.0.0.1"]:
            return
        self.peers.add(ip)
        self.rewrite_peers_file()


    def update_peers(self, new_peers):
        """Adds new peers to the list and updates the peers file."""
        logging.info(f"Current peers before update: {self.peers}")  # Log current peers
        for peer in new_peers:
            if peer not in self.peers:
                self.peers.add(peer)
                logging.info(f"Peer {peer} added to the list.")
            else:
                logging.info(f"Peer {peer} already in the list.")  # Log if peer is already known
        self.rewrite_peers_file()
        logging.info(f"Current peers after update: {self.peers}")  # Log peers after update

        
    def respond_to_heartbeat(self, writer, message):
        """Send a heartbeat_ack in response to a heartbeat."""
        ack_message = {
            "type": "heartbeat_ack",
            "payload": "pong",
            "server_id": self.server_id  # Ensure you have self.server_id defined in your __init__
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        asyncio.create_task(writer.drain())


    async def connect_to_new_peers(self, new_peers):
        """Attempt to connect to new peers not currently being connected to."""
        for peer_ip in new_peers:
            if peer_ip not in self.peers and peer_ip != self.external_ip and peer_ip not in self.connecting_peers:
                self.peers.add(peer_ip)
                self.rewrite_peers_file()
                if peer_ip == self.detect_ip_address():
                    continue
                logging.info(f"Attempting to connect to new peer: {peer_ip}")
                host, port_str = peer_ip.split(":")
                port = int(port_str)
                logging.info(f"Attempting to connect to new peer: {peer_ip}")
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


    async def handle_peer_list(self, peer_data):
        """Processes received peer list and updates internal data structures."""
        try:
            new_peers = set(peer for peer in peer_data if peer not in self.peers and peer != self.external_ip)
            self.update_peers(new_peers)
            await self.connect_to_new_peers(new_peers)  # Connect to new peers asynchronously
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
            new_peers = message.get("payload", [])
            if new_peers:
                await self.handle_peer_list(new_peers)
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
            """
            Attempt to reconnect to a peer with an exponential backoff strategy.
            """
            peer_identifier = f"{host}:{port}"
            logging.info(f"Attempting to reconnect to {peer_identifier}")

            attempt = 0
            while True:
                if peer_identifier not in self.active_peers:
                    try:
                        await self.connect_to_peer(host, port)
                        logging.info(f"Reconnected to {peer_identifier} successfully.")
                        break  # Exit the loop upon successful reconnection
                    except Exception as e:
                        logging.error(f"Reconnection attempt to {peer_identifier} failed: {e}")
                else:
                    logging.info(f"Already connected to {peer_identifier}. No need to reconnect.")
                    break  # Exit the loop if already reconnected

                # Calculate the delay for the next reconnection attempt using exponential backoff
                delay = self.calculate_backoff(attempt)
                logging.info(f"Waiting for {delay} seconds before next reconnection attempt to {peer_identifier}.")
                await asyncio.sleep(delay)
                attempt += 1