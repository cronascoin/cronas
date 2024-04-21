import asyncio
import json
import logging
import os
import socket
import uuid
import random
import aiofiles


logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.p2p_port = p2p_port
        self.peers = set(seeds or [])
        self.seeds = seeds
        self.external_ip = self.detect_ip_address()
        self.hello_seq = 0  # Initialize the hello_seq attribute here
        self.reconnect_delay = 5  # seconds
        self.connecting_peers = set()
        self.active_peers = set()  # Track active connections
        self.retry_attempts = {}  # Map: peer_identifier -> (last_attempt_time, attempt_count)
        self.cooldown_period = 60  # Cooldown period in seconds before retrying a connection

    async def async_init(self):
        """Asynchronous initialization tasks."""
        await self.load_peers()  # Ensure this method is async if file I/O is involved
        await self.connect_to_known_peers()  # A new method to handle connections

    async def connect_to_known_peers(self):
        """Asynchronously attempts to connect to all known peers."""
        default_port = 4334
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
            
    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        assert self is not None, "Null pointer exception: self is null"
        assert addr is not None, "Null pointer exception: addr is null"

        self.active_peers.add(addr)
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

        self.connecting_peers.remove(peer_tuple)  # Ensure this uses the tuple format


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
            print(f"Failed to detect external IP address: {e}")
            return '127.0.0.1'


    async def rewrite_peers_file(self):
        try:
            async with aiofiles.open("peers.dat", "w") as f:
                for peer in self.peers:
                    await f.write(f"{peer}\n")
            logging.info("Peers file rewritten successfully.")
        except IOError as e:
            logging.error(f"Failed to write to peers.dat: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while updating peers.dat: {e}")

    async def load_peers(self):
        """Loads peers from the peers.dat file and adds a default port if not specified."""
        if os.path.exists("peers.dat"):
            async with aiofiles.open("peers.dat", "r") as f:
                async for line in f:
                    peer = line.strip()
                    if ":" not in peer:
                        peer += ":4334"  # Append default port if not specified
                    self.peers.add(peer)
        logging.info("Peers loaded from file.")


    def add_peer(self, peer_info):
        # Assuming peer_info could be just an IP or "IP:Port", we split and take the first part (IP)
        ip = peer_info.split(":")[0]
        if ip in [self.host, self.external_ip, "127.0.0.1"]:
            logging.info(f"Skipping adding self or localhost to peers: {ip}")
            return
        if ip not in self.peers:
            self.peers.add(ip)
            self.rewrite_peers_file()
            logging.info(f"Added new peer: {ip}")


    async def update_peers(self, new_peers):
        updated = False
        for peer in new_peers:
            if peer not in self.peers and peer != self.external_ip:
                self.peers.add(peer)
                logging.info(f"Peer {peer} added to the list.")
                updated = True
            else:
                logging.info(f"Peer {peer} already in the list.")
        
        if updated:
            await self.rewrite_peers_file()  # Assuming this is also made async
        logging.info("Peers file updated successfully.")


        
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
            """
            Attempt to reconnect to a peer with an exponential backoff strategy.
            """
            peer_identifier = f"{host}:{port}"
            logging.info(f"Attempting to reconnect to {peer_identifier}")

            attempt = 0
            while True:
                if host not in self.active_peers:
                    try:
                        await self.connect_to_peer(host, port)
                        logging.info(f"Reconnected to {host} successfully.")
                        break  # Exit the loop upon successful reconnection
                    except Exception as e:
                        logging.error(f"Reconnection attempt to {host} failed: {e}")
                else:
                    logging.info(f"Already connected to {peer_identifier}. No need to reconnect.")
                    break  # Exit the loop if already reconnected

                # Calculate the delay for the next reconnection attempt using exponential backoff
                delay = self.calculate_backoff(attempt)
                logging.info(f"Waiting for {delay} seconds before next reconnection attempt to {peer_identifier}.")
                await asyncio.sleep(delay)
                attempt += 1
                
