import asyncio
import json
import logging
import os
import socket
import uuid
import random
import signal


logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.p2p_port = p2p_port
        self.peers = set(seeds if seeds else [])
        self.external_ip = self.detect_ip_address()
        self.load_peers()
        self.rewrite_peers_file()
        self.hello_seq = 0  # Initialize the hello_seq attribute here
        self.reconnect_delay = 5  # seconds
            
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

    async def start_p2p_server(self):
        self.p2p_server = await asyncio.start_server(
            self.handle_peer_connection, self.host, self.p2p_port)
        logging.info(f"P2P server {self.server_id} listening on {self.host}:{self.p2p_port}")
        async with self.p2p_server:
            await self.p2p_server.serve_forever()

    async def close_p2p_server(self):
        if self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logging.info("P2P server closed.")

    async def connect_to_peer(self, host, port, max_retries=5):
        if host in [self.host, self.external_ip, "127.0.0.1"]:
            logging.info("Attempted to connect to self. Skipping.")
            return

        attempt = 0

        while attempt < max_retries:
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
                    logging.info("Successfully connected, starting to send heartbeats.")
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
                # Implementing a backoff before retrying to connect again.
                attempt += 1
                backoff = min(2 ** attempt + random.uniform(0, 1), 60)
                await asyncio.sleep(backoff)

        if writer is not None and not writer.is_closing():
            writer.close()
            await writer.wait_closed()

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
            logging.info("Peer disconnected.")
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
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                return s.getsockname()[0]
        except Exception:
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
        for peer_ip in new_peers:
            if peer_ip not in self.peers and peer_ip != self.external_ip:
                self.peers.add(peer_ip)
                self.rewrite_peers_file()
                # Prevent connecting to self.
                if peer_ip == self.detect_ip_address():
                    continue
                logging.info(f"Attempting to connect to new peer: {peer_ip}")
                # Asynchronously attempt to connect to the new peer.
                asyncio.create_task(self.connect_to_peer(peer_ip, self.p2p_port))

    async def send_heartbeat(self, writer):
        """Sends a heartbeat message to the connected peer every 60 seconds."""
        try:
            while not writer.is_closing():
                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id
                }
                logging.info(f"Sending heartbeat to peer.")
                writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
                await writer.drain()
                await asyncio.sleep(60)  # Send a heartbeat every 60 seconds.
        except asyncio.CancelledError:
            logging.info("Heartbeat sending task cancelled.")
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")

    def handle_peer_list(self, peer_data):
        """Processes received peer list and updates internal data structures."""
        try:
            new_peers = [peer for peer in peer_data if peer not in self.peers and peer != self.external_ip] 
            self.update_peers(new_peers)
            asyncio.create_task(self.connect_to_new_peers(new_peers))
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
            logging.info("Heartbeat acknowledged to {}".format(addr))

        elif message.get("type") == "peer_list":
            logging.info(f"Received peer list from {addr}")
            new_peers = message.get("payload", [])
            if new_peers:
                self.handle_peer_list(new_peers)
            else:
                logging.warning("Received empty peer list.")

        else:
            logging.info(f"Unhandled message type from {addr}: {message['type']}")


    async def shutdown(peer, rpc_server):
        logging.info("Cancelling all running tasks...")
        
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
            try:
                await task  # Wait for the task cancellation to complete
            except asyncio.CancelledError:
                pass  # Expected part of graceful shutdown
        
        logging.info("All running tasks cancelled, proceeding with server shutdowns...")
        
        await peer.close_p2p_server()
        logging.info("P2P server closed.")
        
        await rpc_server.close_rpc_server()
        logging.info("RPC Server closed.")

        await peer.send_heartbeat()
        logging.info("Heartbeat Messaging closed.")

        logging.info("Shutdown complete.")
