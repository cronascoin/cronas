import asyncio
import json
import logging
import os
import random
import uuid

logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.p2p_port = p2p_port
        self.peers = set(seeds if seeds else [])
        self.hello_seq = 0
        self.load_peers()
        self.rewrite_peers_file()

    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Connected to peer {addr}")
        
        data = await reader.readline()
        message = json.loads(data.decode())
        
        if message.get("type") == "hello":
            logging.info(f"Received handshake from {addr}")
            self.add_peer(addr)  # Add the new peer
            ack_message = {"type": "ack", "payload": "Handshake acknowledged"}
            writer.write(json.dumps(ack_message).encode() + b'\n')
            await writer.drain()
            logging.info(f"Handshake acknowledged to {addr}")
            
        # Initial handshake and message handling loop
        # Assuming handshake acknowledgement has been sent
        while True:
            try:
                data = await asyncio.wait_for(reader.readline(), timeout=30.0)  # 30 seconds timeout for demo
                if not data:
                    logging.info(f"Connection closed by {addr}")
                    break
                message = json.loads(data.decode())
                logging.info(f"Received {message} from {addr}")

                # Example handling of a heartbeat message
                if message.get("type") == "heartbeat":
                    logging.info(f"Heartbeat received from {addr}")
                    response = {"type": "heartbeat_ack", "payload": "pong"}
                    writer.write(json.dumps(response).encode() + b'\n')
                    await writer.drain()
            except asyncio.TimeoutError:
                logging.info(f"Heartbeat timeout for {addr}")
                break  # Exit the loop if a heartbeat message isn't received in time

        writer.close()
        await writer.wait_closed()

    async def start_p2p_server(self):
        server = await asyncio.start_server(self.handle_peer_connection, self.host, self.p2p_port)
        logging.info(f"P2P server {self.server_id} listening on {self.host}:{self.p2p_port}")
        async with server:
            await server.serve_forever()

    async def connect_to_peer(self, host, port, max_retries=5):
        attempt = 0
        writer = None

        try:
            while attempt < max_retries:
                try:
                    logging.info(f"Attempt {attempt + 1} to connect to {host}:{port}")
                    reader, writer = await asyncio.open_connection(host, port)
                    
                    # Increment and send handshake message with sequence number
                    self.hello_seq += 1
                    handshake_msg = {
                        "type": "hello",
                        "payload": f"Hello from {self.host}",
                        "seq": self.hello_seq,  # Include the sequence number
                        "server_id": self.server_id  # Include the server ID
                    }
                    writer.write(json.dumps(handshake_msg).encode() + b'\n')
                    await writer.drain()
                    
                    # Wait for handshake acknowledgment
                    data = await reader.readline()
                    ack_message = json.loads(data.decode())
                    if ack_message.get("type") == "ack":
                        logging.info(f"Handshake acknowledged by {host}:{port}")
                        # Your existing connection logic here...
                        break  # Successfully connected, exit the loop
                    
                    else:
                        logging.info(f"Unexpected response from {host}:{port}")
                        break  # Exit the loop, but try to cleanly close the connection in the finally block
                    
                except Exception as e:
                    logging.error(f"Connection attempt {attempt + 1} to {host}:{port} failed: {e}")
                    attempt += 1
                    backoff = min(2 ** attempt + random.uniform(0, 1), 60)  # Cap the backoff at 60 seconds
                    logging.info(f"Waiting {backoff:.2f} seconds before next attempt...")
                    await asyncio.sleep(backoff)

        finally:
            # Ensure the connection is closed if no longer needed
            if writer is not None:
                writer.close()
                await writer.wait_closed()
                logging.info("Connection closed.")

            if attempt == max_retries:
                logging.error(f"Failed to connect to {host}:{port} after {max_retries} attempts.")
            else:
                logging.info(f"Successfully connected to {host}:{port}.")

            # Ensure the connection is closed if no longer needed
            if writer is not None:
                writer.close()
                await writer.wait_closed()

    def rewrite_peers_file(self):
        with open("peers.dat", "w") as f:
            for peer in self.peers:
                f.write(f"{peer}\n")
        logging.info("Peers file updated.")

    def load_peers(self):
        if os.path.exists("peers.dat"):
            with open("peers.dat", "r") as f:
                for line in f:
                    self.peers.add(line.strip())
        logging.info("Peers loaded from file.")

    def add_peer(self, ip):
        # Assuming peers are stored in a set called self.peers
        self.peers.add(ip)
        self.rewrite_peers_file()