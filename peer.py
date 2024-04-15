import asyncio
import json
import logging
import os
import random

logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.host = host
        self.p2p_port = p2p_port
        self.peers = set(seeds if seeds else [])
        self.rewrite_peers_file()

    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Connected to peer {addr}")
        
        data = await reader.readline()
        message = json.loads(data.decode())
        
        if message.get("type") == "hello":
            logging.info(f"Received handshake from {addr}")
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
        logging.info(f"P2P server listening on {self.host}:{self.p2p_port}")
        async with server:
            await server.serve_forever()

    async def connect_to_peer(self, host, port, max_retries=5):
        attempt = 0
        writer = None

        while attempt < max_retries:
            try:
                logging.info(f"Attempt {attempt + 1} to connect to {host}:{port}")
                reader, writer = await asyncio.open_connection(host, port)
                
                # Send handshake message
                handshake_msg = {"type": "hello", "payload": f"Hello from {self.host}"}
                writer.write(json.dumps(handshake_msg).encode() + b'\n')
                await writer.drain()
                
                # Wait for handshake acknowledgment
                data = await reader.readline()
                ack_message = json.loads(data.decode())
                if ack_message.get("type") == "ack":
                    logging.info(f"Handshake acknowledged by {host}:{port}")
                    
                    # Keep the connection open for further communication
                    try:
                        while True:
                            # Send a heartbeat message every 60 seconds
                            heartbeat_msg = {"type": "heartbeat", "payload": "ping"}
                            writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
                            await writer.drain()
                            
                            # Wait for heartbeat acknowledgment with a 5 minutes timeout
                            data = await asyncio.wait_for(reader.readline(), timeout=300)  # 5 minutes timeout
                            if not data:
                                logging.info("Connection closed by the server.")
                                break
                            ack_message = json.loads(data.decode())
                            if ack_message.get("type") == "heartbeat_ack":
                                logging.info("Heartbeat acknowledged by the server.")
                            else:
                                logging.info("Unexpected message from the server.")
                            
                            await asyncio.sleep(60)  # Sleep for 60 seconds before sending the next heartbeat
                            
                    except asyncio.TimeoutError:
                        logging.error("Heartbeat ack not received within the expected 5 minutes timeframe.")
                    break
                    
                else:
                    logging.info(f"Unexpected response from {host}:{port}")
                    break
                    
            except Exception as e:
                logging.error(f"Connection attempt {attempt + 1} to {host}:{port} failed: {e}")
                attempt += 1
                    
                # Calculate exponential backoff with jitter
                backoff = min(2 ** attempt + random.uniform(0, 1), 60)  # Cap the backoff at 60 seconds
                logging.info(f"Waiting {backoff:.2f} seconds before next attempt...")
                await asyncio.sleep(backoff)

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

    def load_peers(self):
        if os.path.exists("peers.dat"):
            with open("peers.dat", "r") as f:
                for line in f:
                    self.peers.add(line.strip())
