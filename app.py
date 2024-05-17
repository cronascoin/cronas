# Copyright 2024 cronas.org
# app.py

import asyncio
import contextlib
import os
import time
import uuid
import string
import random
import logging
import subprocess
from peer import Peer
from rpc import RPCServer

logging.basicConfig(level=logging.INFO)

def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for _ in range(length))

def get_short_commit_hash():
    result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0:
        return result.stdout.strip()
    else:
        raise RuntimeError(f"Git command failed with error: {result.stderr}")

version = f'0.0.1-{get_short_commit_hash()}'

def load_config(config_path='cronas.conf', default_p2p_port=4333): # Added default_p2p_port parameter
    config = {}
    if not os.path.exists(config_path):
        config = {
            'server_id': str(uuid.uuid4()),
            'rpc_port': '4334',
            'p2p_port': '4333',
            'maxpeers': 10,
            'addnode': ['137.184.80.215:4333'],  # Example seed node
            'rpc_password': generate_password()
        }
        with open(config_path, 'w') as configfile:
            for key, value in config.items():
                if isinstance(value, list):  # Handle list values for 'addnode'
                    for item in value:
                        configfile.write(f"{key}={item}\n")
                else:
                    configfile.write(f"{key}={value}\n")
        logging.info("Config file created with default values.")
    else:
        try:
            with open(config_path, 'r') as configfile:
                for line in configfile:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        if key == 'addnode':
                            # Append default port if not specified
                            value = value if ":" in value else f"{value}:{default_p2p_port}"
                            config.setdefault(key, []).append(value)
                        else:
                            config[key] = value
                    else:
                        logging.warning(f"Skipping malformed line in config file: {line.strip()}")
        except Exception as e:
            logging.error(f"Failed to read the config file: {e}")
        logging.info("Loading from config file.")
    return config


async def shutdown(peer, rpc_server):
    logging.info("Shutting down...")
    # Add error handling for shutdown
    try:
        await peer.close_p2p_server()
    except Exception as e:
        logging.error(f"Error closing P2P server: {e}")
    
    try:
        await rpc_server.close_rpc_server()
    except Exception as e:
        logging.error(f"Error closing RPC server: {e}")
    
    await cancel_remaining_tasks()
    logging.info("Shutdown complete.")

async def cancel_remaining_tasks():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    logging.info(f"Cancelling {len(tasks)} remaining tasks...")
    for task in tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def main():
    config = load_config()
    server_id = config.get('server_id')
    rpc_password = config.get('rpc_password')
    rpc_port = int(config.get('rpc_port', 4334))
    p2p_port = int(config.get('p2p_port', 4333))
    max_peers = int(config.get('maxpeers', 10))  # Get maxpeers from config, default to 10

    peer = Peer('0.0.0.0', p2p_port, server_id, version, max_peers, config=config)

    # Load addnode peers immediately after creating the Peer object
    if not os.path.exists("peers.dat"):
        addnodes = config.get('addnode', [])
        for peer_info in addnodes:
            if peer.is_valid_peer(peer_info):
                peer.peers[peer_info] = int(time.time())

    rpc_server = RPCServer(peer, '127.0.0.1', rpc_port, rpc_password)  # Move this down

    tasks = [
        peer.start(),                 # Start peer first to connect to `addnode` peers
        rpc_server.start_rpc_server(),  
    ]

    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"Error during execution: {e}")
    finally:
        await shutdown(peer, rpc_server) 

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown initiated by user.")
