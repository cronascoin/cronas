#Copyright 2024 cronas.org
# app.py
# app.py

import asyncio
import contextlib
import os
import uuid
import string
import random
import logging
from peer import Peer
from rpc import RPCServer

version = '0.0.1'

logging.basicConfig(level=logging.INFO)

def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for _ in range(length))

def load_config(config_path='cronas.conf'):
    config = {}
    if not os.path.exists(config_path):
        config = {
            'server_id': str(uuid.uuid4()),
            'rpc_password': generate_password(),
            'rpc_port': '4334',
            'p2p_port': '4333',
            'addnode': ['137.184.80.215:4333']  # Example seed node
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
        with open(config_path, 'r') as configfile:
            for line in configfile:
                key, value = line.strip().split('=', 1)
                if key == 'addnode':
                    if key in config:
                        config[key].append(value)
                    else:
                        config[key] = [value]
                else:
                    config[key] = value
        logging.info("Loading from config file.")
    return config

async def shutdown(peer, rpc_server):
    logging.info("Shutting down...")
    await peer.close_p2p_server()
    await rpc_server.close_rpc_server()
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
    addnodes = config.get('addnode', [])


    peer = Peer('0.0.0.0', p2p_port, server_id, version)
    rpc_server = RPCServer(peer, '127.0.0.1', rpc_port, rpc_password)

    await asyncio.gather(
        peer.start_p2p_server(),
        rpc_server.start_rpc_server(),
        *(peer.connect_to_peer(seed.split(':')[0], int(seed.split(':')[1])) for seed in addnodes)
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown complete.")
