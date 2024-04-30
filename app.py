#Copyright 2024 cronas.org
#app.py

import asyncio
import contextlib
import os
import uuid
import configparser
import logging
from peer import Peer
from rpc import RPCServer

version = '0.0.1'

logging.basicConfig(level=logging.INFO)

def load_config(config_path='cronas.conf'):
    config = configparser.ConfigParser()
    if not os.path.exists(config_path):
        config['DEFAULT'] = {
            'server_id': str(uuid.uuid4()),
            'rpc_port': '4334',
            'p2p_port': '4333',
            'addnode': '137.184.80.215:4333'  # Example seed node
        }
        with open(config_path, 'w') as configfile:
            config.write(configfile)
        logging.info("Config file created with default values.")
    else:
        logging.info("Loading from config file.")
    
    config.read(config_path)
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
    server_id = config['DEFAULT']['server_id']
    rpc_port = int(config['DEFAULT']['rpc_port'])
    p2p_port = int(config['DEFAULT']['p2p_port'])
    addnode = config['DEFAULT']['addnode']

    peer = Peer('0.0.0.0', p2p_port, [addnode], server_id, version)
    rpc_server = RPCServer(peer, '127.0.0.1', rpc_port)
    #await peer.async_init()  # Ensures the Peer object is fully initialized before proceeding


    await asyncio.gather(
        peer.start_p2p_server(),
        rpc_server.start_rpc_server(),
        *(peer.connect_to_peer(seed.split(':')[0], int(seed.split(':')[1])) for seed in [addnode])
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown complete.")
