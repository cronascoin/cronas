import configparser
import os
import uuid
import asyncio
import contextlib
import logging
from peer import Peer
from rpc import RPCServer

def check_and_create_config():
    config_path = 'cronas.conf'
    if not os.path.exists(config_path):
        config = configparser.ConfigParser()
        config['DEFAULT'] = {
            'server_id': str(uuid.uuid4()),
            'rpc_port': '4334',
            'p2p_port': '4333',
            'addnode': '137.184.80.215:4333'  # Example seed node
        }
        with open(config_path, 'w') as configfile:
            config.write(configfile)
        print("Config file created with default values.")
    else:
        print("Config file already exists.")
    return load_config(config_path)

def load_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def get_configuration():
    config = check_and_create_config()
    server_id = config['DEFAULT']['server_id']
    rpc_port = int(config['DEFAULT']['rpc_port'])
    p2p_port = int(config['DEFAULT']['p2p_port'])
    addnode = config['DEFAULT']['addnode']
    return server_id, rpc_port, p2p_port, addnode

logging.basicConfig(level=logging.INFO)

async def shutdown(peer, rpc_server):
    logging.info("Shutting down...")
    await peer.cancel_heartbeat_tasks()
    await peer.close_p2p_server()
    await rpc_server.close_rpc_server()
    await cancel_remaining_tasks()
    logging.info("Shutdown complete.")

async def cancel_remaining_tasks():
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

async def main():
    server_id, rpc_port, p2p_port, addnode = get_configuration()
    p2p_host = '0.0.0.0'
    rpc_host = '127.0.0.1'
    seeds = [addnode]  # Use addnode from config file

    # Create and initialize the Peer object
    peer = Peer(p2p_host, p2p_port, seeds, server_id)

    await peer.async_init()  # Ensures the Peer object is fully initialized before proceeding
    
    # Initialize the RPC server with the already initialized Peer object
    rpc_server = RPCServer(peer, rpc_host, rpc_port)

    try:
        await asyncio.gather(
            peer.start_p2p_server(),
            rpc_server.start_rpc_server(),
            *(peer.connect_to_peer(seed.split(':')[0], int(seed.split(':')[1])) for seed in seeds)
        )
    except asyncio.CancelledError:
        logging.info("CancelledError caught, shutting down.")
        await shutdown(peer, rpc_server)
    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt. Initiating shutdown...")
        await shutdown(peer, rpc_server)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    asyncio.run(main())
