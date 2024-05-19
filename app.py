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
import netifaces
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

def get_mac_address():
    interfaces = netifaces.interfaces()
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        if mac_address_info := addresses.get(netifaces.AF_LINK):
            return mac_address_info[0]['addr']
    return None

def generate_uuid_from_mac(mac_address):
    namespace = uuid.UUID('00000000-0000-0000-0000-000000000000')
    mac_address_clean = mac_address.replace(':', '').replace('-', '').lower()
    return uuid.uuid5(namespace, mac_address_clean)

def read_config(config_path='cronas.conf'):
    config = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as configfile:
                for line in configfile:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        if key == 'addnode':
                            config.setdefault(key, []).append(value)
                        else:
                            config[key] = value
        except Exception as e:
            logging.error(f"Failed to read the config file: {e}")
    return config

def write_config(config, config_path='cronas.conf'):
    try:
        with open(config_path, 'w') as configfile:
            for key, value in config.items():
                if isinstance(value, list):
                    for item in value:
                        configfile.write(f"{key}={item}\n")
                else:
                    configfile.write(f"{key}={value}\n")
        logging.info("Config file updated.")
    except Exception as e:
        logging.error(f"Failed to write to the config file: {e}")

def load_config(config_path='cronas.conf', default_p2p_port=4333):
    config = read_config(config_path)

    if mac_address := get_mac_address():
        generated_uuid = generate_uuid_from_mac(mac_address)
        correct_server_id = str(generated_uuid)
        logging.info(f"MAC Address: {mac_address}, Generated UUID: {correct_server_id}")
    else:
        logging.warning("MAC address could not be found, generating random UUID for server_id.")
        correct_server_id = str(uuid.uuid4())

    if 'server_id' not in config or config['server_id'] != correct_server_id:
        config['server_id'] = correct_server_id
        write_config(config, config_path)
        logging.info("Server ID has been updated in the config file.")
    else:
        logging.info("Server ID in the config file is correct.")

    if 'rpc_port' not in config:
        config['rpc_port'] = '4334'
    if 'p2p_port' not in config:
        config['p2p_port'] = '4333'
    if 'maxpeers' not in config:
        config['maxpeers'] = 10
    if 'addnode' not in config:
        config['addnode'] = ['137.184.80.215:4333']  # Example seed node
    if 'rpc_password' not in config:
        config['rpc_password'] = generate_password()

    return config

async def shutdown(peer, rpc_server):
    logging.info("Shutting down...")
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
    max_peers = int(config.get('maxpeers', 10))

    peer = Peer('0.0.0.0', p2p_port, server_id, version, max_peers, config=config)

    if not os.path.exists("peers.dat"):
        addnodes = config.get('addnode', [])
        for peer_info in addnodes:
            if peer.is_valid_peer(peer_info):
                peer.peers[peer_info] = int(time.time())

    rpc_server = RPCServer(peer, '127.0.0.1', rpc_port, rpc_password)

    tasks = [
        peer.start(),
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
