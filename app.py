# Copyright 2024 Cronas.org
# app.py

import asyncio
import contextlib
import os
import uuid
import string
import random
import logging
import subprocess
import netifaces
import requests
import argparse
from peer import Peer
from rpc import RPCServer

def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for _ in range(length))

def get_short_commit_hash():
    result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0:
        return result.stdout.strip()
    else:
        return 'unknown'  # Handle case where Git command fails

version = f'0.0.1-{get_short_commit_hash()}'

def get_mac_address():
    interfaces = netifaces.interfaces()
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        if mac_address_info := addresses.get(netifaces.AF_LINK):
            return mac_address_info[0]['addr']
    return None

def generate_uuid_from_mac_and_ip(mac_address, ip_address):
    namespace = uuid.UUID('00000000-0000-0000-0000-000000000000')
    combined = f"{mac_address.lower()}-{ip_address}"
    return uuid.uuid5(namespace, combined)

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
        logging.info("Config file updated successfully.")
    except Exception as e:
        logging.error(f"Failed to write to the config file '{config_path}': {e}")
        raise e

def get_out_ip():
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=5)
        response.raise_for_status()
        ip = response.json()['ip']
        if ip == '127.0.0.1':
            raise ValueError("Detected out_ip is 127.0.0.1, which is invalid.")
        return ip
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Failed to detect external IP address: {e}")
        return get_fallback_ip()

def get_fallback_ip():
    """Fallback method to detect external IP using a different service."""
    try:
        response = requests.get('https://ifconfig.me', timeout=5)
        response.raise_for_status()
        ip = response.text.strip()
        if ip == '127.0.0.1':
            raise ValueError("Fallback detected out_ip is 127.0.0.1, which is invalid.")
        return ip
    except requests.RequestException as e:
        logging.error(f"Fallback method failed to detect external IP address: {e}")
        return None

def load_config(config_path='cronas.conf'):
    config = read_config(config_path)

    mac_address = get_mac_address()
    out_ip = get_out_ip()
    
    if mac_address and out_ip:
        generated_uuid = generate_uuid_from_mac_and_ip(mac_address, out_ip)
        correct_server_id = str(generated_uuid)
        logging.info(f"MAC Address: {mac_address}, Public IP: {out_ip}, Generated UUID: {correct_server_id}")
    else:
        logging.warning("MAC address or public IP address could not be found, generating random UUID for server_id.")
        correct_server_id = str(uuid.uuid4())

    config_changed = False

    if 'server_id' not in config or config['server_id'] != correct_server_id:
        config['server_id'] = correct_server_id
        config_changed = True

    default_config = {
        'rpc_port': '4334',
        'p2p_port': '4333',
        'maxpeers': 10,
        'addnode': ['137.184.80.215:4333'],
        'rpc_password': generate_password(),
        'debug': 'false',
        'log_level': 'INFO'
    }

    for key, value in default_config.items():
        if key not in config:
            config[key] = value
            config_changed = True

    if config_changed:
        write_config(config, config_path)
        logging.info("Configuration updated.")
    else:
        logging.info("Configuration is up to date.")

    # Set the logging level dynamically
    log_level = config.get('log_level', 'INFO').upper()
    logging.getLogger().setLevel(log_level)

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

async def main(config_path):
    config = load_config(config_path)
    server_id = config.get('server_id')
    rpc_password = config.get('rpc_password')
    rpc_port = int(config.get('rpc_port', 4334))
    p2p_port = int(config.get('p2p_port', 4333))
    max_peers = int(config.get('maxpeers', 10))

    peer = Peer('0.0.0.0', p2p_port, server_id, version, max_peers, config=config)

    rpc_server = RPCServer(peer, '127.0.0.1', rpc_port, rpc_password)

    # Create tasks for peer and RPC server
    peer_task = asyncio.create_task(peer.start())
    rpc_task = asyncio.create_task(rpc_server.start_rpc_server())

    try:
        await asyncio.gather(peer_task, rpc_task)
    except Exception as e:
        logging.error(f"Error during execution: {e}")
    finally:
        await shutdown(peer, rpc_server)

def parse_args():
    parser = argparse.ArgumentParser(description="Cronas P2P App")
    parser.add_argument('--config', type=str, default='cronas.conf', help='Path to configuration file')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    try:
        asyncio.run(main(config_path=args.config))
    except KeyboardInterrupt:
        logging.info("Shutdown initiated by user.")
