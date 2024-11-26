# app.py

import asyncio
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
from crypto import Crypto

# Centralized Logging Configuration
def setup_logging(log_level='INFO'):
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for _ in range(length))

def get_short_commit_hash():
    result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True)
    return result.stdout.strip() if result.returncode == 0 else 'unknown'

version = f'0.0.1-{get_short_commit_hash()}'

def get_mac_address():
    interfaces = netifaces.interfaces()
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        if netifaces.AF_LINK in addresses:
            if mac_address_info := addresses[netifaces.AF_LINK]:
                mac_address = mac_address_info[0]['addr']
                if mac_address and mac_address != '00:00:00:00:00:00':
                    return mac_address
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
        'maxpeers': '10',
        'addnode': ['137.184.80.215:4333'],  # Replace with your seed nodes
        'rpc_username': 'admin',
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
    setup_logging(log_level)

    return config

async def shutdown(peer, rpc_server, crypto):
    logging.info("Shutting down...")
    try:
        await asyncio.wait_for(peer.shutdown(), timeout=10)
    except asyncio.TimeoutError:
        logging.warning("Timeout while shutting down Peer.")
    except Exception as e:
        logging.error(f"Error shutting down Peer: {e}", exc_info=True)

    try:
        await asyncio.wait_for(rpc_server.close_rpc_server(), timeout=5)
    except asyncio.TimeoutError:
        logging.warning("Timeout while closing RPC server.")
    except Exception as e:
        logging.error(f"Error closing RPC server: {e}", exc_info=True)

    try:
        await asyncio.wait_for(crypto.shutdown(), timeout=10)
    except asyncio.TimeoutError:
        logging.warning("Timeout while shutting down Crypto module.")
    except Exception as e:
        logging.error(f"Error shutting down Crypto module: {e}", exc_info=True)

    if pending_tasks := [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    ]:
        logging.info(f"Cancelling {len(pending_tasks)} pending tasks...")
        for task in pending_tasks:
            task.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(*pending_tasks, return_exceptions=True), timeout=15)
        except asyncio.TimeoutError:
            logging.warning("Timeout while cancelling pending tasks.")
        except Exception as e:
            logging.error(f"Error cancelling tasks: {e}", exc_info=True)
    logging.info("Shutdown complete.")

async def main(config_path):
    config = load_config(config_path)
    server_id = config.get('server_id')
    rpc_username = config.get('rpc_username', 'admin')
    rpc_password = config.get('rpc_password')
    rpc_port = int(config.get('rpc_port', '4334'))
    p2p_port = int(config.get('p2p_port', '4333'))
    max_peers = int(config.get('maxpeers', '10'))

    # Create Peer instance
    peer = Peer('0.0.0.0', p2p_port, server_id, version, max_peers, config=config)

    # Create Crypto instance and set it in Peer
    crypto = Crypto(peer)
    peer.set_crypto(crypto)

    # Create RPCServer instance
    rpc_server = RPCServer(peer, '127.0.0.1', rpc_port, rpc_password, rpc_username=rpc_username)

    # Start the Crypto module before starting the Peer
    await crypto.start()

    # Create tasks for peer and RPC server
    peer_task = asyncio.create_task(peer.start())
    rpc_task = asyncio.create_task(rpc_server.start_rpc_server())

    try:
        await asyncio.gather(peer_task, rpc_task)
    except KeyboardInterrupt:
        logging.info("Shutdown initiated by user.")
        await shutdown(peer, rpc_server, crypto)
    except Exception as e:
        logging.error(f"Error during execution: {e}", exc_info=True)
        await shutdown(peer, rpc_server, crypto)
    finally:
        if not peer.shutdown_flag:
            await shutdown(peer, rpc_server, crypto)

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
    except Exception as e:
        logging.error(f"Unhandled exception: {e}", exc_info=True)
