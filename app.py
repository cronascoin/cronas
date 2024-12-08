# app.py

import asyncio
import contextlib
import logging
import signal
import sys
import os
import uuid
import string
import random
import subprocess
import netifaces
import requests
import argparse
import platform

from crypto import Crypto
from peer import Peer
from block_reward import BlockReward
from rpc import RPCServer
from message import MessageHandler  # Import the MessageHandler class

# ----------------------------
# Versioning and Metadata
# ----------------------------

def get_short_commit_hash():
    """
    Retrieves the short Git commit hash for versioning.

    Returns:
        str: Short commit hash or 'unknown' if retrieval fails.
    """
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.SubprocessError:
        return 'unknown'

version = f'0.0.1-{get_short_commit_hash()}'

# ----------------------------
# Configuration Management
# ----------------------------

def generate_password(length=12):
    """
    Generates a random password containing letters, digits, and punctuation.

    Args:
        length (int): Length of the password.

    Returns:
        str: Generated password.
    """
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for _ in range(length))

def get_mac_address():
    """
    Retrieves the MAC address of the first non-loopback interface.

    Returns:
        str or None: MAC address or None if not found.
    """
    interfaces = netifaces.interfaces()
    for interface in interfaces:
        addresses = netifaces.ifaddresses(interface)
        if netifaces.AF_LINK in addresses:
            if mac_address_info := addresses[netifaces.AF_LINK]:
                mac_address = mac_address_info[0].get('addr')
                if mac_address and mac_address != '00:00:00:00:00:00':
                    return mac_address
    return None

def get_out_ip():
    """
    Detects the external IP address using external services.

    Returns:
        str or None: External IP address or None if detection fails.
    """
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=5)
        response.raise_for_status()
        ip = response.json().get('ip')
        if ip == '127.0.0.1':
            raise ValueError("Detected external IP is 127.0.0.1, which is invalid.")
        return ip
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Failed to detect external IP address: {e}")
        return get_fallback_ip()

def get_fallback_ip():
    """
    Fallback method to detect external IP using a different service.

    Returns:
        str or None: External IP address or None if detection fails.
    """
    try:
        response = requests.get('https://ifconfig.me', timeout=5)
        response.raise_for_status()
        ip = response.text.strip()
        if ip == '127.0.0.1':
            raise ValueError("Fallback detected external IP is 127.0.0.1, which is invalid.")
        return ip
    except requests.RequestException as e:
        logging.error(f"Fallback method failed to detect external IP address: {e}")
        return None

def generate_uuid_from_mac_and_ip(mac_address, ip_address):
    """
    Generates a UUID based on MAC address and IP address.

    Args:
        mac_address (str): MAC address.
        ip_address (str): IP address.

    Returns:
        str: Generated UUID.
    """
    namespace = uuid.UUID('00000000-0000-0000-0000-000000000000')
    combined = f"{mac_address.lower()}-{ip_address}"
    return str(uuid.uuid5(namespace, combined))

def read_config(config_path='cronas.conf'):
    """
    Reads the configuration file and parses key-value pairs.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        dict: Configuration parameters.
    """
    config = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as configfile:
                for line in configfile:
                    # Ignore comments and empty lines
                    if '=' in line and not line.strip().startswith('#'):
                        key, value = line.strip().split('=', 1)
                        if key == 'addnode':
                            # Support multiple addnode entries
                            config.setdefault(key, []).append(value)
                        else:
                            config[key] = value
        except Exception as e:
            logging.error(f"Failed to read the config file: {e}")
            sys.exit(1)
    else:
        logging.warning("Config file not found. Using default settings.")
    return config

def write_config(config, config_path='cronas.conf'):
    """
    Writes the configuration dictionary back to the configuration file.

    Args:
        config (dict): Configuration parameters.
        config_path (str): Path to the configuration file.
    """
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

def generate_server_id(config, config_path='cronas.conf'):
    """
    Generates and ensures the server_id is present and correct in the config.

    Args:
        config (dict): Current configuration.
        config_path (str): Path to the configuration file.

    Returns:
        str: The correct server_id.
    """
    mac_address = get_mac_address()
    out_ip = get_out_ip()

    if mac_address and out_ip:
        generated_uuid = generate_uuid_from_mac_and_ip(mac_address, out_ip)
        correct_server_id = generated_uuid
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
        'maxpeers': '10',
        'addnode': ['137.184.80.215:4333'],  # Replace with your seed nodes
        'rpc_username': 'admin',
        'rpc_password': generate_password(),
        'debug': 'false',
        'log_level': 'INFO',
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

    return config

def setup_logging_config(config):
    """
    Configures the logging based on the configuration.

    Args:
        config (dict): Configuration parameters.
    """
    log_level = config.get('log_level', 'INFO').upper()
    numeric_level = getattr(logging, log_level, logging.INFO)
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    
    file_handler = logging.FileHandler('cronas.log')
    file_handler.setLevel(numeric_level)
    
    # Create formatters and add to handlers
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


# ----------------------------
# Argument Parsing
# ----------------------------

def parse_args():
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Cronas P2P App")
    parser.add_argument('--config', type=str, default='cronas.conf', help='Path to configuration file')
    return parser.parse_args()

# ----------------------------
# Main Application
# ----------------------------

async def main(config_path):
    """
    Main entry point for the application. Initializes modules and starts services.

    Args:
        config_path (str): Path to the configuration file.
    """
    try:
        # Load configuration
        config = read_config(config_path)

        # Generate or validate server_id
        config = generate_server_id(config, config_path)

        # Setup logging based on config
        logger = setup_logging_config(config)
        logger.info(f"Starting Cronas P2P App version {version}")

        # Hard-Coded Parameters
        BLOCK_REWARD_AMOUNT = 24.0
        UPTIME_FILE = 'uptime.json'
        BLOCKCHAIN_JSON = 'blockchain.json'
        UTXOS_JSON = 'utxos.json'
        WALLET_FILE = 'wallet.dat'
        HOST = '0.0.0.0'
        PORT = 4333
        RPC_HOST = '0.0.0.0'  # Changed to bind to all interfaces
        # Adjusted 'localhost' to '0.0.0.0' to allow external access if needed

        # Initialize Crypto module with necessary parameters
        crypto = Crypto(
            blockchain_file=BLOCKCHAIN_JSON,
            utxo_file=UTXOS_JSON,
            wallet_file=WALLET_FILE
        )

        # Initialize Peer module
        addnode_list = config.get('addnode', [])

        peer = Peer(
            host=HOST,
            port=PORT,
            crypto=crypto,
            version=version,
            debug=config.get('debug', 'false').lower() == 'true',
            max_peers=int(config.get('maxpeers', '10')),
            server_id=config.get('server_id'),
            addnode=addnode_list
        )
        logger.info("Peer module initialized.")

        # Initialize MessageHandler with the peer instance
        message_handler = MessageHandler(peer=peer)
        peer.message = message_handler

        # Initialize BlockReward module
        block_reward = BlockReward(
            crypto=crypto,
            peer=peer,
            uptime_file=UPTIME_FILE,
            reward_amount=BLOCK_REWARD_AMOUNT,
        )

        # Retrieve RPC credentials from the configuration
        rpc_password = config.get('rpc_password', 'securepassword')
        rpc_username = config.get('rpc_username', 'admin')

        # Initialize RPC Server
        rpc_server = RPCServer(
            peer=peer,
            crypto=crypto,  # Added missing 'crypto' argument
            host=RPC_HOST,
            rpc_port=int(config.get('rpc_port', '4334')),
            rpc_username=rpc_username,
            rpc_password=rpc_password,
        )

        # Start Crypto module (loads data)
        await crypto.initialize()

        # Start RPC Server
        asyncio.create_task(rpc_server.start_rpc_server())

        # Connect to known peers
        for peer_address in addnode_list:
            try:
                addr, port = peer_address.split(':')
                logger.info(f"Attempting to connect to peer at {addr}:{port}")
                asyncio.create_task(peer.connect_to_peer(addr, int(port)))
            except ValueError:
                logger.warning(f"Invalid peer address format: {peer_address}")

        # Define shutdown handler inside main so it has access to rpc_server, peer, block_reward, and crypto
        async def shutdown_handler(signame):
            logger.info(f"Received signal {signame}: initiating shutdown...")

            logger.info("Shutting down RPC server...")
            await rpc_server.stop_rpc_server()

            logger.info("Shutting down Peer connections...")
            await peer.shutdown()

            logger.info("Shutting down BlockReward...")
            await block_reward.shutdown()

            logger.info("Shutting down Crypto module...")
            await crypto.shutdown()

            logger.info("Shutdown complete.")
            asyncio.get_event_loop().stop()

        # Register shutdown signals if supported
        supported_signals = ('SIGINT', 'SIGTERM')
        if platform.system() != 'Windows':
            for signame in supported_signals:
                try:
                    asyncio.get_running_loop().add_signal_handler(
                        getattr(signal, signame),
                        lambda signame=signame: asyncio.create_task(shutdown_handler(signame)),
                    )
                except NotImplementedError:
                    logger.warning(f"Signal {signame} not implemented on this platform.")
        else:
            logger.warning("Signal handling for graceful shutdown is not fully supported on Windows.")
            # Consider implementing an alternative shutdown mechanism if needed

        # Keep the application running indefinitely
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.Event().wait()

    except Exception:
        logging.exception("An error occurred in the main function.")
        sys.exit(1)

if __name__ == '__main__':
    args = parse_args()
    config_path = args.config
    try:
        asyncio.run(main(config_path))
    except KeyboardInterrupt:
        logging.info("Cronas P2P App shutting down gracefully.")
