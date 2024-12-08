#!/usr/bin/env python3
"""
Cronas P2P Network CLI

This script allows users to interact with the Cronas P2P Network RPC server.
Available commands include fetching peer information, sending transactions,
adding nodes, sending messages, broadcasting messages, retrieving messages,
getting wallet info, and initiating blockchain synchronization.

Usage:
    python3 cli.py <command> [arguments]

Commands:
    getpeerinfo                                         - Get information about connected peers
    transaction <receiver_address> <amount>             - Send a transaction
    addnode <addr> <port>                               - Add a new node by IP address and port
    sendmessage <recipient_id> <message>                - Send a message to a specific peer
    broadcast <message>                                 - Broadcast a message to all peers
    getmessages                                         - Retrieve received messages
    getwalletinfo                                       - Get wallet address and balance
    sync                                                - Initiate blockchain synchronization
    help                                                - Display this help message
"""

import requests
import sys
import json
import datetime
import base64
import os
import logging
import argparse

# Configuration Constants
RPC_SERVER = "http://localhost:4334"  # RPC server URL
CONFIG_FILE = "cronas.conf"            # Configuration file path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def read_config(config_path=CONFIG_FILE):
    """
    Reads the configuration from the specified config file.

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
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if key.lower() == 'addnode':
                            config.setdefault(key, []).append(value)
                        else:
                            config[key.lower()] = value
        except Exception as e:
            logger.error(f"Failed to read the config file: {e}")
    else:
        logger.warning(f"Config file '{config_path}' not found. Proceeding with environment variables only.")
    return config


def get_auth_header(config):
    """
    Constructs the authentication header using Basic Auth.

    Args:
        config (dict): Configuration parameters containing RPC credentials.

    Returns:
        dict: Headers with the Authorization field set.
    """
    rpc_username = os.getenv('RPC_USERNAME', config.get('rpc_username'))
    rpc_password = os.getenv('RPC_PASSWORD', config.get('rpc_password'))
    if not rpc_username or not rpc_password:
        logger.error("RPC credentials not set in cronas.conf or environment variables. Please set 'rpc_username' and 'rpc_password'.")
        sys.exit(1)
    credentials = f"{rpc_username}:{rpc_password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {"Authorization": f"Basic {encoded_credentials}"}


def get_peer_info(config):
    """
    Fetches information about connected peers from the RPC server.

    Args:
        config (dict): Configuration parameters.
    """
    try:
        headers = get_auth_header(config)
        response = requests.get(f"{RPC_SERVER}/getpeerinfo", headers=headers, timeout=10)
        if response.status_code == 200:
            peers = response.json().get("peers", [])
            if peers:
                print("Peers List:")
                for peer in peers:
                    print(json.dumps(peer, indent=4))
            else:
                print("No connected peers found.")
        elif response.status_code == 401:
            logger.error("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            logger.error(f"Failed to fetch peers. Status Code: {response.status_code}")
            try:
                error = response.json().get('error', 'Unknown error')
            except json.JSONDecodeError:
                error = response.text
            logger.error(f"Error: {error}")
    except requests.Timeout:
        logger.error("Request timed out while trying to fetch peer information.")
    except requests.RequestException as e:
        logger.error(f"Error connecting to RPC server: {e}")


def send_transaction(config, receiver_address, amount):
    """
    Sends a transaction to the specified receiver address with the given amount.

    Args:
        config (dict): Configuration parameters.
        receiver_address (str): The recipient's wallet address.
        amount (float): The amount to send.
    """
    transaction = {
        "receiver_address": receiver_address,
        "amount": amount
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/transaction", json=transaction, headers=headers, timeout=10)
        if response.status_code == 200:
            print("Transaction submitted successfully.")
            transaction_hash = response.json().get("transaction_hash")
            if transaction_hash:
                print(f"Transaction Hash: {transaction_hash}")
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            logger.error(f"Bad Request: {error}")
        elif response.status_code == 401:
            logger.error("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            logger.error(f"Failed to submit transaction. Status Code: {response.status_code}")
            try:
                error = response.json().get('error', 'Unknown error')
            except json.JSONDecodeError:
                error = response.text
            logger.error(f"Error: {error}")
    except requests.Timeout:
        logger.error("Request timed out while trying to send transaction.")
    except requests.RequestException as e:
        logger.error(f"Error connecting to RPC server: {e}")


def add_node(config, addr, port):
    """
    Adds a new node to the network by specifying its address and port.

    Args:
        config (dict): Configuration parameters.
        addr (str): IP address of the node.
        port (int): Listening port of the node.
    """
    data = {
        "addr": addr,
        "listening_port": port
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/addnode", json=data, headers=headers, timeout=10)
        if response.status_code == 200:
            message = response.json().get("message", "Node added successfully.")
            print(message)
        elif response.status_code == 409:
            logger.warning("Peer already exists.")
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            logger.error(f"Bad Request: {error}")
        elif response.status_code == 401:
            logger.error("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            logger.error(f"Failed to add node. Status Code: {response.status_code}")
            try:
                error = response.json().get('error', 'Unknown error')
            except json.JSONDecodeError:
                error = response.text
            logger.error(f"Error: {error}")
    except requests.Timeout:
        logger.error("Request timed out while trying to add node.")
    except requests.RequestException as e:
        logger.error(f"Error connecting to RPC server: {e}")


def send_message(config, recipient_id, content):
    """
    Sends a message to a specific peer.

    Args:
        config (dict): Configuration parameters.
        recipient_id (str): The unique ID of the recipient peer.
        content (str): The message content.
    """
    data = {
        "recipient_id": recipient_id,
        "content": content
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers=headers, timeout=10)
        if response.status_code == 200:
            message = response.json().get("message", "Message sent successfully.")
            print(message)
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            logger.error(f"Bad Request: {error}")
        elif response.status_code == 401:
            logger.error("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            logger.error(f"Failed to send message. Status Code: {response.status_code}")
            try:
                error = response.json().get('error', 'Unknown error')
            except json.JSONDecodeError:
                error = response.text
            logger.error(f"Error: {error}")
    except requests.Timeout:
        logger.error("Request timed out while trying to send message.")
    except requests.RequestException as e:
        logger.error(f"Error connecting to RPC server: {e}")


def broadcast_message(config, content):
    """
    Broadcasts a message to all connected peers.

    Args:
        config (dict): Configuration parameters.
        content (str): The message content.
    """
    data = {
        "recipient_id": None,  # Indicates broadcast
        "content": content
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers=headers, timeout=10)
        if response.status_code == 200:
            message = response.json().get("message", "Broadcast message sent successfully.")
            print(message)
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            logger.error(f"Bad Request: {error}")
        elif response.status_code == 401:
            logger.error("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            logger.error(f"Failed to broadcast message. Status Code: {response.status_code}")
            try:
                error = response.json().get('error', 'Unknown error')
            except json.JSONDecodeError:
                error = response.text
            logger.error(f"Error: {error}")
    except requests.Timeout:
        logger.error("Request timed out while trying to broadcast message.")
    except requests.RequestException as e:
        logger.error(f"Error connecting to RPC server: {e}")


def get_messages(config):
    """
    Retrieves received messages from the RPC server.

    Args:
        config (dict): Configuration parameters.
    """
    try:
        headers = get_auth_header(config)
        response = requests.get(f"{RPC_SERVER}/getmessages", headers=headers, timeout=10)
        if response.status_code == 200:
            messages = response.json().get("messages", [])
            if messages:
                print("Received Messages:")
                for msg in messages:
                    timestamp_str = msg.get('timestamp', '')
                    sender = msg.get('sender_id', 'unknown')
                    content = msg.get('content', '')
                    # Attempt ISO 8601 parse or fallback to UNIX timestamp
                    timestamp = None
                    try:
                        timestamp = datetime.datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    except ValueError:
                        try:
                            timestamp = datetime.datetime.utcfromtimestamp(float(timestamp_str))
                        except (ValueError, TypeError):
                            timestamp = datetime.datetime.now()
                    timestamp_formatted = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"[{timestamp_formatted}] From {sender}: {content}")
            else:
                print("No messages received.")
        elif response.status_code == 401:
            logger.error("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            logger.error(f"Failed to fetch messages. Status Code: {response.status_code}")
            try:
                error = response.json().get('error', 'Unknown error')
            except json.JSONDecodeError:
                error = response.text
            logger.error(f"Error: {error}")
    except requests.Timeout:
        logger.error("Request timed out while trying to fetch messages.")
    except requests.RequestException as e:
        logger.error(f"Error connecting to RPC server: {e}")


def get_wallet_info(config):
    """
    Retrieves wallet address and balance information from the RPC server.

    Args:
        config (dict): Configuration parameters.
    """
    try:
        headers = get_auth_header(config)
        response = requests.get(f"{RPC_SERVER}/getwalletinfo", headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("Wallet Address:", data.get('address', 'N/A'))
            print("Balance:", data.get('balance', 'N/A'))
        elif response.status_code == 401:
            logger.error("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            logger.error(f"Failed to get wallet info. Status Code: {response.status_code}")
            try:
                error = response.json().get('error', 'Unknown error')
            except json.JSONDecodeError:
                error = response.text
            logger.error(f"Error: {error}")
    except requests.Timeout:
        logger.error("Request timed out while trying to get wallet info.")
    except requests.RequestException as e:
        logger.error(f"Error connecting to RPC server: {e}")


def sync_blockchain(config):
    """
    Initiates blockchain synchronization with the RPC server.

    Args:
        config (dict): Configuration parameters.
    """
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/sync", headers=headers, timeout=10)
        if response.status_code == 200:
            print("Blockchain synchronization initiated.")
        else:
            logger.error(f"Failed to initiate synchronization. Status Code: {response.status_code}")
            try:
                error = response.json().get('error', 'Unknown error')
            except json.JSONDecodeError:
                error = response.text
            logger.error(f"Error: {error}")
    except requests.Timeout:
        logger.error("Request timed out while trying to initiate blockchain synchronization.")
    except requests.RequestException as e:
        logger.error(f"Error connecting to RPC server: {e}")


def display_help():
    """
    Displays the help message with available commands and their usage.
    """
    help_message = """
Cronas P2P Network CLI
Usage:
    cli.py <command> [arguments]

Available Commands:
    getpeerinfo                                         - Get information about connected peers
    transaction <receiver_address> <amount>             - Send a transaction
    addnode <addr> <port>                               - Add a new node by IP address and port
    sendmessage <recipient_id> <message>                - Send a message to a specific peer
    broadcast <message>                                 - Broadcast a message to all peers
    getmessages                                         - Retrieve received messages
    getwalletinfo                                       - Get wallet address and balance
    sync                                                - Initiate blockchain synchronization
    help                                                - Display this help message

Examples:
    python3 cli.py getpeerinfo
    python3 cli.py transaction 1A2b3C4d5E6f7G8h9I0j 10.5
    python3 cli.py addnode 192.168.1.10 4333
    python3 cli.py sendmessage peer123 "Hello, peer!"
    python3 cli.py broadcast "Hello, everyone!"
    python3 cli.py getmessages
    python3 cli.py getwalletinfo
    python3 cli.py sync
"""
    print(help_message)


def main():
    """
    The main function that parses command-line arguments and executes the corresponding functions.
    """
    parser = argparse.ArgumentParser(description="Cronas P2P Network CLI", add_help=False)
    parser.add_argument('command', nargs='*', help='Command to run')
    args = parser.parse_args()

    if not args.command or args.command[0].lower() in ["help", "--help", "-h"]:
        display_help()
        return

    command = args.command[0].lower()
    config = read_config()

    if command == "getpeerinfo":
        get_peer_info(config)
    elif command == "transaction" and len(args.command) == 3:
        _, receiver_address, amount = args.command
        try:
            amount = float(amount)
            if amount <= 0:
                raise ValueError("Amount must be greater than zero.")
            send_transaction(config, receiver_address, amount)
        except ValueError as ve:
            logger.error(f"Invalid amount: {ve}")
            print("Usage: cli.py transaction <receiver_address> <amount>")
    elif command == "addnode" and len(args.command) == 3:
        _, addr, port = args.command
        try:
            port = int(port)
            if not (1 <= port <= 65535):
                raise ValueError("Port must be between 1 and 65535.")
            add_node(config, addr, port)
        except ValueError as ve:
            logger.error(f"Invalid port number: {ve}")
            print("Usage: cli.py addnode <addr> <port>")
    elif command == "sendmessage" and len(args.command) >= 3:
        _, recipient_id, *message_parts = args.command
        content = ' '.join(message_parts)
        if not content:
            logger.error("Message content cannot be empty.")
            print("Usage: cli.py sendmessage <recipient_id> <message>")
        else:
            send_message(config, recipient_id, content)
    elif command == "broadcast" and len(args.command) >= 2:
        _, *message_parts = args.command
        content = ' '.join(message_parts)
        if not content:
            logger.error("Broadcast message content cannot be empty.")
            print("Usage: cli.py broadcast <message>")
        else:
            broadcast_message(config, content)
    elif command == "getmessages":
        get_messages(config)
    elif command == "getwalletinfo":
        get_wallet_info(config)
    elif command == "sync":
        sync_blockchain(config)
    else:
        logger.error("Invalid command or arguments.")
        display_help()


if __name__ == '__main__':
    main()
