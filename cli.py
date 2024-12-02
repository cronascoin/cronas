# cli.py

import requests
import sys
import json
import datetime
import base64
import os
import logging
import argparse

RPC_SERVER = "https://localhost:4334"  # Updated to use HTTPS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def read_config(config_path='cronas.conf'):
    config = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as configfile:
                for line in configfile:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        if key == 'addnode':
                            config.setdefault(key, []).append(value)
                        else:
                            config[key] = value
        except Exception as e:
            print(f"Failed to read the config file: {e}")
    return config

def get_auth_header(config):
    rpc_username = os.getenv('RPC_USERNAME', config.get('rpc_username'))
    rpc_password = os.getenv('RPC_PASSWORD', config.get('rpc_password'))
    if not rpc_username or not rpc_password:
        print("RPC credentials not set in cronas.conf or environment variables. Please set 'rpc_username' and 'rpc_password'.")
        sys.exit(1)
    credentials = f"{rpc_username}:{rpc_password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {"Authorization": f"Basic {encoded_credentials}"}

def get_peer_info(config):
    try:
        headers = get_auth_header(config)
        response = requests.get(f"{RPC_SERVER}/getpeerinfo", headers=headers, verify='peer_certificate.crt')
        if response.status_code == 200:
            peers = response.json().get("peers", [])
            print("Peers List:")
            for peer in peers:
                print(json.dumps(peer, indent=4))
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            print(f"Failed to fetch peers. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def send_transaction(config, receiver_address, amount):
    transaction = {
        "receiver_address": receiver_address,
        "amount": amount
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/transaction", json=transaction, headers=headers, verify='peer_certificate.crt')
        if response.status_code == 200:
            print("Transaction submitted successfully.")
            if transaction_hash := response.json().get("transaction_hash"):
                print(f"Transaction Hash: {transaction_hash}")
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            print(f"Bad Request: {error}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            print(f"Failed to submit transaction. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def add_node(config, addr, port):
    data = {
        "addr": addr,
        "listening_port": port
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/addnode", json=data, headers=headers, verify='peer_certificate.crt')
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 409:
            print("Peer already exists.")
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            print(f"Bad Request: {error}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            print(f"Failed to add node. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def send_message(config, recipient_id, content):
    data = {
        "recipient_id": recipient_id,
        "content": content
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers=headers, verify='peer_certificate.crt')
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            print(f"Bad Request: {error}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            print(f"Failed to send message. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def broadcast_message(config, content):
    data = {
        "recipient_id": None,  # Indicates broadcast
        "content": content
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers=headers, verify='peer_certificate.crt')
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            print(f"Bad Request: {error}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            print(f"Failed to broadcast message. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def get_messages(config):
    try:
        headers = get_auth_header(config)
        response = requests.get(f"{RPC_SERVER}/getmessages", headers=headers, verify='peer_certificate.crt')
        if response.status_code == 200:
            if messages := response.json().get("messages", []):
                print("Received Messages:")
                for msg in messages:
                    timestamp_str = msg.get('timestamp', '')
                    try:
                        # Attempt to parse ISO format
                        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    except ValueError:
                        # Fallback to UNIX timestamp
                        timestamp = datetime.utcfromtimestamp(float(timestamp_str))
                    timestamp_formatted = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    sender = msg.get('sender_id', 'unknown')
                    content = msg.get('content', '')
                    print(f"[{timestamp_formatted}] From {sender}: {content}")
            else:
                print("No messages received.")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            print(f"Failed to fetch messages. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def get_wallet_info(config):
    try:
        headers = get_auth_header(config)
        response = requests.get(f"{RPC_SERVER}/getwalletinfo", headers=headers, verify='peer_certificate.crt')
        if response.status_code == 200:
            data = response.json()
            print("Wallet Address:", data.get('address'))
            print("Balance:", data.get('balance'))
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf or environment variables.")
        else:
            print(f"Failed to get wallet info. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def sync_blockchain(config):
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/sync", headers=headers, verify='peer_certificate.crt')
        if response.status_code == 200:
            print("Blockchain synchronization initiated.")
        else:
            print(f"Failed to initiate synchronization. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def display_help():
    print("Cronas P2P Network CLI")
    print("Usage:")
    print("  cli.py getpeerinfo                                         - Get information about connected peers")
    print("  cli.py transaction <receiver_address> <amount>             - Send a transaction")
    print("  cli.py addnode <addr> <port>                               - Add a new node by IP address and port")
    print("  cli.py sendmessage <recipient_id> <message>                - Send a message to a specific peer")
    print("  cli.py broadcast <message>                                 - Broadcast a message to all peers")
    print("  cli.py getmessages                                         - Retrieve received messages")
    print("  cli.py getwalletinfo                                       - Get wallet address and balance")
    print("  cli.py sync                                                - Initiate blockchain synchronization")
    print("  cli.py help                                                - Display this help message")

def main():
    parser = argparse.ArgumentParser(description="Cronas P2P Network CLI", add_help=False)
    parser.add_argument('command', nargs='*', help='Command to run')
    args = parser.parse_args()

    if not args.command or args.command[0] in ["help", "--help"]:
        display_help()
        return

    command = args.command[0]
    config = read_config()

    if command == "getpeerinfo":
        get_peer_info(config)
    elif command == "transaction" and len(args.command) == 3:
        _, receiver_address, amount = args.command
        try:
            amount = float(amount)
            send_transaction(config, receiver_address, amount)
        except ValueError:
            print("Invalid amount. Amount should be a number.")
    elif command == "addnode" and len(args.command) == 3:
        _, addr, port = args.command
        try:
            port = int(port)
            add_node(config, addr, port)
        except ValueError:
            print("Invalid port number. Port should be an integer.")
    elif command == "sendmessage" and len(args.command) >= 3:
        _, recipient_id, *message_parts = args.command
        content = ' '.join(message_parts)
        send_message(config, recipient_id, content)
    elif command == "broadcast" and len(args.command) >= 2:
        _, *message_parts = args.command
        content = ' '.join(message_parts)
        broadcast_message(config, content)
    elif command == "getmessages":
        get_messages(config)
    elif command == "getwalletinfo":
        get_wallet_info(config)
    elif command == "sync":
        sync_blockchain(config)
    else:
        print("Invalid command or arguments. Use 'cli.py help' for usage.")

if __name__ == '__main__':
    main()
