# cli.py

import requests
import sys
import json
import datetime
import base64
import os

RPC_SERVER = "http://localhost:4334"

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
            print(f"Failed to read the config file: {e}")
    return config

def get_auth_header(config):
    rpc_username = config.get('rpc_username')
    rpc_password = config.get('rpc_password')
    if not rpc_username or not rpc_password:
        print("RPC credentials not set in cronas.conf. Please set 'rpc_username' and 'rpc_password'.")
        sys.exit(1)
    credentials = f"{rpc_username}:{rpc_password}"
    credentials_bytes = credentials.encode('utf-8')
    encoded_credentials = base64.b64encode(credentials_bytes).decode('utf-8')
    return {"Authorization": f"Basic {encoded_credentials}"}

def get_peer_info(config):
    try:
        headers = get_auth_header(config)
        response = requests.get(f"{RPC_SERVER}/getpeerinfo", headers=headers)
        if response.status_code == 200:
            print("Peers List:")
            peers = response.json()
            print(json.dumps(peers, indent=4))
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf.")
        else:
            print(f"Failed to fetch peers. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def send_transaction(config, sender, receiver, amount):
    transaction = {
        "sender": sender,
        "receiver": receiver,
        "amount": amount
    }
    try:
        headers = get_auth_header(config)
        response = requests.post(f"{RPC_SERVER}/transaction", json=transaction, headers=headers)
        if response.status_code == 200:
            print("Transaction submitted successfully.")
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            print(f"Bad Request: {error}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf.")
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
        response = requests.post(f"{RPC_SERVER}/addnode", json=data, headers=headers)
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 409:
            print("Peer already exists.")
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            print(f"Bad Request: {error}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf.")
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
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers=headers)
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            print(f"Bad Request: {error}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf.")
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
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers=headers)
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 400:
            error = response.json().get('error', 'Unknown error')
            print(f"Bad Request: {error}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf.")
        else:
            print(f"Failed to broadcast message. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def get_messages(config):
    try:
        headers = get_auth_header(config)
        response = requests.get(f"{RPC_SERVER}/getmessages", headers=headers)
        if response.status_code == 200:
            if messages := response.json().get("messages", []):
                print("Received Messages:")
                for msg in messages:
                    timestamp = datetime.datetime.fromtimestamp(msg['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"[{timestamp}] From {msg['sender_id']}: {msg['content']}")
            else:
                print("No messages received.")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password in cronas.conf.")
        else:
            print(f"Failed to fetch messages. Status Code: {response.status_code}")
            error = response.json().get('error', 'Unknown error')
            print(f"Error: {error}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def display_help():
    print("Cronas P2P Network CLI")
    print("Usage:")
    print("  cli.py getpeerinfo                                         - Get information about connected peers")
    print("  cli.py transaction <sender> <receiver> <amount>           - Send a transaction")
    print("  cli.py addnode <addr> <port>                              - Add a new node by IP address and port")
    print("  cli.py sendmessage <recipient_id> <message>               - Send a message to a specific peer")
    print("  cli.py broadcast <message>                                - Broadcast a message to all peers")
    print("  cli.py getmessages                                        - Retrieve received messages")
    print("  cli.py help                                               - Display this help message")

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ["help", "--help"]:
        display_help()
        return

    command = sys.argv[1]

    # Read configuration from cronas.conf
    config = read_config()

    if command == "getpeerinfo":
        get_peer_info(config)
    elif command == "transaction" and len(sys.argv) == 5:
        _, _, sender, receiver, amount = sys.argv
        try:
            amount = float(amount)
            send_transaction(config, sender, receiver, amount)
        except ValueError:
            print("Invalid amount. Amount should be a number.")
    elif command == "addnode" and len(sys.argv) == 4:
        _, _, addr, port = sys.argv
        try:
            port = int(port)
            add_node(config, addr, port)
        except ValueError:
            print("Invalid port number. Port should be an integer.")
    elif command == "sendmessage" and len(sys.argv) >= 4:
        _, _, recipient_id, *message_parts = sys.argv
        content = ' '.join(message_parts)
        send_message(config, recipient_id, content)
    elif command == "broadcast" and len(sys.argv) >= 3:
        _, _, *message_parts = sys.argv
        content = ' '.join(message_parts)
        broadcast_message(config, content)
    elif command == "getmessages":
        get_messages(config)
    else:
        print("Invalid command or arguments. Use 'cli.py help' for usage.")

if __name__ == '__main__':
    main()
