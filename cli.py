# cli.py

import requests
import sys
import json
import datetime
import base64
import os

RPC_SERVER = "http://localhost:4334"

def get_auth_header(username, password):
    credentials = f"{username}:{password}"
    credentials_bytes = credentials.encode('utf-8')
    encoded_credentials = base64.b64encode(credentials_bytes).decode('utf-8')
    return {"Authorization": f"Basic {encoded_credentials}"}

def get_peer_info(rpc_username, rpc_password):
    try:
        headers = get_auth_header(rpc_username, rpc_password)
        response = requests.get(f"{RPC_SERVER}/getpeerinfo", headers=headers)
        if response.status_code == 200:
            print("Peers List:")
            peers = response.json()
            print(json.dumps(peers, indent=4))
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password.")
        else:
            print(f"Failed to fetch peers. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def send_transaction(rpc_username, rpc_password, sender, receiver, amount):
    transaction = {
        "sender": sender,
        "receiver": receiver,
        "amount": amount
    }
    try:
        headers = get_auth_header(rpc_username, rpc_password)
        response = requests.post(f"{RPC_SERVER}/transaction", json=transaction, headers=headers)
        if response.status_code == 200:
            print("Transaction submitted successfully.")
        elif response.status_code == 400:
            print(f"Bad Request: {response.json().get('error', 'Unknown error')}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password.")
        else:
            print(f"Failed to submit transaction. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def add_node(rpc_username, rpc_password, addr, port):
    data = {
        "addr": addr,
        "listening_port": port
    }
    try:
        headers = get_auth_header(rpc_username, rpc_password)
        response = requests.post(f"{RPC_SERVER}/addnode", json=data, headers=headers)
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 409:
            print("Peer already exists.")
        elif response.status_code == 400:
            print(f"Bad Request: {response.json().get('error', 'Unknown error')}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password.")
        else:
            print(f"Failed to add node. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def send_message(rpc_username, rpc_password, recipient_id, content):
    data = {
        "recipient_id": recipient_id,
        "content": content
    }
    try:
        headers = get_auth_header(rpc_username, rpc_password)
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers=headers)
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 400:
            print(f"Bad Request: {response.json().get('error', 'Unknown error')}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password.")
        else:
            print(f"Failed to send message. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def broadcast_message(rpc_username, rpc_password, content):
    data = {
        "recipient_id": None,  # Indicates broadcast
        "content": content
    }
    try:
        headers = get_auth_header(rpc_username, rpc_password)
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers=headers)
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 400:
            print(f"Bad Request: {response.json().get('error', 'Unknown error')}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC username and password.")
        else:
            print(f"Failed to broadcast message. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def get_messages(rpc_username, rpc_password):
    try:
        headers = get_auth_header(rpc_username, rpc_password)
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
            print("Unauthorized access. Check your RPC username and password.")
        else:
            print(f"Failed to fetch messages. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
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

    # Fetch RPC credentials from environment variables for security
    rpc_username = os.getenv('CRONAS_RPC_USERNAME')
    rpc_password = os.getenv('CRONAS_RPC_PASSWORD')

    if not rpc_username or not rpc_password:
        print("RPC credentials not set. Please set CRONAS_RPC_USERNAME and CRONAS_RPC_PASSWORD environment variables.")
        return

    if command == "getpeerinfo":
        get_peer_info(rpc_username, rpc_password)
    elif command == "transaction" and len(sys.argv) == 5:
        _, _, sender, receiver, amount = sys.argv
        try:
            amount = float(amount)
            send_transaction(rpc_username, rpc_password, sender, receiver, amount)
        except ValueError:
            print("Invalid amount. Amount should be a number.")
    elif command == "addnode" and len(sys.argv) == 4:
        _, _, addr, port = sys.argv
        try:
            port = int(port)
            add_node(rpc_username, rpc_password, addr, port)
        except ValueError:
            print("Invalid port number. Port should be an integer.")
    elif command == "sendmessage" and len(sys.argv) >= 4:
        _, _, recipient_id, *message_parts = sys.argv
        content = ' '.join(message_parts)
        send_message(rpc_username, rpc_password, recipient_id, content)
    elif command == "broadcast" and len(sys.argv) >= 3:
        _, _, *message_parts = sys.argv
        content = ' '.join(message_parts)
        broadcast_message(rpc_username, rpc_password, content)
    elif command == "getmessages":
        get_messages(rpc_username, rpc_password)
    else:
        print("Invalid command or arguments. Use 'cli.py help' for usage.")

if __name__ == '__main__':
    main()
