import requests
import sys
import json
import datetime

RPC_SERVER = "http://localhost:4334"

def get_peer_info():
    try:
        response = requests.get(f"{RPC_SERVER}/getpeerinfo", headers={"Authorization": "Basic your_encoded_credentials"})
        if response.status_code == 200:
            print("Peers List:")
            peers = response.json()
            print(json.dumps(peers, indent=4))
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC password.")
        else:
            print(f"Failed to fetch peers. Status Code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def send_transaction(sender, receiver, amount):
    transaction = {
        "sender": sender,
        "receiver": receiver,
        "amount": amount
    }
    try:
        response = requests.post(f"{RPC_SERVER}/transaction", json=transaction, headers={"Authorization": "Basic your_encoded_credentials"})
        if response.status_code == 200:
            print("Transaction submitted successfully.")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC password.")
        else:
            print(f"Failed to submit transaction. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def add_node(addr, port):
    data = {
        "addr": addr,
        "listening_port": port
    }
    try:
        response = requests.post(f"{RPC_SERVER}/addnode", json=data, headers={"Authorization": "Basic your_encoded_credentials"})
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 409:
            print("Peer already exists.")
        elif response.status_code == 400:
            print(f"Bad Request: {response.json().get('error', 'Unknown error')}")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC password.")
        else:
            print(f"Failed to add node. Status Code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def send_message(recipient_id, content):
    data = {
        "recipient_id": recipient_id,
        "content": content
    }
    try:
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers={"Authorization": "Basic your_encoded_credentials"})
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC password.")
        else:
            print(f"Failed to send message. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def broadcast_message(content):
    data = {
        "recipient_id": None,  # Indicates broadcast
        "content": content
    }
    try:
        response = requests.post(f"{RPC_SERVER}/sendmessage", json=data, headers={"Authorization": "Basic your_encoded_credentials"})
        if response.status_code == 200:
            print(response.json()["message"])
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC password.")
        else:
            print(f"Failed to broadcast message. Status Code: {response.status_code}")
            print(f"Error: {response.json().get('error', 'Unknown error')}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def get_messages():
    try:
        response = requests.get(f"{RPC_SERVER}/getmessages", headers={"Authorization": "Basic your_encoded_credentials"})
        if response.status_code == 200:
            if messages := response.json().get("messages", []):
                print("Received Messages:")
                for msg in messages:
                    timestamp = datetime.datetime.fromtimestamp(msg['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"[{timestamp}] From {msg['sender_id']}: {msg['content']}")
            else:
                print("No messages received.")
        elif response.status_code == 401:
            print("Unauthorized access. Check your RPC password.")
        else:
            print(f"Failed to fetch messages. Status Code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error connecting to RPC server: {e}")

def display_help():
    print("Cronas P2P Network CLI")
    print("Usage:")
    print("  cli.py getpeerinfo                                         - Get information about connected peers")
    print("  cli.py transaction <sender> <receiver> <amount>           - Send a transaction")
    print("  cli.py addnode <addr> <port>                              - Add a new node by IP address and port")
    print("  cli.py sendmessage <recipient_id> <message>               - Send a message to a specific peer")
    print("  cli.py broadcast <message>                                 - Broadcast a message to all peers")
    print("  cli.py getmessages                                         - Retrieve received messages")
    print("  cli.py help                                                - Display this help message")

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ["help", "--help"]:
        display_help()
        return

    command = sys.argv[1]

    if command == "getpeerinfo":
        get_peer_info()
    elif command == "transaction" and len(sys.argv) == 5:
        _, _, sender, receiver, amount = sys.argv
        try:
            amount = float(amount)
            send_transaction(sender, receiver, amount)
        except ValueError:
            print("Invalid amount. Amount should be a number.")
    elif command == "addnode" and len(sys.argv) == 4:
        _, _, addr, port = sys.argv
        try:
            port = int(port)
            add_node(addr, port)
        except ValueError:
            print("Invalid port number. Port should be an integer.")
    elif command == "sendmessage" and len(sys.argv) >= 4:
        _, _, recipient_id, *message_parts = sys.argv
        content = ' '.join(message_parts)
        send_message(recipient_id, content)
    elif command == "broadcast" and len(sys.argv) >= 3:
        _, _, *message_parts = sys.argv
        content = ' '.join(message_parts)
        broadcast_message(content)
    elif command == "getmessages":
        get_messages()
    else:
        print("Invalid command or arguments. Use 'cli.py help' for usage.")

if __name__ == '__main__':
    main()
