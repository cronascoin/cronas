#Copyright 2024 cronas.org
# cli.py

import requests
import sys
import json

RPC_SERVER = "http://localhost:4334"

def get_peer_info():
    response = requests.get(f"{RPC_SERVER}/getpeerinfo")
    if response.status_code == 200:
        print("Peers List:")
        peers = response.json()
        print(json.dumps(peers, indent=4))  # Output in JSON format with indentation
    else:
        print("Failed to fetch peers.")

def send_transaction(sender, receiver, amount):
    transaction = {
        "sender": sender,
        "receiver": receiver,
        "amount": amount
    }
    response = requests.post(f"{RPC_SERVER}/transaction", json=transaction)
    if response.status_code == 200:
        print("Transaction submitted successfully.")
    else:
        print("Failed to submit transaction.")

def add_node(addr, port):  # Simplified to take only address and port
    data = {
        "addr": addr,
        "listening_port": port
    }
    try:
        response = requests.post(f"{RPC_SERVER}/addnode", json=data)
        response.raise_for_status()
        print(response.json()["message"])  # Display the server's response message
    except requests.RequestException as e:
        print(f"Error adding node: {e}")

def display_help():
    print("Cronas P2P Network CLI")
    print("Usage:")
    print("  cli.py getpeerinfo               - Get information about connected peers")
    print("  cli.py transaction <sender> <receiver> <amount>   - Send a transaction")
    print("  cli.py addnode <addr> <port> - Add a new node by IP address and port")
    print("  cli.py help                    - Display this help message")

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ["help", "--help"]:
        display_help()
        return

    command = sys.argv[1]

    if command == "getpeerinfo":
        get_peer_info()
    elif command == "transaction" and len(sys.argv) == 5:
        _, _, sender, receiver, amount = sys.argv
        send_transaction(sender, receiver, float(amount))  # Convert amount to float
    elif command == "addnode" and len(sys.argv) == 4:  # Check for correct number of arguments
        _, _, addr, port = sys.argv
        try:
            port = int(port)  # Validate port is an integer
            add_node(addr, port)
        except ValueError:
            print("Invalid port number. Port should be an integer.")
    else:
        print("Invalid command or arguments. Use 'cli.py help' for usage.")

if __name__ == '__main__':
    main()