#Copyright 2024 cronas.org
# cli.py

import requests
import sys
import json

RPC_SERVER = "http://localhost:4334"

def get_peers():
    response = requests.get(f"{RPC_SERVER}/peers")
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

def add_node(addr, addrlocal, addrbind, server_id, version):
    data = {
        "addr": addr,
        "addrlocal": addrlocal,
        "addrbind": addrbind,
        "server_id": server_id,
        "version": version
    }
    response = requests.post(f"{RPC_SERVER}/addnode", json=data)
    if response.status_code == 200:
        print("Node added successfully.")
    else:
        print(f"Failed to add node: {response.text}")

def display_help():
    print("Usage:")
    print("  cli.py addnode <addr> <addrlocal> <addrbind> <server_id> <version>  Add a new node")
    print("  cli.py peers                 Retrieve and display the list of peers in JSON format")
    print("  cli.py transaction <sender> <receiver> <amount>  Send a transaction")
    print("  cli.py help                  Display this help message")
    print("  cli.py --help                Display this help message")

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ["help", "--help"]:
        display_help()
        return

    command = sys.argv[1]

    if command == "peers":
        get_peers()
    elif command == "transaction" and len(sys.argv) == 5:
        _, _, sender, receiver, amount = sys.argv
        send_transaction(sender, receiver, amount)
    elif command == "addnode" and len(sys.argv) == 7:
        _, _, addr, addrlocal, addrbind, server_id, version = sys.argv
        add_node(addr, addrlocal, addrbind, server_id, version)
    else:
        print("Invalid command or arguments.")

if __name__ == '__main__':
    main()
