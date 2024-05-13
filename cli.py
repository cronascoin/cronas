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

def add_node(ip):
    response = requests.post(f"{RPC_SERVER}/addnode", json={"ip": ip})
    if response.status_code == 200:
        print("Node added successfully.")
    else:
        print(f"Failed to add node: {response.text}")

def display_help():
    print("Usage:")
    print("  cli.py addnode <ip>          Add a new node by IP address")
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
    elif command == "addnode" and len(sys.argv) == 3:
        _, _, ip = sys.argv
        add_node(ip)
    else:
        print("Invalid command or arguments.")

if __name__ == '__main__':
    main()
