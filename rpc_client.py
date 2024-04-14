import socket
import json

def call_rpc(method, params):
    request = json.dumps({"method": method, "params": params})
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(('localhost', 4333))  # Ensure this matches your RPC server's host and port
        sock.sendall(request.encode())
        response = json.loads(sock.recv(1024).decode())
    return response

def main():
    # Example call to list_peers RPC method
    peers_response = call_rpc("list_peers", [])
    print(f"Peers Response: {peers_response}")

if __name__ == "__main__":
    main()
