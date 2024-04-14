import socket
import signal
import sys
import select

SERVER = "137.184.80.215"
PORT = 4334
NICKNAME = "draggdasry"
CHANNEL = "#Cronas"
REALNAME = "bill"

def send_message(sock, message):
    sock.send((message + "\r\n").encode("utf-8"))

def join_channel(sock, channel):
    send_message(sock, "JOIN " + channel)

def send_private_message(sock, recipient, message):
    send_message(sock, "PRIVMSG " + recipient + " :" + message)

def graceful_exit(signum, frame):
    print("\nDisconnecting from server...")
    send_message(irc_socket, "QUIT :Client exited")
    irc_socket.close()
    print("Disconnected.")
    sys.exit(0)

def main():
    global irc_socket
    irc_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    irc_socket.connect((SERVER, PORT))
    send_message(irc_socket, "NICK " + NICKNAME)
    send_message(irc_socket, "USER " + NICKNAME + " 0 * :" + REALNAME)
    join_channel(irc_socket, CHANNEL)

    signal.signal(signal.SIGINT, graceful_exit)

    while True:
        ready_to_read, _, _ = select.select([irc_socket], [], [], 0.5)
        if ready_to_read:
            response = irc_socket.recv(2048).decode("utf-8")
            print(response)

            if "PING" in response:
                ping_token = response.split()[1] if len(response.split()) > 1 else None
                if ping_token:
                    send_message(irc_socket, "PONG " + ping_token)
            elif "PRIVMSG" in response:
                sender = response.split('!', 1)[0][1:]
                message = response.split('PRIVMSG', 1)[1].split(':', 1)[1]
                print(sender + ": " + message)
                if "hello" in message.lower():
                    send_private_message(irc_socket, CHANNEL, "Hello, " + sender + "!")

if __name__ == "__main__":
    main()
