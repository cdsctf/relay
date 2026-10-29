"""Local TCP/UDP echo targets for the Rust probe example."""

import socket
import threading


def handle_tcp(connection):
    with connection:
        while data := connection.recv(65535):
            connection.sendall(data)


def tcp_echo():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 9000))
        server.listen()
        while True:
            connection, _ = server.accept()
            threading.Thread(target=handle_tcp, args=(connection,), daemon=True).start()


def udp_echo():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
        server.bind(("127.0.0.1", 9000))
        while True:
            data, peer = server.recvfrom(65535)
            server.sendto(data, peer)


if __name__ == "__main__":
    threading.Thread(target=udp_echo, daemon=True).start()
    print("TCP and UDP echo on 127.0.0.1:9000", flush=True)
    tcp_echo()
