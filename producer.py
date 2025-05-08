# producer.py
import socket
import json
import time

from earthquakes import fetch_earthquakes
from fires import fetch_fires

HOST = 'localhost'
PORT = 9999

from datetime import datetime
import requests


def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(1)
        print(f"[Socket] Listening on {HOST}:{PORT}")
        conn, addr = s.accept()
        print(f"[Socket] Connected by {addr}")
        
        with conn:
            while True:
                print("[Socket] Sending earthquake data...")
                quakes = fetch_earthquakes()
                for event in quakes:
                    json_data = json.dumps(event)
                    conn.sendall((json_data + "\n").encode("utf-8"))
                    print(f"[Socket] Sent: {json_data}")

                print("[Socket] Sending fire data...")
                fires = fetch_fires()
                for fire in fires:
                    json_data = json.dumps(fire)
                    conn.sendall((json_data + "\n").encode("utf-8"))
                    print(f"[Socket] Sent: {json_data}")
                time.sleep(60)

if __name__ == "__main__":
    start_server()
