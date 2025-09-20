#!/usr/bin/env python3
# v2x_multicast_node.py
import socket, struct, json, time, argparse, threading, uuid

parser = argparse.ArgumentParser()
parser.add_argument("--id", default=None, help="node id (default: random)")
parser.add_argument("--group", default="239.255.0.1", help="multicast group")
parser.add_argument("--port", type=int, default=5007, help="UDP port")
parser.add_argument("--rate", type=float, default=5.0, help="messages per second")
args = parser.parse_args()

NODE_ID = args.id if args.id else f"node-{uuid.uuid4().hex[:6]}"
MCAST_GRP = args.group
PORT = args.port
RATE = args.rate
INTERVAL = 1.0 / RATE

# socket to receive multicast
recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    recv_sock.bind(('', PORT))
except OSError:
    recv_sock.bind(('0.0.0.0', PORT))

mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

# socket to send multicast
send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
# Optional: set TTL so multicast stays in local network
send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

print(f"[{NODE_ID}] multicast node starting, group={MCAST_GRP}:{PORT}, rate={RATE} msg/s")

def recv_loop():
    while True:
        try:
            data, addr = recv_sock.recvfrom(8192)
            try:
                msg = json.loads(data.decode('utf-8'))
            except Exception:
                print(f"[{NODE_ID}] Received non-JSON from {addr}")
                continue
            # ignore our own messages if we want (they'll come back on multicast)
            if msg.get("id") == NODE_ID:
                continue
            # handle message (replace with OVERTON ingestion hook)
            print(time.strftime("%Y-%m-%d %H:%M:%S"), f"RX from {addr}: ", msg)
        except Exception as e:
            print(f"[{NODE_ID}] recv error:", e)

def send_loop():
    seq = 0
    while True:
        bsm = {
            "id": NODE_ID,
            "seq": seq,
            "ts": time.time(),
            # Example changing lat/lon for testing; replace with real GPS if available
            "lat": 38.895 + 0.0001 * ((seq) % 10),
            "lon": -77.036 + 0.0001 * ((seq) % 7),
            "speed": 5.0 + (seq % 5)
        }
        try:
            send_sock.sendto(json.dumps(bsm).encode('utf-8'), (MCAST_GRP, PORT))
        except Exception as e:
            print(f"[{NODE_ID}] send error:", e)
        seq += 1
        time.sleep(INTERVAL)

t1 = threading.Thread(target=recv_loop, daemon=True)
t2 = threading.Thread(target=send_loop, daemon=True)
t1.start()
t2.start()
# keep main alive
while True:
    time.sleep(60)
