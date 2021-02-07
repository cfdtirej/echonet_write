import socket
import time

# ip_con = "192.168.30.30"
ip_con = "127.0.0.1"
ip_102 = "172.24.7.226"
ip_101 = "172.24.7.223"
port = 3610
msg_b7 = b'\x10\x81\x00\x00\x05\xFF\x01\x02\x87\x01\x62\x01\xB7\x00'
set_ch_telegram = b'\x10\x81\x00\x00\x05\xFF\x01\x02\x87\x01\x61\x01\xB6\x02\x01\x10'
interval = 0.05

# def send_telegram(ip_con, ip_el, port, msg):
#     sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#     # sock.sendto(set_ch_telegram, (ip_102, port))
#     start_time = time.time()
#     sock.sendto(str(start_time).encode(), (ip_con, port))
#     # sock.close()
#     time.sleep(0.05)
#     # sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#     sock.sendto(msg_b7, (ip_102, port))
#     time.sleep(0.05)
#     sock.sendto(msg_b7, (ip_101, port))
#     sock.close()

#     return None

while True:
    # send_telegram(ip_con, ip_102, port, msg_b7)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # sock.sendto(set_ch_telegram, (ip_102, port))
    start_time = time.time()
    sock.sendto(str(start_time).encode(), (ip_con, port))
    # sock.close()
    time.sleep(interval)
    # sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(msg_b7, (ip_101, port))
    time.sleep(interval)
    sock.sendto(msg_b7, (ip_102, port))
    sock.close()
    time.sleep(2.0 - interval*2)