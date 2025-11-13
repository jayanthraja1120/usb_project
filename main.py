#!/usr/bin/env python3
import os
import shutil
import socket
import pandas as pd
import time

# ======== CONFIGURATION ========
USB_PATH = "/media/pi/USB DEVICE"
DEST_PATH = "/home/pi/usb_project/usb_data.csv"

SERVER_HOST = "192.168.50.2"
SERVER_PORT = 1024

HEADER = "STM:1:1::1"
FOOTER = ":"
FIRST_TAG = b'\x02'
END_TAG = b'\x03'


# ======== FUNCTION 1: CHECK USB ========
def check_usb():
    """Check if USB pendrive is inserted and mounted."""
    if os.path.ismount(USB_PATH):
        print(f"[USB] Pendrive detected at: {USB_PATH}")
        return True
    else:
        print("[USB] No pendrive detected.")
        return False


# ======== FUNCTION 2: COPY USB TO LOCAL ========
def copy_usb_to_local():
    """Copy the first CSV file found in the USB drive to local directory."""
    for root, _, files in os.walk(USB_PATH):
        for file in files:
            if file.lower().endswith(".csv"):
                src = os.path.join(root, file)
                shutil.copy(src, DEST_PATH)
                print(f"[COPY] Copied {file} → {DEST_PATH}")
                return True
    print("[COPY] No CSV file found in USB.")
    return False


# ======== FUNCTION 3: SEND TCP ========
def send_tcp():
    """Send CSV data line-by-line.
       Send each line ONCE, then wait up to 5 minutes for ACK='OK' from server.
       No retransmissions allowed."""

    if not os.path.exists(DEST_PATH):
        print(f"[TCP] No CSV found at {DEST_PATH}")
        return False

    # ---- READ CSV ----
    try:
        df = pd.read_csv(DEST_PATH, header=None)
        lines = df[0].dropna().astype(str).tolist()
        print(f"[TCP] Loaded {len(lines)} lines.")
    except Exception as e:
        print(f"[TCP] CSV Read Error: {e}")
        return False

    # ---- CONNECT ----
    try:
        sock = socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=5)
        sock.settimeout(3)
        print(f"[TCP] Connected to {SERVER_HOST}:{SERVER_PORT}")
    except Exception as e:
        print(f"[TCP] Connect Error: {e}")
        return False

    # ---- SEND & WAIT FOR ACK=OK ----
    for index, text in enumerate(lines):

        msg_str = f"{HEADER}{text}{FOOTER}"
        packet = FIRST_TAG + msg_str.encode() + END_TAG

        print(f"\n[TCP] Sending line {index+1}: {text}")
        print(f"[TCP] Packet: {packet}")

        # Send only once
        try:
            sock.sendall(packet)
            print("[TCP] Packet sent. Waiting for ACK='OK'...")
        except Exception as e:
            print(f"[TCP] Send error: {e}")
            sock.close()
            return False

        # ---- WAIT FOR ACK='OK' (5 minutes max) ----
        ack_received = False
        start_time = time.time()

        while (time.time() - start_time) < 300:  # 5 minutes

            try:
                incoming = sock.recv(1024)
            except socket.timeout:
                print("[TCP] Waiting for ACK...")
                continue
            except Exception as e:
                print(f"[TCP] Error while waiting for ACK: {e}")
                break

            if not incoming:
                print("[TCP] Connection closed by server.")
                break

            incoming_str = incoming.decode(errors="ignore").strip()

            # ---- ACCEPT ACK ONLY IF EXACTLY "OK" ----
            if incoming_str == "OK":
                print("[TCP] ACK RECEIVED ✔ (OK)")
                ack_received = True
                break
            else:
                print(f"[TCP] Ignored non-ACK message: {incoming_str}")

        if not ack_received:
            print("[TCP] ERROR: No valid ACK='OK' received in 5 minutes. Stopping.")
            sock.close()
            return False

    sock.close()
    print("\n[TCP] All lines sent successfully. Connection closed.")
    return True


# ======== MAIN FUNCTION ========
def main():
    if check_usb():
        if copy_usb_to_local():
            print("[SYSTEM] CSV copied successfully. Sending to TCP server...")
            #send_tcp()
        else:
            print("[SYSTEM] CSV not found on USB.")
    else:
        print("[SYSTEM] USB not detected. Checking for local CSV...")
        if os.path.exists(DEST_PATH):
            send_tcp()
        else:
            print("[SYSTEM] No local CSV file available to send.")


# ======== ENTRY POINT ========
if __name__ == "__main__":
    main()

