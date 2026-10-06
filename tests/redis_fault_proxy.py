import os
import select
import socket
import threading


REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
PROXY_PORT = int(os.environ.get("PROXY_PORT", "6379"))
STATS_PORT = int(os.environ.get("STATS_PORT", "9091"))
FAULT_ON_SUCCESSFUL_EXEC_NUMBER = int(
    os.environ.get("FAULT_ON_SUCCESSFUL_EXEC_NUMBER", "2")
)

LOCK = threading.Lock()
STATS = {
    "accepted_connections": 0,
    "multi_exec_attempts": 0,
    "successful_execs": 0,
    "dropped_exec_replies": 0,
    "drop_publish_count": 0,
    "execs_after_drop": 0,
    "publishes_after_drop": 0,
}


def recv_exact(sock, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise EOFError("socket closed during RESP frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_line(sock):
    data = bytearray()
    while not data.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise EOFError("socket closed during RESP line")
        data.extend(chunk)
    return bytes(data)


def read_resp_frame(sock):
    marker = recv_exact(sock, 1)
    line = recv_line(sock)
    raw = marker + line
    value_line = line[:-2]

    if marker == b"+":
        return raw, value_line
    if marker == b"-":
        return raw, ("error", value_line)
    if marker == b":":
        return raw, int(value_line)
    if marker == b"$":
        length = int(value_line)
        if length == -1:
            return raw, None
        body = recv_exact(sock, length + 2)
        if not body.endswith(b"\r\n"):
            raise RuntimeError("invalid RESP bulk terminator")
        return raw + body, body[:-2]
    if marker == b"*":
        count = int(value_line)
        if count == -1:
            return raw, None
        parts = []
        for _ in range(count):
            item_raw, item = read_resp_frame(sock)
            raw += item_raw
            parts.append(item)
        return raw, parts
    raise RuntimeError(f"unsupported RESP marker {marker!r}")


def relay_bidirectionally(client, upstream):
    peers = (client, upstream)
    while True:
        readable, _, _ = select.select(peers, [], [], 1.0)
        for source in readable:
            target = upstream if source is client else client
            data = source.recv(65536)
            if not data:
                return
            target.sendall(data)


def connection_worker(client):
    upstream = None
    try:
        client.settimeout(None)
        upstream = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=5)
        upstream.settimeout(None)
        with LOCK:
            STATS["accepted_connections"] += 1

        in_multi = False
        transaction_publish_count = 0
        while True:
            raw_command, args = read_resp_frame(client)
            if not isinstance(args, list) or not args or not isinstance(args[0], bytes):
                raise RuntimeError("expected a RESP array command")
            command = args[0].upper()

            upstream.sendall(raw_command)
            if command in (b"SUBSCRIBE", b"PSUBSCRIBE", b"SSUBSCRIBE"):
                relay_bidirectionally(client, upstream)
                return

            raw_reply, reply = read_resp_frame(upstream)
            if command == b"MULTI":
                in_multi = True
                transaction_publish_count = 0
            elif command == b"PUBLISH" and in_multi:
                transaction_publish_count += 1
                with LOCK:
                    if STATS["dropped_exec_replies"]:
                        STATS["publishes_after_drop"] += 1

            should_drop_reply = False
            if command == b"EXEC":
                # Consume the complete EXEC reply before closing the app-side socket.
                successful_exec = isinstance(reply, list)
                with LOCK:
                    STATS["multi_exec_attempts"] += 1
                    if successful_exec:
                        STATS["successful_execs"] += 1
                    if STATS["dropped_exec_replies"]:
                        STATS["execs_after_drop"] += 1
                    elif (
                        successful_exec
                        and STATS["successful_execs"]
                        == FAULT_ON_SUCCESSFUL_EXEC_NUMBER
                    ):
                        STATS["dropped_exec_replies"] = 1
                        STATS["drop_publish_count"] = transaction_publish_count
                        should_drop_reply = True
                in_multi = False
                transaction_publish_count = 0

            if should_drop_reply:
                return
            client.sendall(raw_reply)
    except (EOFError, OSError):
        pass
    finally:
        for sock in (client, upstream):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass


def stats_snapshot():
    with LOCK:
        return dict(STATS)


def stats_worker(client):
    try:
        client.settimeout(3)
        request = recv_line(client).strip().upper()
        if request != b"STATS":
            client.sendall(b"error=expected STATS\n\n")
            return
        lines = [f"{key}={value}" for key, value in stats_snapshot().items()]
        client.sendall(("\n".join(lines) + "\n\n").encode("ascii"))
    except (EOFError, OSError):
        pass
    finally:
        client.close()


def serve(listener, handler):
    while True:
        client, _ = listener.accept()
        threading.Thread(target=handler, args=(client,), daemon=True).start()


def main():
    redis_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    redis_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    redis_listener.bind(("0.0.0.0", PROXY_PORT))
    redis_listener.listen()

    stats_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stats_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    stats_listener.bind(("0.0.0.0", STATS_PORT))
    stats_listener.listen()

    threading.Thread(target=serve, args=(stats_listener, stats_worker), daemon=True).start()
    serve(redis_listener, connection_worker)


if __name__ == "__main__":
    main()
