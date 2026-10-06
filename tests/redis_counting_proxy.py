import copy
import json
import os
import select
import socket
import threading


REDIS_HOST = os.environ.get("REDIS_HOST", "redis-protocol")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
PROXY_PORT = int(os.environ.get("PROXY_PORT", "6379"))
STATS_PORT = int(os.environ.get("STATS_PORT", "9091"))
OUTPUT_NAMES = ("chunked", "send", "truncate", "drop", "immediate")
DATABASE_OUTPUTS = {str(index + 1): name for index, name in enumerate(OUTPUT_NAMES)}

LOCK = threading.Lock()
STATS = {
    "commands": {"MULTI": 0, "EXEC": 0, "PUBLISH": 0},
    "outputs": {
        name: {
            "publish_count": 0,
            "direct_publish_count": 0,
            "transaction_count": 0,
            "transaction_request_bytes": [],
            "direct_request_bytes": [],
        }
        for name in OUTPUT_NAMES
    },
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


def read_request(sock):
    marker = recv_exact(sock, 1)
    line = recv_line(sock)
    if marker != b"*":
        raise RuntimeError(f"expected RESP command array, got {marker!r}")
    count = int(line[:-2])
    raw = marker + line
    arguments = []
    for _ in range(count):
        bulk_marker = recv_exact(sock, 1)
        bulk_line = recv_line(sock)
        if bulk_marker != b"$":
            raise RuntimeError(f"expected RESP bulk argument, got {bulk_marker!r}")
        length = int(bulk_line[:-2])
        body = recv_exact(sock, length + 2)
        if body[-2:] != b"\r\n":
            raise RuntimeError("invalid RESP request bulk terminator")
        raw += bulk_marker + bulk_line + body
        arguments.append(body[:-2])
    return raw, arguments


def read_response(sock):
    marker = recv_exact(sock, 1)
    line = recv_line(sock)
    raw = marker + line
    value = line[:-2]

    if marker in (b"+", b"-"):
        return raw, value if marker == b"+" else ("error", value)
    if marker == b":":
        return raw, int(value)
    if marker in (b",", b"#", b"_"):
        return raw, value
    if marker in (b"$", b"!", b"="):
        length = int(value)
        if length == -1:
            return raw, None
        body = recv_exact(sock, length + 2)
        if not body.endswith(b"\r\n"):
            raise RuntimeError("invalid RESP response bulk terminator")
        return raw + body, body[:-2]
    if marker in (b"*", b"~", b">"):
        count = int(value)
        if count == -1:
            return raw, None
        items = []
        for _ in range(count):
            item_raw, item = read_response(sock)
            raw += item_raw
            items.append(item)
        return raw, items
    if marker in (b"%", b"|"):
        count = int(value)
        items = []
        for _ in range(count * 2):
            item_raw, item = read_response(sock)
            raw += item_raw
            items.append(item)
        if marker == b"|":
            following_raw, following = read_response(sock)
            return raw + following_raw, following
        return raw, items
    raise RuntimeError(f"unsupported RESP response marker {marker!r}")


def output_for_channel(channel):
    for name in OUTPUT_NAMES:
        if channel.startswith(f"out:{name}:".encode()):
            return name
    return None


def record_command(command):
    if command in STATS["commands"]:
        STATS["commands"][command] += 1


def connection_worker(client):
    upstream = None
    output_name = None
    in_multi = False
    transaction_bytes = 0
    transaction_output = None
    try:
        client.settimeout(None)
        upstream = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=5)
        upstream.settimeout(None)
        while True:
            raw_command, arguments = read_request(client)
            if not arguments:
                raise RuntimeError("empty RESP command")
            command = arguments[0].upper().decode("ascii")
            publish_output = (
                output_for_channel(arguments[1])
                if command == "PUBLISH" and len(arguments) > 1
                else None
            )

            if command == "SELECT" and len(arguments) > 1:
                output_name = DATABASE_OUTPUTS.get(arguments[1].decode("ascii"))
            elif command == "MULTI":
                in_multi = True
                transaction_bytes = len(raw_command)
                transaction_output = output_name
            elif command == "PUBLISH":
                with LOCK:
                    record_command(command)
                    if publish_output is not None:
                        output_stats = STATS["outputs"][publish_output]
                        output_stats["publish_count"] += 1
                        if in_multi:
                            if transaction_output is None:
                                transaction_output = publish_output
                        else:
                            output_stats["direct_publish_count"] += 1
                            output_stats["direct_request_bytes"].append(len(raw_command))
                if in_multi:
                    transaction_bytes += len(raw_command)
            elif command == "EXEC":
                with LOCK:
                    record_command(command)
                if in_multi:
                    transaction_bytes += len(raw_command)

            upstream.sendall(raw_command)
            raw_response, response = read_response(upstream)
            client.sendall(raw_response)

            if command == "MULTI":
                with LOCK:
                    record_command(command)
            elif command == "EXEC":
                if in_multi and transaction_output is not None:
                    with LOCK:
                        output_stats = STATS["outputs"][transaction_output]
                        output_stats["transaction_count"] += 1
                        output_stats["transaction_request_bytes"].append(
                            transaction_bytes
                        )
                in_multi = False
                transaction_bytes = 0
                transaction_output = None
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
        return copy.deepcopy(STATS)


def stats_worker(client):
    try:
        client.settimeout(3)
        request = recv_line(client).strip().upper()
        if request != b"STATS":
            client.sendall(b"error=expected STATS\n")
            return
        client.sendall(json.dumps(stats_snapshot(), separators=(",", ":")).encode() + b"\n")
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
