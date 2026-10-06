import os
import socket


REDIS_HOST = os.environ.get("REDIS_HOST", "10.253.240.10")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


def send_command(sock, *arguments):
    encoded = [
        argument if isinstance(argument, bytes) else str(argument).encode()
        for argument in arguments
    ]
    frame = [f"*{len(encoded)}\r\n".encode()]
    for argument in encoded:
        frame.extend((f"${len(argument)}\r\n".encode(), argument, b"\r\n"))
    sock.sendall(b"".join(frame))


def read_exact(reader, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = reader.read(remaining)
        if not chunk:
            raise RuntimeError("Redis closed the connection")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_response(reader):
    marker = read_exact(reader, 1)
    if not marker:
        raise RuntimeError("Redis closed the connection")
    line = reader.readline().rstrip(b"\r\n")

    if marker == b"+":
        return line.decode()
    if marker == b"-":
        raise RuntimeError(line.decode())
    if marker == b":":
        return int(line)
    if marker == b"$":
        length = int(line)
        if length == -1:
            return None
        value = read_exact(reader, length)
        if read_exact(reader, 2) != b"\r\n":
            raise RuntimeError("Invalid Redis bulk string terminator")
        return value
    if marker == b"*":
        count = int(line)
        if count == -1:
            return None
        return [read_response(reader) for _ in range(count)]
    raise RuntimeError(f"Unsupported RESP marker: {marker!r}")


def connect(database):
    sock = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=5)
    sock.settimeout(5)
    reader = sock.makefile("rb", buffering=0)
    send_command(sock, "SELECT", database)
    if read_response(reader) != "OK":
        raise RuntimeError(f"Redis did not select database {database}")
    return sock, reader


def close(sock, reader):
    reader.close()
    sock.close()
