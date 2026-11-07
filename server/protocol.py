"""Ayudantes compartidos para el protocolo JSON con longitud prefijada."""

import json
import socket
import struct
from typing import Any


HEADER_SIZE = 4
MAX_MESSAGE_SIZE = 10 * 1024 * 1024


def recv_exact(sock: socket.socket, size: int) -> bytes:
    """Recibe exactamente ``size`` bytes o lanza ConnectionError al detectar EOF."""
    if size < 0:
        raise ValueError("el tamaño no puede ser negativo")

    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise ConnectionError("la conexión se cerró mientras se recibían datos")
        chunks.extend(chunk)
    return bytes(chunks)


def send_msg(sock: socket.socket, obj: Any) -> None:
    """Serializa un objeto como JSON UTF-8 con longitud prefijada big-endian."""
    payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(payload) > MAX_MESSAGE_SIZE:
        raise ValueError("el mensaje es demasiado grande")
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def recv_msg(sock: socket.socket) -> Any:
    """Recibe y decodifica un mensaje JSON UTF-8 con longitud prefijada."""
    header = recv_exact(sock, HEADER_SIZE)
    (payload_size,) = struct.unpack("!I", header)
    if payload_size > MAX_MESSAGE_SIZE:
        raise ValueError("el mensaje es demasiado grande")

    payload = recv_exact(sock, payload_size)
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("el mensaje JSON UTF-8 no es válido") from exc
