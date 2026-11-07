"""Cliente de línea de comandos para el sistema distribuido de tareas."""

import argparse
import logging
import os
import random
import socket
import sys
import threading
import time
import uuid
from typing import Any

try:
    from server.protocol import recv_msg, send_msg
except ModuleNotFoundError as exc:
    if exc.name != "server":
        raise
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from server.protocol import recv_msg, send_msg


LOGGER = logging.getLogger(__name__)


def build_tasks(count: int) -> list[dict[str, Any]]:
    """Genera una colección variada de tareas de demostración."""
    tasks: list[dict[str, Any]] = []
    sample_texts = [
        "De tanto hablar, ya ni recuerdo qué piensas",
        "De criticar, hoy se alimenta tu ausencia",
        "Dispárame, no dudes más",
        "Dentro de la guerra, puedes descubrir",
        "Tu bandera blanca se pintó de gris",
        "Y eso duele más Ver la realidad",
        "Porque todo de mí hace quebrar tu conciencia",
        "Todo de ti me hizo perder la inocencia",
    ]
    task_types = ["primes", "reverse", "wordcount"]
    for index in range(count):
        task_type = random.choice(task_types)
        if task_type == "primes":
            payload: dict[str, Any] = {"n": random.randint(5000, 20000)}
        else:
            payload = {"text": random.choice(sample_texts)}
        tasks.append(
            {
                "task_id": f"task-{index + 1:03d}-{uuid.uuid4().hex[:8]}",
                "type": task_type,
                "payload": payload,
            }
        )
    return tasks


def print_result(response: dict[str, Any], elapsed: float, task_types: dict[str, str]) -> None:
    """Imprime una respuesta usando una fila compacta de tabla."""
    task_id = str(response.get("task_id", ""))
    task_type = task_types.get(task_id, "?")
    worker = str(response.get("worker", "?"))
    if response.get("status") == "ok":
        result = str(response.get("result"))
    else:
        result = f"ERROR: {response.get('error', 'error desconocido')}"
    print(f"{task_id:<24} {task_type:<10} {worker:<18} {result:<70} {elapsed:>8.3f}s")


def run_client(host: str, port: int, task_count: int) -> int:
    """Envía tareas e imprime las respuestas a medida que llegan."""
    tasks = build_tasks(task_count)
    task_types = {task["task_id"]: task["type"] for task in tasks}
    sent_at: dict[str, float] = {}
    received = 0
    receive_error: list[str] = []
    receive_lock = threading.Lock()

    try:
        with socket.create_connection((host, port), timeout=10.0) as connection:
            connection.settimeout(10.0)
            print(f"{'task_id':<24} {'type':<10} {'worker':<18} {'resultado':<70} {'tiempo':>8}")
            print("-" * 140)

            def receive_results() -> None:
                nonlocal received
                try:
                    while True:
                        response = recv_msg(connection)
                        if not isinstance(response, dict):
                            raise ValueError("el dispatcher devolvió una respuesta no válida")
                        task_id = response.get("task_id")
                        if not isinstance(task_id, str):
                            raise ValueError("la respuesta contiene un task_id no válido")
                        elapsed = time.monotonic() - sent_at.get(task_id, time.monotonic())
                        with receive_lock:
                            print_result(response, elapsed, task_types)
                            received += 1
                            if received == task_count:
                                return
                except (ConnectionError, OSError, socket.timeout, ValueError) as exc:
                    receive_error.append(str(exc))

            receiver = threading.Thread(target=receive_results, daemon=True)
            receiver.start()
            for task in tasks:
                sent_at[task["task_id"]] = time.monotonic()
                try:
                    send_msg(connection, task)
                except (OSError, socket.timeout, ValueError) as exc:
                    receive_error.append(
                        f"no se pudo enviar {task['task_id']}: {exc}"
                    )
                    break
            receiver.join(timeout=15.0)
            if received != task_count and not receive_error:
                receive_error.append(
                    "se agotó el tiempo de espera para recibir todas las respuestas"
                )
    except (ConnectionError, OSError, socket.timeout) as exc:
        print(f"Error de conexión con el dispatcher: {exc}")
        return 1

    if receive_error:
        print(f"Error del cliente: {receive_error[0]}")
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    """Analiza los argumentos de la línea de comandos."""
    parser = argparse.ArgumentParser(description="Ejecuta el cliente de tareas distribuidas.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--tasks", type=int, default=10)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port debe estar entre 1 y 65535")
    if args.tasks < 1:
        parser.error("--tasks debe ser como mínimo 1")
    return args


def main() -> None:
    """Inicia el cliente a partir de los argumentos de la línea de comandos."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    args = parse_args()
    raise SystemExit(run_client(args.host, args.port, args.tasks))


if __name__ == "__main__":
    main()
