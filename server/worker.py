"""Implementación del servidor worker de tareas."""

import argparse
import logging
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from protocol import recv_msg, send_msg


LOGGER = logging.getLogger(__name__)
TaskFunction = Callable[[dict[str, Any]], Any]


def count_primes(payload: dict[str, Any]) -> int:
    """Devuelve la cantidad de números primos menores o iguales que N."""
    limit = payload.get("n")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
        raise ValueError("'n' debe ser un entero no negativo")

    count = 0
    for candidate in range(2, limit + 1):
        is_prime = True
        divisor = 2
        while divisor * divisor <= candidate:
            if candidate % divisor == 0:
                is_prime = False
                break
            divisor += 1
        if is_prime:
            count += 1
    return count


def reverse_text(payload: dict[str, Any]) -> str:
    """Devuelve el texto del payload con sus caracteres en orden inverso."""
    text = payload.get("text")
    if not isinstance(text, str):
        raise ValueError("'text' debe ser una cadena de texto")
    return text[::-1]


def count_words(payload: dict[str, Any]) -> int:
    """Devuelve la cantidad de palabras separadas por espacios en blanco."""
    text = payload.get("text")
    if not isinstance(text, str):
        raise ValueError("'text' debe ser una cadena de texto")
    return len(text.split())


TASK_HANDLERS: dict[str, TaskFunction] = {
    "primes": count_primes,
    "reverse": reverse_text,
    "wordcount": count_words,
}


def execute_task(task: dict[str, Any], worker_name: str) -> dict[str, Any]:
    """Ejecuta una tarea y convierte los fallos en una respuesta de error."""
    task_id = task.get("task_id", "")
    try:
        task_type = task["type"]
        payload = task["payload"]
        if not isinstance(task_id, str):
            raise ValueError("'task_id' debe ser una cadena de texto")
        if not isinstance(task_type, str):
            raise ValueError("'type' debe ser una cadena de texto")
        if not isinstance(payload, dict):
            raise ValueError("'payload' debe ser un objeto")

        handler = TASK_HANDLERS.get(task_type)
        if handler is None:
            raise ValueError(f"tipo de tarea desconocido: {task_type}")
        result = handler(payload)
        return {
            "task_id": task_id,
            "status": "ok",
            "result": result,
            "error": None,
            "worker": worker_name,
        }
    except (KeyError, TypeError, ValueError) as exc:
        return {
            "task_id": task_id if isinstance(task_id, str) else "",
            "status": "error",
            "result": None,
            "error": str(exc),
            "worker": worker_name,
        }


def handle_dispatcher(
    connection: socket.socket,
    address: tuple[str, int],
    executor: ThreadPoolExecutor,
    worker_name: str,
) -> None:
    """Recibe tareas de una conexión del dispatcher y las envía al pool."""
    send_lock = threading.Lock()
    pending: set[Any] = set()

    def send_result(future: Any) -> None:
        try:
            response = future.result()
            with send_lock:
                send_msg(connection, response)
        except (ConnectionError, OSError, socket.timeout) as exc:
            LOGGER.warning("No se pudo enviar el resultado a %s: %s", address, exc)
        finally:
            pending.discard(future)

    try:
        while True:
            try:
                task = recv_msg(connection)
            except (ConnectionError, OSError, socket.timeout):
                break
            except ValueError as exc:
                LOGGER.warning("Mensaje no válido de %s: %s", address, exc)
                response = {
                    "task_id": "",
                    "status": "error",
                    "result": None,
                    "error": str(exc),
                    "worker": worker_name,
                }
                with send_lock:
                    send_msg(connection, response)
                continue

            if not isinstance(task, dict):
                task = {"task_id": "", "type": "", "payload": None}
            future = executor.submit(execute_task, task, worker_name)
            pending.add(future)
            future.add_done_callback(send_result)
    finally:
        for future in list(pending):
            try:
                future.result()
            except Exception:
                LOGGER.exception("Fallo inesperado de tarea para %s", address)
        connection.close()
        LOGGER.info("Conexión del dispatcher cerrada: %s", address)


def serve(host: str, port: int, name: str, threads: int) -> None:
    """Ejecuta el servidor worker hasta que sea interrumpido."""
    stop_event = threading.Event()
    executor = ThreadPoolExecutor(max_workers=threads, thread_name_prefix=f"{name}-task")
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.settimeout(1.0)
    server_socket.bind((host, port))
    server_socket.listen()
    LOGGER.info(
        "Worker %s escuchando en %s:%d con %d hilos", name, host, port, threads
    )

    connections: list[socket.socket] = []
    connection_threads: list[threading.Thread] = []
    try:
        while not stop_event.is_set():
            try:
                connection, address = server_socket.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                if not stop_event.is_set():
                    LOGGER.error("Falló la aceptación de una conexión: %s", exc)
                break
            connections.append(connection)
            thread = threading.Thread(
                target=handle_dispatcher,
                args=(connection, address, executor, name),
                daemon=True,
            )
            connection_threads.append(thread)
            thread.start()
    except KeyboardInterrupt:
        LOGGER.info("Deteniendo worker %s", name)
    finally:
        stop_event.set()
        server_socket.close()
        for connection in connections:
            connection.close()
        for thread in connection_threads:
            thread.join(timeout=2)
        executor.shutdown(wait=True, cancel_futures=False)
        LOGGER.info("Worker %s detenido", name)


def parse_args() -> argparse.Namespace:
    """Analiza los argumentos de la línea de comandos."""
    parser = argparse.ArgumentParser(description="Ejecuta un worker de tareas.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    if args.port < 1 or args.port > 65535:
        parser.error("--port debe estar entre 1 y 65535")
    if args.threads < 1:
        parser.error("--threads debe ser como mínimo 1")
    return args


def main() -> None:
    """Inicia el worker a partir de los argumentos de la línea de comandos."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    serve(args.host, args.port, args.name, args.threads)


if __name__ == "__main__":
    main()
