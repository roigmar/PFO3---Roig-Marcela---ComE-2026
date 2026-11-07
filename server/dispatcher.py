"""Servidor TCP dispatcher que distribuye tareas entre workers."""

import argparse
import logging
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from protocol import recv_msg, send_msg


LOGGER = logging.getLogger(__name__)
WORKER_TIMEOUT = 5.0


class WorkerPool:
    """Selector de workers round-robin seguro para hilos."""

    def __init__(self, workers: list[tuple[str, int]]) -> None:
        if not workers:
            raise ValueError("se requiere al menos un worker")
        self._workers = workers
        self._next_index = 0
        self._lock = threading.Lock()

    def ordered_workers(self) -> list[tuple[str, int]]:
        """Devuelve los workers comenzando en la próxima posición round-robin."""
        with self._lock:
            start = self._next_index
            self._next_index = (self._next_index + 1) % len(self._workers)
        return self._workers[start:] + self._workers[:start]


def forward_task(
    task: dict[str, Any],
    worker_pool: WorkerPool,
    client_address: tuple[str, int],
) -> dict[str, Any]:
    """Envía una tarea a los workers hasta que uno devuelva una respuesta."""
    task_id = task.get("task_id", "")
    last_error = "todos los workers fallaron"
    for worker_host, worker_port in worker_pool.ordered_workers():
        worker_label = f"{worker_host}:{worker_port}"
        LOGGER.info(
            "cliente=%s task_id=%s worker=%s",
            client_address,
            task_id,
            worker_label,
        )
        try:
            with socket.create_connection(
                (worker_host, worker_port), timeout=WORKER_TIMEOUT
            ) as worker_socket:
                worker_socket.settimeout(WORKER_TIMEOUT)
                send_msg(worker_socket, task)
                response = recv_msg(worker_socket)
                if not isinstance(response, dict):
                    raise ValueError("el worker devolvió una respuesta no válida")
                return response
        except (ConnectionError, OSError, socket.timeout, ValueError) as exc:
            last_error = f"{worker_label}: {exc}"
            LOGGER.warning(
                "fallo de worker cliente=%s task_id=%s worker=%s error=%s",
                client_address,
                task_id,
                worker_label,
                exc,
            )

    return {
        "task_id": task_id if isinstance(task_id, str) else "",
        "status": "error",
        "result": None,
        "error": last_error,
        "worker": "dispatcher",
    }


def handle_client(
    connection: socket.socket,
    address: tuple[str, int],
    worker_pool: WorkerPool,
    executor: ThreadPoolExecutor,
) -> None:
    """Recibe tareas del cliente y envía respuestas terminadas en paralelo."""
    send_lock = threading.Lock()
    pending: set[Any] = set()

    def send_result(future: Any) -> None:
        try:
            response = future.result()
            with send_lock:
                send_msg(connection, response)
        except (ConnectionError, OSError, socket.timeout) as exc:
            LOGGER.warning("No se pudo responder al cliente %s: %s", address, exc)
        finally:
            pending.discard(future)

    LOGGER.info("Cliente conectado: %s", address)
    try:
        while True:
            try:
                task = recv_msg(connection)
            except (ConnectionError, OSError, socket.timeout):
                break
            except ValueError as exc:
                LOGGER.warning("Mensaje no válido del cliente %s: %s", address, exc)
                response = {
                    "task_id": "",
                    "status": "error",
                    "result": None,
                    "error": str(exc),
                    "worker": "dispatcher",
                }
                with send_lock:
                    send_msg(connection, response)
                continue

            if not isinstance(task, dict):
                task = {"task_id": "", "type": "", "payload": None}
            future = executor.submit(forward_task, task, worker_pool, address)
            pending.add(future)
            future.add_done_callback(send_result)
    finally:
        for future in list(pending):
            try:
                future.result()
            except Exception:
                LOGGER.exception("Fallo inesperado de tarea del cliente %s", address)
        connection.close()
        LOGGER.info("Cliente desconectado: %s", address)


def parse_worker(value: str) -> tuple[str, int]:
    """Analiza una dirección de worker con formato host:port."""
    host, separator, port_text = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("el worker debe usar el formato host:port")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("el puerto del worker debe ser un entero") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(
            "el puerto del worker debe estar entre 1 y 65535"
        )
    return host, port


def serve(host: str, port: int, workers: list[tuple[str, int]]) -> None:
    """Ejecuta el dispatcher hasta que sea interrumpido."""
    worker_pool = WorkerPool(workers)
    executor = ThreadPoolExecutor(max_workers=max(4, len(workers) * 4))
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.settimeout(1.0)
    server_socket.bind((host, port))
    server_socket.listen()
    LOGGER.info("Dispatcher escuchando en %s:%d", host, port)

    connections: list[socket.socket] = []
    connection_threads: list[threading.Thread] = []
    try:
        while True:
            try:
                connection, address = server_socket.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                LOGGER.error("Falló la aceptación de una conexión: %s", exc)
                break
            connections.append(connection)
            thread = threading.Thread(
                target=handle_client,
                args=(connection, address, worker_pool, executor),
                daemon=True,
            )
            connection_threads.append(thread)
            thread.start()
    except KeyboardInterrupt:
        LOGGER.info("Deteniendo dispatcher")
    finally:
        server_socket.close()
        for connection in connections:
            connection.close()
        for thread in connection_threads:
            thread.join(timeout=2)
        executor.shutdown(wait=True, cancel_futures=False)
        LOGGER.info("Dispatcher detenido")


def parse_args() -> argparse.Namespace:
    """Analiza los argumentos de la línea de comandos."""
    parser = argparse.ArgumentParser(description="Ejecuta el dispatcher de tareas.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--workers", nargs="+", required=True, type=parse_worker)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port debe estar entre 1 y 65535")
    return args


def main() -> None:
    """Inicia el dispatcher a partir de los argumentos de la línea de comandos."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    serve(args.host, args.port, args.workers)


if __name__ == "__main__":
    main()
