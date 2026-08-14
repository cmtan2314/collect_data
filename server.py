"""TCP front end for the state machine. One line = one message, both directions.

    client -> server        server -> client
    NEXT                    OK <command>        command accepted
    CANCEL                  ERR <text>          command refused
    STATUS                  STATE <name>        on every state change, and on STATUS
    TASK <name>             (the client asks for the first one with STATUS)

The server owns no state machine logic: it turns a line into a call to the
handler given by main(), and pushes whatever main() broadcasts back out to every
connected client.

Threads: one accept loop, plus one reader thread per connected client. Both watch
SHUTDOWN so Ctrl-C in the terminal takes everything down.
"""

import socket
import threading

from events import SHUTDOWN

HOST = ""  # every interface, so the GUI can sit on another machine
PORT = 1245
ENCODING = "utf-8"
ACCEPT_TIMEOUT = 0.5  # seconds; how often the accept loop rechecks SHUTDOWN


class Server:
    def __init__(self, handle_command=None, host=HOST, port=PORT):
        # (command: str) -> reply line. Assigned after the machine exists, since
        # the handler needs it and the machine needs the server to broadcast.
        self.handle_command = handle_command
        # (count: int) -> None, called whenever a client connects or drops
        self.on_clients_changed = None
        self.host = host
        self.port = port
        self._clients = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------- outgoing
    def broadcast(self, line):
        """Send one line to every client. Dead sockets are dropped silently."""
        payload = (line + "\n").encode(ENCODING)
        with self._lock:
            clients = list(self._clients)

        for conn in clients:
            try:
                conn.sendall(payload)
            except OSError:
                self._drop(conn)

    def broadcast_state(self, name):
        """Listener handed to the state machine: one line per state change."""
        self.broadcast(f"STATE {name}")

    def _drop(self, conn):
        with self._lock:
            if conn not in self._clients:
                return  # already dropped, do not report the same loss twice
            self._clients.remove(conn)
            count = len(self._clients)
        conn.close()
        self._report_clients(count)

    def _report_clients(self, count):
        if self.on_clients_changed is not None:
            self.on_clients_changed(count)

    # ------------------------------------------------------------- incoming
    def serve_forever(self):
        """Accept loop. Runs in its own thread until SHUTDOWN is set."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                listener.bind((self.host, self.port))
            except OSError as error:
                # usually another copy of this program is still running. Say so
                # loudly: this runs in a thread, a traceback here is easy to miss
                print(f"[server  ] cannot listen on port {self.port}: {error}")
                print("[server  ] no GUI will be able to connect")
                return

            listener.listen()
            listener.settimeout(ACCEPT_TIMEOUT)
            print(f"[server  ] listening on port {self.port}")

            while not SHUTDOWN.is_set():
                try:
                    conn, address = listener.accept()
                except socket.timeout:
                    continue
                except OSError as error:
                    print(f"[server  ] accept failed: {error}")
                    break

                with self._lock:
                    self._clients.append(conn)
                    count = len(self._clients)
                print(f"[server  ] client connected: {address[0]}:{address[1]}")
                self._report_clients(count)
                threading.Thread(target=self._read_client, args=(conn, address),
                                 daemon=True).start()

        print("[server  ] stopped")

    def _read_client(self, conn, address):
        """One thread per client: read lines, answer each one."""
        try:
            with conn.makefile("r", encoding=ENCODING) as stream:
                for line in stream:
                    command = line.strip()
                    if not command:
                        continue
                    if self.handle_command is None:
                        reply = "ERR server not wired up yet"
                    else:
                        reply = self.handle_command(command)
                    conn.sendall((reply + "\n").encode(ENCODING))
        except OSError as error:
            print(f"[server  ] client {address[0]} dropped: {error}")
        finally:
            print(f"[server  ] client disconnected: {address[0]}:{address[1]}")
            self._drop(conn)
