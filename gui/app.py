"""Tkinter client for the state machine server (see ../server.py).

    config + Load             read the yaml, fill the task list
    host / port + Connect     open the TCP link
    task + Send task          TASK <name>, the server rebuilds its machine
    Next / Cancel             send NEXT / CANCEL
    status box                every line the server sends back

The config is read here only to know which task names exist. What actually gets
sent is the NAME; the config that counts is the one sitting next to the server.

Threads: tkinter is not thread safe, so the reader thread never touches a widget.
It drops received lines into a Queue and the widgets are only ever updated from
the tk main loop, by drain_inbox() re-armed with after().

Run:  python3 gui/app.py            (needs tkinter + pyyaml, both already there)
"""

import queue
import socket
import threading
import tkinter as tk
from pathlib import Path
from tkinter import scrolledtext

import yaml

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1245
ENCODING = "utf-8"

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "config" / "config_example.yaml"
KEY_TASK = "Task"  # same reserved key as in ../main.py

CONNECT_TIMEOUT = 3.0   # seconds to wait for the server
DRAIN_PERIOD = 100      # ms between two reads of the inbox
MAX_LINES = 500         # keep the status box from growing forever


class Client:
    """Socket half: connect, send a line, read lines into an inbox queue."""

    def __init__(self, inbox):
        self.inbox = inbox
        self._socket = None
        self._reader = None

    @property
    def connected(self):
        return self._socket is not None

    def connect(self, host, port):
        self.close()
        self._socket = socket.create_connection((host, port), CONNECT_TIMEOUT)
        self._socket.settimeout(None)
        self._reader = threading.Thread(target=self._read_forever, daemon=True)
        self._reader.start()

    def send(self, command):
        if not self.connected:
            raise ConnectionError("not connected")
        self._socket.sendall((command + "\n").encode(ENCODING))

    def close(self):
        if self._socket is None:
            return
        socket_to_close, self._socket = self._socket, None
        socket_to_close.close()

    def _read_forever(self):
        """Reader thread: every line goes to the inbox, never to a widget."""
        sock = self._socket
        try:
            with sock.makefile("r", encoding=ENCODING) as stream:
                for line in stream:
                    self.inbox.put(line.rstrip("\n"))
        except OSError as error:
            self.inbox.put(f"connection lost: {error}")
        finally:
            if self._socket is sock:  # not replaced by a newer connection
                self._socket = None
            self.inbox.put("disconnected")


class App:
    def __init__(self, root):
        self.root = root
        self.inbox = queue.Queue()
        self.client = Client(self.inbox)

        root.title("State machine")
        root.minsize(520, 400)
        self._build()
        self.on_load_config()  # the default config is right next to the server
        self.root.after(DRAIN_PERIOD, self.drain_inbox)

    # ---------------------------------------------------------------- layout
    def _build(self):
        config_row = tk.Frame(self.root)
        config_row.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(config_row, text="config").pack(side="left")
        self.config_entry = tk.Entry(config_row)
        self.config_entry.insert(0, str(DEFAULT_CONFIG))
        self.config_entry.pack(side="left", fill="x", expand=True, padx=4)
        tk.Button(config_row, text="Load", command=self.on_load_config).pack(side="left")

        connect_row = tk.Frame(self.root)
        connect_row.pack(fill="x", padx=8, pady=(8, 4))

        tk.Label(connect_row, text="host").pack(side="left")
        self.host_entry = tk.Entry(connect_row, width=14)
        self.host_entry.insert(0, DEFAULT_HOST)
        self.host_entry.pack(side="left", padx=(4, 8))

        tk.Label(connect_row, text="port").pack(side="left")
        self.port_entry = tk.Entry(connect_row, width=6)
        self.port_entry.insert(0, str(DEFAULT_PORT))
        self.port_entry.pack(side="left", padx=(4, 8))

        self.connect_button = tk.Button(connect_row, text="Connect",
                                        command=self.on_connect)
        self.connect_button.pack(side="left")

        task_row = tk.Frame(self.root)
        task_row.pack(fill="x", padx=8, pady=4)
        tk.Label(task_row, text="task").pack(side="left")
        self.task_name = tk.StringVar(value="")
        self.task_menu = tk.OptionMenu(task_row, self.task_name, "")
        self.task_menu.pack(side="left", padx=4)
        self.task_button = tk.Button(task_row, text="Send task",
                                     command=self.on_send_task)
        self.task_button.pack(side="left")

        command_row = tk.Frame(self.root)
        command_row.pack(fill="x", padx=8, pady=4)
        self.next_button = tk.Button(command_row, text="Next", width=12,
                                     command=lambda: self.send("NEXT"))
        self.next_button.pack(side="left")
        self.cancel_button = tk.Button(command_row, text="Cancel", width=12,
                                       command=lambda: self.send("CANCEL"))
        self.cancel_button.pack(side="left", padx=8)

        self.state_label = tk.Label(self.root, text="state: -", anchor="w")
        self.state_label.pack(fill="x", padx=8)

        self.status_box = scrolledtext.ScrolledText(self.root, height=12,
                                                    state="disabled", wrap="word")
        self.status_box.pack(fill="both", expand=True, padx=8, pady=8)

        self._set_buttons(connected=False)

    def _set_buttons(self, connected):
        state = "normal" if connected else "disabled"
        self.next_button.config(state=state)
        self.cancel_button.config(state=state)
        self.task_button.config(state=state)
        self.connect_button.config(text="Disconnect" if connected else "Connect")

    # ---------------------------------------------------------------- events
    def on_load_config(self):
        """Read the yaml just far enough to list the task names."""
        path = Path(self.config_entry.get().strip())
        try:
            raw = yaml.safe_load(path.read_text()) or {}
        except (OSError, yaml.YAMLError) as error:
            self.log(f"cannot read {path}: {error}")
            return

        tasks = list(raw.get(KEY_TASK) or [])
        if not tasks:
            self.log(f"{path}: no '{KEY_TASK}:' section")
            return

        menu = self.task_menu["menu"]
        menu.delete(0, "end")
        for task in tasks:
            menu.add_command(label=task, command=tk._setit(self.task_name, task))
        self.task_name.set(tasks[0])
        self.log(f"loaded {len(tasks)} task(s) from {path.name}: {', '.join(tasks)}")

    def on_send_task(self):
        task = self.task_name.get()
        if not task:
            self.log("no task selected, press Load first")
            return
        self.send(f"TASK {task}")

    def on_connect(self):
        if self.client.connected:
            self.client.close()
            self.log("disconnected")
            self._set_buttons(connected=False)
            return

        host = self.host_entry.get().strip() or DEFAULT_HOST
        port = self.port_entry.get().strip()
        if not port.isdigit():
            self.log(f"bad port '{port}'")
            return

        try:
            self.client.connect(host, int(port))
        except OSError as error:
            self.log(f"cannot connect to {host}:{port}: {error}")
            return

        self.log(f"connected to {host}:{port}")
        self._set_buttons(connected=True)
        self.send("STATUS")  # the server only pushes on change, so ask once

    def send(self, command):
        try:
            self.client.send(command)
        except (OSError, ConnectionError) as error:
            self.log(f"send failed: {error}")
            self._set_buttons(connected=False)
            return
        self.log(f"> {command}")

    def drain_inbox(self):
        """Tk main loop: the only place widgets are touched."""
        while True:
            try:
                line = self.inbox.get_nowait()
            except queue.Empty:
                break

            self.log(line)
            if line.startswith("STATE "):
                self.state_label.config(text=f"state: {line[len('STATE '):]}")
            elif line == "disconnected":
                self._set_buttons(connected=False)

        self.root.after(DRAIN_PERIOD, self.drain_inbox)

    def log(self, line):
        self.status_box.config(state="normal")
        self.status_box.insert("end", line + "\n")

        extra = int(self.status_box.index("end-1c").split(".")[0]) - MAX_LINES
        if extra > 0:
            self.status_box.delete("1.0", f"{extra + 1}.0")

        self.status_box.see("end")
        self.status_box.config(state="disabled")


def main():
    root = tk.Tk()
    app = App(root)
    root.protocol("WM_DELETE_WINDOW", lambda: (app.client.close(), root.destroy()))
    root.mainloop()


if __name__ == "__main__":
    main()
