"""Tkinter client for the state machine (see ../server.py). Needs paho-mqtt 2.x.

    config + Load             read the yaml, fill the task list
    broker ip / port          the machine running the state machine, which is the
                              machine running the broker
    task + Send task          TASK <name>, the server rebuilds its machine
    Next / Cancel             send NEXT / CANCEL
    status box                every reply and state change

The config is read here only to know which task names exist. What actually gets
sent is the NAME; the config that counts is the one sitting next to the server.

While connected, this app keeps a presence message alive on its own topic, and
registers offline as its last will. That is how the server notices a GUI that was
killed rather than closed, and starts its countdown.

Threads: tkinter is not thread safe, and paho's callbacks run in paho's thread, so
those callbacks never touch a widget. They drop lines into a Queue, and the
widgets are only ever updated from the tk main loop by drain_inbox(), re-armed
with after().

Run:  python3 gui/app.py            (needs tkinter, pyyaml, paho-mqtt)
"""

import queue
import sys
import tkinter as tk
from pathlib import Path
from tkinter import scrolledtext
from uuid import uuid4

import paho.mqtt.client as mqtt
import yaml

# protocol.py sits one level up. Copying this app to another machine means taking
# that file along; it is the one place the wire format is written down.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import protocol  # noqa: E402
from protocol import KEEPALIVE, QOS, Command  # noqa: E402

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = protocol.BROKER_PORT

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "config" / "config_example.yaml"
KEY_TASK = "Task"          # same reserved keys as in ../main.py
KEY_SEQUENCE = "sequence"
START_STATE = "Start"
END_STATE = "End"
CANCEL_STATE = "Cancel"

# ---- the chain drawing
CHAIN_HEIGHT = 130
BOX_HEIGHT = 34
BOX_MIN_WIDTH = 54
BOX_RADIUS = 8
BOX_GAP = 22       # room for the arrow between two boxes
CHAIN_MARGIN = 12
CANCEL_DROP = 46   # how far below the chain the Cancel box sits

CANVAS_BG = "#f8f9fa"
IDLE_FILL = "#e9edf2"
IDLE_LINE = "#b9c2cc"
IDLE_TEXT = "#3b4650"
LIVE_FILL = "#2f9e44"   # the state the machine is in right now
LIVE_LINE = "#237634"
LIVE_TEXT = "#ffffff"
ABORT_FILL = "#e03131"  # same idea, but Cancel gets red rather than green
ABORT_LINE = "#a92222"
ARROW_COLOR = "#8d97a1"

DRAIN_PERIOD = 100  # ms between two reads of the inbox
MAX_LINES = 500     # keep the status box from growing forever

# what the inbox carries: (kind, payload). Everything but a note is a decoded
# message, notes are lines this app wrote about itself.
KIND_STATE = "state"
KIND_SERVER = "server"
KIND_REPLY = "reply"
KIND_NOTE = "note"
NOTE_DISCONNECTED = "disconnected"


class MqttLink:
    """MQTT half: connect, publish commands, drop incoming lines into an inbox."""

    def __init__(self, inbox):
        self.inbox = inbox
        self.client_id = f"gui-{uuid4().hex[:8]}"  # several GUIs can run at once
        self.topics = protocol.Topics()  # replaced at connect by the config's base
        self._client = None
        self._last_id = 0

    @property
    def presence_topic(self):
        return self.topics.client(self.client_id)

    @property
    def connected(self):
        return self._client is not None

    def connect(self, host, port, topics=None):
        self.close()
        self.topics = topics or protocol.Topics()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_disconnect = self._on_disconnect
        # left behind for the broker to publish if this app dies without saying bye
        client.will_set(self.presence_topic, protocol.encode(protocol.presence(False)),
                        qos=QOS, retain=True)

        # assign before connecting: paho's thread can run _on_connect the moment
        # loop_start() returns, and that callback publishes through self._client
        self._client = client
        try:
            client.connect(host, port, KEEPALIVE)
        except OSError:
            self._client = None
            raise
        client.loop_start()

    def send(self, command, state=None, task=None, **fields):
        """Publish one command and return the id it was given, so the reply can
        be matched to the button that caused it.

        state and task are what THIS app believed when the button was pressed,
        which is worth having in the log when they turn out to be stale.
        """
        if not self.connected:
            raise ConnectionError("not connected")

        self._last_id += 1
        self._publish(self.topics.command,
                      protocol.command(self._last_id, command, state, task, **fields))
        return self._last_id

    def close(self):
        if self._client is None:
            return
        client, self._client = self._client, None
        client.publish(self.presence_topic, protocol.encode(protocol.presence(False)),
                       qos=QOS, retain=True)
        client.disconnect()  # flushes the presence message first
        client.loop_stop()

    def _publish(self, topic, payload, retain=False):
        self._client.publish(topic, protocol.encode(payload), qos=QOS, retain=retain)

    # ------------------------------------------------ paho thread from here on
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self.inbox.put((KIND_NOTE, f"broker refused the connection: {reason_code}"))
            return

        client.subscribe([(self.topics.reply, QOS), (self.topics.state, QOS),
                          (self.topics.server, QOS)])
        self._publish(self.presence_topic, protocol.presence(True), retain=True)
        # state and server are retained, so subscribing is enough to learn both

    def _on_message(self, client, userdata, message):
        try:
            payload = protocol.decode(message.payload)
        except ValueError as error:
            self.inbox.put((KIND_NOTE, f"bad payload on {message.topic}: {error}"))
            return

        if message.topic == self.topics.state:
            self.inbox.put((KIND_STATE, payload))
        elif message.topic == self.topics.server:
            self.inbox.put((KIND_SERVER, payload))
        else:
            self.inbox.put((KIND_REPLY, payload))

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code,
                       properties):
        self.inbox.put((KIND_NOTE, NOTE_DISCONNECTED))


class ChainView:
    """Draws Start -> ... -> End for one task and paints the live state.

    Cancel hangs below the row because it is reachable from every state, drawing
    an arrow from each of them would be a mess of lines.
    """

    def __init__(self, parent):
        self.canvas = tk.Canvas(parent, height=CHAIN_HEIGHT, bg=CANVAS_BG,
                                highlightthickness=1, highlightbackground=IDLE_LINE)
        self.canvas.bind("<Configure>", lambda event: self.redraw())
        self.chain = []       # [Start, ...sequence..., End]
        self.task = None
        self.current = None
        self.failed = set()   # states whose recording went wrong this lap

    def show(self, task, sequence):
        self.task = task
        self.chain = [START_STATE] + list(sequence) + [END_STATE]
        self.redraw()

    def set_current(self, state):
        if state == self.current:
            return
        self.current = state
        self.redraw()

    def mark_failed(self, state):
        """Paint a state red and leave it that way until the next lap starts."""
        if state is None or state in self.failed:
            return
        self.failed.add(state)
        self.redraw()

    def clear_failed(self):
        if not self.failed:
            return
        self.failed.clear()
        self.redraw()

    # ------------------------------------------------------------------ drawing
    def redraw(self):
        canvas = self.canvas
        canvas.delete("all")

        width = canvas.winfo_width()
        if width <= 1:  # not laid out yet, <Configure> will call again
            return
        if not self.chain:
            canvas.create_text(width / 2, CHAIN_HEIGHT / 2, fill=IDLE_TEXT,
                               text="no task loaded")
            return

        count = len(self.chain)
        span = width - 2 * CHAIN_MARGIN - BOX_GAP * (count - 1)
        box_width = max(BOX_MIN_WIDTH, span / count)
        top = CHAIN_MARGIN
        middle = top + BOX_HEIGHT / 2

        left = CHAIN_MARGIN
        for index, name in enumerate(self.chain):
            self._box(left, top, left + box_width, top + BOX_HEIGHT, name)
            if index < count - 1:
                canvas.create_line(left + box_width + 3, middle,
                                   left + box_width + BOX_GAP - 3, middle,
                                   arrow="last", fill=ARROW_COLOR, width=2)
            left += box_width + BOX_GAP

        self._draw_cancel(width, top, box_width)
        canvas.create_text(CHAIN_MARGIN, CHAIN_HEIGHT - 10, anchor="w",
                           fill=IDLE_TEXT, text=f"task: {self.task or '-'}")

    def _draw_cancel(self, width, top, box_width):
        """Cancel below the middle, dashed in from the row and back out to Start."""
        canvas = self.canvas
        cancel_top = top + BOX_HEIGHT + CANCEL_DROP
        cancel_left = (width - box_width) / 2
        cancel_right = cancel_left + box_width
        cancel_middle = cancel_top + BOX_HEIGHT / 2

        # from the chain down into Cancel: any state can take this way out
        canvas.create_line(width / 2, top + BOX_HEIGHT, width / 2, cancel_top - 2,
                           arrow="last", fill=ARROW_COLOR, dash=(3, 3))
        # and from Cancel back to Start, around the left
        start_x = CHAIN_MARGIN + box_width / 2
        canvas.create_line(cancel_left - 2, cancel_middle, start_x - box_width / 2 - 6,
                           cancel_middle, start_x - box_width / 2 - 6,
                           top + BOX_HEIGHT / 2, CHAIN_MARGIN - 2, top + BOX_HEIGHT / 2,
                           arrow="last", fill=ARROW_COLOR, dash=(3, 3))

        self._box(cancel_left, cancel_top, cancel_right, cancel_top + BOX_HEIGHT,
                  CANCEL_STATE)

    def _box(self, x0, y0, x1, y1, name):
        # red beats green: a state that failed stays marked even while the
        # machine is still standing in it
        live = name == self.current
        if name in self.failed or (live and name == CANCEL_STATE):
            fill, line, text = ABORT_FILL, ABORT_LINE, LIVE_TEXT
        elif live:
            fill, line, text = LIVE_FILL, LIVE_LINE, LIVE_TEXT
        else:
            fill, line, text = IDLE_FILL, IDLE_LINE, IDLE_TEXT
        live = live or name in self.failed  # bold either way

        self._rounded(x0, y0, x1, y1, fill=fill, outline=line, width=2)
        self.canvas.create_text((x0 + x1) / 2, (y0 + y1) / 2, text=name, fill=text,
                                font=("TkDefaultFont", 9, "bold" if live else "normal"))

    def _rounded(self, x0, y0, x1, y1, **options):
        """Canvas has no rounded rectangle: a smoothed polygon is the usual trick."""
        radius = BOX_RADIUS
        points = [x0 + radius, y0, x1 - radius, y0, x1, y0, x1, y0 + radius,
                  x1, y1 - radius, x1, y1, x1 - radius, y1, x0 + radius, y1,
                  x0, y1, x0, y1 - radius, x0, y0 + radius, x0, y0]
        return self.canvas.create_polygon(points, smooth=True, **options)


class App:
    def __init__(self, root):
        self.root = root
        self.inbox = queue.Queue()
        self.link = MqttLink(self.inbox)
        # reaching the broker is not enough: the state machine has to be up too
        self.server_online = False
        self.last_state = None  # echoed back in every command we send
        self.server_task = None  # the task the server says is loaded, not a local pick
        self.sequences = {}     # task name -> sequence, read from the config
        self.topics = protocol.Topics()  # base comes from the config, see Load

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
        connect_row.pack(fill="x", padx=8, pady=4)

        tk.Label(connect_row, text="broker ip").pack(side="left")
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
                                     command=lambda: self.send(Command.NEXT))
        self.next_button.pack(side="left")
        self.cancel_button = tk.Button(command_row, text="Cancel", width=12,
                                       command=lambda: self.send(Command.CANCEL))
        self.cancel_button.pack(side="left", padx=8)

        self.chain_view = ChainView(self.root)
        self.chain_view.canvas.pack(fill="x", padx=8, pady=4)

        status_row = tk.Frame(self.root)
        status_row.pack(fill="x", padx=8)
        self.state_label = tk.Label(status_row, text="state: -", anchor="w")
        self.state_label.pack(side="left")
        self.server_label = tk.Label(status_row, text="server: -", anchor="e")
        self.server_label.pack(side="right")

        self.status_box = scrolledtext.ScrolledText(self.root, height=12,
                                                    state="disabled", wrap="word")
        self.status_box.pack(fill="both", expand=True, padx=8, pady=8)

        self._refresh_buttons()

    def _refresh_buttons(self):
        """Commands only make sense with a broker AND a state machine behind it.

        Picking a task needs one thing more: the server only swaps a task from
        Start, so outside Start the whole task row is greyed out rather than left
        there to be pressed and refused.
        """
        live = self.link.connected and self.server_online
        usable = "normal" if live else "disabled"
        self.next_button.config(state=usable)
        self.cancel_button.config(state=usable)

        at_start = "normal" if live and self.last_state == START_STATE else "disabled"
        self.task_button.config(state=at_start)
        self.task_menu.config(state=at_start)

        self.connect_button.config(
            text="Disconnect" if self.link.connected else "Connect")

    # ---------------------------------------------------------------- events
    def on_load_config(self):
        """Read the yaml far enough to list the tasks and know their sequences.

        The sequences are only used to draw the chain. What the server runs is
        still the config on its own disk.
        """
        path = Path(self.config_entry.get().strip())
        try:
            raw = yaml.safe_load(path.read_text()) or {}
        except (OSError, yaml.YAMLError) as error:
            self.log(f"cannot read {path}: {error}")
            return

        section = raw.get(KEY_TASK) or {}
        self.sequences = {name: list((spec or {}).get(KEY_SEQUENCE) or [])
                          for name, spec in section.items()}
        tasks = list(self.sequences)
        if not tasks:
            self.log(f"{path}: no '{KEY_TASK}:' section")
            return

        menu = self.task_menu["menu"]
        menu.delete(0, "end")
        for task in tasks:
            menu.add_command(label=task,
                             command=tk._setit(self.task_name, task, self.show_chain))
        # nothing is picked here on purpose: which task is loaded is the server's
        # answer, and it arrives on its own since the state topic is retained
        self.select_task(self.server_task)
        self.log(f"loaded {len(tasks)} task(s) from {path.name}: {', '.join(tasks)}")

    def select_task(self, task):
        """Put a task in the dropdown and draw it. Unknown names leave both alone."""
        if task not in self.sequences:
            return
        self.task_name.set(task)
        self.show_chain(task)

    def show_chain(self, task):
        """Draw a task. Called when one is picked, and when the server says which
        one it is running: the running one wins, the dropdown is only a proposal."""
        if task in self.sequences:
            self.chain_view.show(task, self.sequences[task])

    def on_connect(self):
        if self.link.connected:
            self.link.close()
            self.log("disconnected")
            self._on_server_gone()
            return

        host = self.host_entry.get().strip() or DEFAULT_HOST
        port = self.port_entry.get().strip()
        if not port.isdigit():
            self.log(f"bad port '{port}'")
            return

        try:
            self.link.connect(host, int(port), self.topics)
        except OSError as error:
            self.log(f"cannot reach the broker at {host}:{port}: {error}")
            return

        self.log(f"connected to {host}:{port} as {self.link.client_id}")
        self._refresh_buttons()  # the server topic decides the rest, it is retained

    def on_send_task(self):
        if not self.task_name.get():
            self.log("no task selected, press Load first")
            return
        self.send(Command.TASK)

    def send(self, command):
        """Every command carries the task picked in the window and the last state
        seen, so cmd "task" needs no special case: the task field is already there.
        """
        task = self.task_name.get() or None
        try:
            request_id = self.link.send(command, state=self.last_state, task=task)
        except (OSError, ConnectionError) as error:
            self.log(f"send failed: {error}")
            self._on_server_gone()
            return

        self.log(f"> #{request_id} {command}" + (f" task={task}" if task else ""))

    def drain_inbox(self):
        """Tk main loop: the only place widgets are touched."""
        while True:
            try:
                kind, payload = self.inbox.get_nowait()
            except queue.Empty:
                break

            match kind:
                case k if k == KIND_NOTE:
                    self.log(payload)
                    if payload == NOTE_DISCONNECTED:
                        self._on_server_gone()
                case k if k == KIND_STATE:
                    self.log(protocol.describe(payload))
                    self.last_state = payload.get("state")
                    self.state_label.config(text=f"state: {self.last_state}")
                    self._follow_server_task(payload.get("task"))
                    self.chain_view.set_current(self.last_state)
                    self._on_state_error(payload)
                    self._refresh_buttons()  # the task row follows the state
                case k if k == KIND_SERVER:
                    self._on_server_presence(protocol.is_online(payload))
                case _:
                    self.log(f"< #{payload.get('id', '?')} "
                             f"{protocol.describe(payload)}")

        self.root.after(DRAIN_PERIOD, self.drain_inbox)

    def _on_state_error(self, payload):
        """Recording trouble arrives in info.error, on the state it happened in."""
        error = (payload.get("info") or {}).get("error")
        if error:
            self.chain_view.mark_failed(payload.get("state"))
            self.state_label.config(text=f"state: {self.last_state}  FAILED")
            self.log(f"!! {error}")
        elif payload.get("state") == START_STATE:
            self.chain_view.clear_failed()  # a fresh lap starts with a clean chain

    def _follow_server_task(self, task):
        """Only move the dropdown when the SERVER changed task.

        Every state message carries the running task, and resetting the dropdown
        on each of them would undo a pick the operator just made while standing
        in Start, before pressing Send task.
        """
        self.show_chain(task)
        if task == self.server_task:
            return

        self.server_task = task
        self.select_task(task)

    def _on_server_presence(self, online):
        """The state machine said hello or goodbye (or its last will did)."""
        self.server_online = online
        text = "online" if online else "offline"
        self.log(f"server {text}")
        self.server_label.config(text=f"server: {text}")
        if not online:
            # the retained state is whatever it was when the server died: stale
            self.last_state = None
            self.state_label.config(text="state: -")
            self.chain_view.set_current(None)
        self._refresh_buttons()

    def _on_server_gone(self):
        self.server_online = False
        self.last_state = None
        self.server_task = None  # next connect asks the server again
        self.server_label.config(text="server: -")
        self.state_label.config(text="state: -")
        self.chain_view.set_current(None)
        self._refresh_buttons()

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
    root.protocol("WM_DELETE_WINDOW", lambda: (app.link.close(), root.destroy()))
    root.mainloop()


if __name__ == "__main__":
    main()
