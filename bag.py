"""Recording the ROS topics named in the config: one recorder for a whole lap.

    Start   start `ros2 bag record`, then pause it. The lap has a name from here on
    a state cut a chunk boundary, so this state owns its own file
    End     stop the recorder, the lap is kept
    Cancel  stop and delete the whole lap

One process runs from Start to End rather than one per state, so the recorder is
never restarted in the middle of the work. Splitting is done through rosbag2's
own split_bagfile service, which is what makes per-state files possible without
per-state processes.

Those service calls go through a rclpy client held open, not `ros2 service call`.
Measured on this machine: 1.0 s per subprocess call against 3 ms through a client
that is already connected. They happen in the tick thread, so a second each would
be a second of the state machine standing still.

The naming follows from one process per lap. The LAP folder is
<ddmmyyyy>_<n>_<task>, n being one past the highest the folder already holds.
Inside it every chunk carries the id of the state that wrote it, from the config:

    17082026_4_Normal2/         the lap, and which task it ran
        17082026_3.mcap         Place, id: 3
        17082026_2.mcap         Pick,  id: 2
        17082026_4.mcap         Drop,  id: 4
        states.json             which id was which state, and when
        metadata.yaml

rosbag2 numbers its own chunks <lap>_0, <lap>_1 ... and will not take a name from
us, so the renaming happens once at the end of the lap, when the recorder has
closed everything and written metadata.yaml. metadata.yaml lists those file names
and is patched with the new ones, otherwise `ros2 bag play` finds nothing.

Every lap name goes into `written` the moment recording starts, before it can
possibly succeed. A lap that died halfway still left bytes on the disk, and
Cancel has to be able to delete those too.
"""

import json
import os
import re
import shutil
import signal
import subprocess
import time
from datetime import datetime
from pathlib import Path

import yaml

DEFAULT_ROOT = Path.home() / "bags"
DATE_FORMAT = "%d%m%Y"
NAME_PATTERN = re.compile(r"^(?P<date>\d{8})_(?P<id>\d+)(?:_(?P<task>.+))?$")
FIRST_ID = 1
UNSAFE_IN_NAME = re.compile(r"[^A-Za-z0-9.-]+")  # a task name is free text in yaml
SUBSCRIBED_PATTERN = re.compile(r"Subscribed to topic '([^']+)'")  # from the log

RECORD_COMMAND = ("ros2", "bag", "record", "-o")
LOG_SUFFIX = ".log"  # stderr of one lap, beside its folder
RECORDER_NODE = "rosbag2_recorder"  # prefix for --node-name, see start_lap()
CHUNK_MAP_FILE = "states.json"
METADATA_FILE = "metadata.yaml"  # rosbag2 writes it on close, holds the file names
ID_SEPARATOR = "-"  # two states in one chunk (a split that failed): 17082026_2-3

MIN_FREE_BYTES = 200 * 1024 * 1024  # refuse to start below this, see _check_space
STOP_TIMEOUT = 5.0     # seconds to let ros2 bag close the file after SIGINT
SERVICE_TIMEOUT = 5.0  # the recorder needs about a second to offer its services
CALL_TIMEOUT = 2.0
ERROR_TAIL = 300       # characters of stderr worth showing
GRAPH_GRACE = 1.5      # a rclpy node just created knows of nothing yet, see below


class BagError(RuntimeError):
    """Recording could not start, or died on its own."""


def _canonical(topic):
    """The graph names topics absolutely, the config is allowed to leave the / out."""
    topic = str(topic).strip()
    return topic if topic.startswith("/") else f"/{topic}"


def _stop_signals():
    """Which signals to try, in order, to close a recorder.

    SIGINT is the one rosbag2 documents, and it closes a bag in about a sixth of
    a second. But a process started in the background gets SIGINT set to SIG_IGN
    by the shell, and an ignored disposition is inherited straight through exec:
    the recorder then cannot hear SIGINT at all and every stop costs a five second
    timeout before anything else is tried. When that is the case, lead with
    SIGTERM, which rosbag2 also shuts down on cleanly.
    """
    if signal.getsignal(signal.SIGINT) is signal.SIG_IGN:
        return (signal.SIGTERM, signal.SIGKILL)
    return (signal.SIGINT, signal.SIGTERM, signal.SIGKILL)


class _Services:
    """The recorder's own services, through a rclpy node kept open.

    Built lazily: without ROS on the path this stays asleep and recording still
    works, it just cannot be split.

    The clients are bound to ONE named recorder. That matters more than it looks:
    every `ros2 bag record` is called rosbag2_recorder unless told otherwise, so a
    leftover recorder from an earlier run answers the same service names and there
    is no way to tell whose reply came back. Splits then land in someone else's
    bag and the one we meant to stop never hears the request.
    """

    SERVICES = {"split": "split_bagfile", "pause": "pause", "resume": "resume"}

    def __init__(self):
        self._rclpy = None
        self._node = None
        self._types = None
        self._clients = {}
        self._target = None  # recorder node the clients currently point at
        self._owns_rclpy = False
        self._warned = False

    def bind(self, node_name):
        """Point the clients at one recorder. The node itself is reused."""
        if not self._ensure_node():
            return False
        if self._target == node_name:
            return True

        for client in self._clients.values():
            self._node.destroy_client(client)
        self._clients = {
            key: self._node.create_client(self._types[key], f"/{node_name}/{path}")
            for key, path in self.SERVICES.items()
        }
        self._target = node_name
        return True

    def _ensure_node(self):
        """Costs about half a second, which is why it happens while Start waits."""
        if self._node is not None:
            return True

        try:
            import rclpy
            from rosbag2_interfaces.srv import Pause, Resume, SplitBagfile
        except ImportError as error:
            if not self._warned:  # once per run, not once per state
                print(f"[warn    ] no rclpy ({error}), bags will not be split")
                self._warned = True
            return False

        if not rclpy.ok():
            rclpy.init()
            self._owns_rclpy = True

        self._rclpy = rclpy
        self._node = rclpy.create_node(f"state_machine_bag_{os.getpid()}")
        self._types = {"split": SplitBagfile, "pause": Pause, "resume": Resume}
        return True

    def call(self, key):
        """Fire one service and wait for it. False on any kind of no."""
        if self._node is None or not self._clients:
            return False

        client = self._clients[key]
        if not client.service_is_ready():
            # a fresh recorder takes a moment to advertise, and every lap starts
            # a new one, so waiting here is normal rather than an error
            if not client.wait_for_service(timeout_sec=SERVICE_TIMEOUT):
                print(f"[warn    ] {self._target}/{key} did not answer in "
                      f"{SERVICE_TIMEOUT}s")
                return False

        future = client.call_async(self._types[key].Request())
        self._rclpy.spin_until_future_complete(self._node, future,
                                               timeout_sec=CALL_TIMEOUT)
        if not future.done():
            print(f"[warn    ] {self._target}/{key} timed out")
            return False
        return True

    def graph_topics(self):
        """Topic names the ROS graph knows right now, None if there is no rclpy.

        Read from the local discovery cache, not a service call, so asking on
        every state costs nothing worth measuring.
        """
        if self._node is None:
            return None
        return {name for name, _types in self._node.get_topic_names_and_types()}

    def close(self):
        if self._node is not None:
            self._node.destroy_node()
            self._node = None
            self._clients = {}
            self._target = None
        if self._owns_rclpy and self._rclpy is not None and self._rclpy.ok():
            self._rclpy.shutdown()
            self._owns_rclpy = False


class BagRecorder:
    def __init__(self, topics=(), root=DEFAULT_ROOT):
        self.configure(topics, root)
        self.written = []      # lap names started since the last reset, oldest first
        self.last_error = None
        self._process = None
        self._name = None
        self._log = None
        self._log_path = None
        self._chunks = []      # [{"chunk": 0, "state": "Pick", "at": "..."}]
        self._services = _Services()

    def configure(self, topics, root):
        self.topics = [str(topic) for topic in (topics or [])]
        self.root = Path(root).expanduser()

    @property
    def enabled(self):
        """No topics in the config means this whole file does nothing."""
        return bool(self.topics)

    @property
    def recording(self):
        return self._process is not None

    # -------------------------------------------------------------- the lap
    def reset(self):
        """A new lap begins: forget the previous one, its bags are keepers now."""
        if self.written:
            print(f"[bag     ] keeping {len(self.written)} lap(s) already recorded")
        self.written = []
        self.last_error = None

    def discard(self):
        """Delete every lap of this run. Only ever touches names we wrote."""
        for name in self.written:
            target = self.root / name
            try:
                shutil.rmtree(target)
                print(f"[bag     ] deleted {target}")
            except FileNotFoundError:
                pass  # never got created, nothing to do
            except OSError as error:
                print(f"[warn    ] cannot delete {target}: {error}")
            (self.root / f"{name}{LOG_SUFFIX}").unlink(missing_ok=True)
        self.written = []

    # ------------------------------------------------------------ recording
    def start_lap(self, task=None):
        """Launch the recorder for a whole lap. Raises BagError if it cannot."""
        if not self.enabled:
            return None
        if self.recording:
            self.stop_lap()  # should not happen, but never leave one running

        self.root.mkdir(parents=True, exist_ok=True)
        self._check_space()

        name = self.next_name(task)
        # a name of our own, so the service calls below cannot land on someone
        # else's recorder: every ros2 bag record is rosbag2_recorder by default
        node_name = f"{RECORDER_NODE}_{os.getpid()}_{len(self.written)}"
        command = [*RECORD_COMMAND, str(self.root / name),
                   "--node-name", node_name,
                   # it reads stdin for its own keyboard controls, which is the
                   # same stdin the operator types Enter into. Without this the
                   # two fight over every keypress, and a stdin that later closes
                   # leaves the recorder unable to shut down in time.
                   "--disable-keyboard-controls",
                   "--start-paused",  # nothing to record until Start is left
                   *self.topics]
        # stderr goes to a file, never to a pipe. A pipe nobody reads fills up
        # after 64 KB and the recorder then blocks inside its own logging, deaf
        # to SIGINT: the lap cannot be closed and the bag needs reindexing.
        log_path = self.root / f"{name}{LOG_SUFFIX}"
        try:
            log_file = open(log_path, "wb")
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.DEVNULL,
                                       stderr=log_file)
        except FileNotFoundError as error:
            raise BagError("'ros2' not found, has the ROS setup been sourced?") \
                from error
        except OSError as error:
            raise BagError(f"cannot run ros2 bag record: {error}") from error

        # remembered before it can succeed: a half written lap is still ours
        self.written.append(name)
        self._process = process
        self._log = log_file
        self._log_path = log_path
        self._name = name
        self._chunks = []
        self.last_error = None
        print(f"[bag     ] lap {name} <- {len(self.topics)} topic(s)")

        self._services.bind(node_name)  # the half second it costs is free here
        return name

    def split_for(self, state, state_id):
        """Cut the file so `state` gets its own chunk.

        `state_id` is the state's own id from the config, carried here by the
        machine; it becomes the chunk's name at the end of the lap.
        """
        if not self.recording:
            return

        if not self._chunks:
            index = 0  # the file the recorder opened is still empty, use it
        elif self._services.call("split"):
            index = self._chunks[-1]["chunk"] + 1
        else:
            print(f"[warn    ] no split for {state}, it shares the previous file")
            index = self._chunks[-1]["chunk"]

        # what the graph looked like at this exact moment, so a lap that came back
        # half empty can be explained afterwards instead of guessed at
        self._chunks.append({"chunk": index, "state": state, "id": state_id,
                             "at": datetime.now().astimezone().isoformat(),
                             "missing": self.missing_topics()})
        self._write_chunk_map()
        print(f"[bag     ] {self._name}: chunk {index} <- {state} (id {state_id})")

    def pause(self):
        """Stop writing without stopping the recorder. A lap begins paused
        already (--start-paused), this is for pausing it again later."""
        if self.recording:
            self._services.call("pause")

    def resume(self):
        if self.recording:
            self._services.call("resume")

    def stop_lap(self):
        """Close the lap. SIGINT is what ros2 bag listens for; without a clean
        close there is no metadata.yaml and the bag needs reindexing."""
        if not self.recording:
            return

        process, name = self._process, self._name
        self._process = self._name = None
        self._close_log()

        if process.poll() is None:
            # the first one normally closes the bag in well under a second. The
            # rest is for a recorder that is truly stuck, and a bag left that way
            # needs ros2 bag reindex.
            for sig in _stop_signals():
                process.send_signal(sig)
                try:
                    process.wait(timeout=STOP_TIMEOUT)
                    break
                except subprocess.TimeoutExpired:
                    print(f"[warn    ] {name} ignored {sig.name} for "
                          f"{STOP_TIMEOUT}s, escalating")

        print(f"[bag     ] stopped {name}, {len(self._chunks)} chunk(s)")
        never = self.never_subscribed()
        if never:
            print(f"[warn    ] {name}: never recorded {', '.join(never)}, "
                  f"the topic never appeared or its QoS did not match")
        self._name_chunks_after_states(name)

    def missing_topics(self, grace=0.0):
        """Configured topics that nobody is publishing at this moment.

        `grace` is for the one call right after the node is created: discovery is
        asynchronous and a fresh node knows of nothing for a few hundred
        milliseconds, so asking straight away reports every topic as missing.
        Waiting only happens while something IS missing, and the answer is
        returned the moment the list comes back empty.

        `ros2 bag record` says nothing about a topic that does not exist: it keeps
        polling for it, records nothing, and still exits cleanly leaving a valid
        empty bag. A typo in the config would otherwise surface days later, when
        somebody opens the bag looking for data that was never there.
        """
        if not self.enabled:
            return []

        deadline = time.monotonic() + grace
        while True:
            present = self._services.graph_topics()
            if present is None:
                return []  # no rclpy, no way to know, better silent than crying wolf
            missing = [topic for topic in self.topics
                       if _canonical(topic) not in present]
            if not missing or time.monotonic() >= deadline:
                return missing
            time.sleep(0.1)

    def never_subscribed(self):
        """Topics the recorder itself never said it subscribed to, over the lap.

        Catches what missing_topics() cannot: a topic that does exist but whose
        QoS the recorder cannot match. There is no error for that either, the
        subscription is simply never made and no message ever arrives.
        """
        if self._log_path is None:
            return []
        try:
            log = self._log_path.read_text(errors="replace")
        except OSError:
            return []
        seen = set(SUBSCRIBED_PATTERN.findall(log))
        return [topic for topic in self.topics if _canonical(topic) not in seen]

    def trouble(self):
        """Error text if the recorder died by itself, None while all is well.

        Called every tick: a disk that fills up mid-lap shows up here, not at the
        next start_lap().
        """
        if not self.recording or self._process.poll() is None:
            return None

        process, name = self._process, self._name
        self._process = self._name = None
        self._close_log()
        reason = self._read_error() or f"exit code {process.returncode}"
        self.last_error = f"recording {name} stopped on its own: {reason}"
        return self.last_error

    # --------------------------------------------------------------- naming
    def next_name(self, task=None):
        """<ddmmyyyy>_<n>_<task>, n one past the highest the folder already holds.

        The counter is what keeps two laps of the same task on the same day apart,
        the task is there so the folder says what was being recorded without
        having to open states.json.
        """
        name = f"{datetime.now().strftime(DATE_FORMAT)}_{self._next_id()}"
        if task:
            name += f"_{UNSAFE_IN_NAME.sub('_', str(task)).strip('_')}"
        return name

    def _next_id(self):
        used = []
        for entry in self.root.iterdir() if self.root.is_dir() else []:
            match = NAME_PATTERN.match(entry.name)
            if match:
                used.append(int(match["id"]))
        return max(used) + 1 if used else FIRST_ID

    def _name_chunks_after_states(self, lap):
        """Rename every chunk to <ddmmyyyy>_<state id>, once the lap is closed.

        Has to wait until here: rosbag2 keeps the files open and only writes
        metadata.yaml when it stops, and metadata.yaml is where the names it will
        look for later are listed. Renaming without patching that file leaves a
        bag that no tool can open.

        A lap that died on its own has no metadata.yaml. Its chunks are left
        alone, numbered: that bag needs `ros2 bag reindex` before anything, and
        renaming underneath it would only make the reindex harder.
        """
        folder = self.root / lap
        meta_path = folder / METADATA_FILE
        try:
            text = meta_path.read_text()
            paths = yaml.safe_load(text)["rosbag2_bagfile_information"] \
                                        ["relative_file_paths"]
        except (OSError, KeyError, TypeError, yaml.YAMLError) as error:
            print(f"[warn    ] {lap}: cannot read {METADATA_FILE} ({error}), "
                  f"chunks keep their numbers")
            return

        date = lap.split("_")[0]
        # a split that failed leaves two states in one chunk, they share a name
        ids = {}
        for record in self._chunks:
            ids.setdefault(record["chunk"], []).append(str(record["id"]))

        taken = set()
        for index, chunk_ids in sorted(ids.items()):
            if index >= len(paths):
                continue  # rosbag2 dropped an empty chunk, nothing to rename
            old = paths[index]
            new = f"{date}_{ID_SEPARATOR.join(chunk_ids)}{Path(old).suffix}"
            if new == old or new in taken:
                continue  # same id twice in one lap: leave the second alone
            try:
                (folder / old).rename(folder / new)
            except OSError as error:
                print(f"[warn    ] {lap}: cannot rename {old}: {error}")
                continue
            text = text.replace(old, new)
            taken.add(new)
            for record in self._chunks:
                if record["chunk"] == index:
                    record["file"] = new

        try:
            meta_path.write_text(text)
        except OSError as error:
            print(f"[warn    ] {lap}: cannot patch {METADATA_FILE}: {error}")
        self._write_chunk_map(lap)
        print(f"[bag     ] {lap}: " + ", ".join(
            record.get("file", "?") + " = " + record["state"]
            for record in self._chunks))

    def _write_chunk_map(self, lap=None):
        """states.json beside the bag: rosbag2 numbers the chunks, this says which
        state each number was, and what its file ended up being called."""
        lap = lap or self._name
        if lap is None:
            return
        target = self.root / lap / CHUNK_MAP_FILE
        try:
            target.write_text(json.dumps(self._chunks, indent=2))
        except OSError as error:
            print(f"[warn    ] cannot write {target}: {error}")

    # --------------------------------------------------------------- checks
    def _check_space(self):
        try:
            free = shutil.disk_usage(self.root).free
        except OSError as error:
            raise BagError(f"cannot read free space on {self.root}: {error}") from error

        if free < MIN_FREE_BYTES:
            raise BagError(f"no space left on {self.root}: "
                           f"{free // (1024 * 1024)} MB free, "
                           f"{MIN_FREE_BYTES // (1024 * 1024)} MB needed")

    def _close_log(self):
        if self._log is not None:
            self._log.close()
            self._log = None

    def _read_error(self):
        """The tail of what the recorder printed before giving up."""
        if self._log_path is None:
            return ""
        try:
            return self._log_path.read_text(errors="replace").strip()[-ERROR_TAIL:]
        except OSError:
            return ""


# One recorder for the whole program, the way CALLBACK_MAP and the request flags
# are one. main() fills it in from the config at startup.
RECORDER = BagRecorder()


def shutdown():
    """Close the recorder on the way out of the program.

    Not optional: a recorder killed with its process leaves no metadata.yaml, and
    the chunk it had open at that moment ends up zero bytes. The work of the last
    state is simply gone.
    """
    RECORDER.stop_lap()
    RECORDER._services.close()


def configure(topics, root):
    RECORDER.configure(topics, root)
    if RECORDER.enabled:
        print(f"[bag     ] {len(RECORDER.topics)} topic(s) -> {RECORDER.root}")
    else:
        print("[bag     ] no topic in the config, recording is off")
