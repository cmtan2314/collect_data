"""Recording the ROS topics named in the config: one recorder for a whole lap.

    Start   build the recorder, paused. The lap has a name from here on
    a state cut a chunk boundary, so this state owns its own file
    End     stop the recorder, the lap is kept
    Cancel  stop and delete the whole lap

The recorder is rosbag2's own C++ recorder driven through rosbag2_py, living in
this process. Not `ros2 bag record` as a subprocess: that meant talking to it
through signals (and an ignored SIGINT is inherited through exec, so a server
started in the background could not stop it at all), fighting it over stdin,
parsing its log to learn what it had subscribed to, and watching the bag folder
grow to guess when it was safe to kill. Here stop() is a method that returns when
it is done, and every failure is an exception with a message.

One recorder runs from Start to End rather than one per state, so nothing is
restarted in the middle of the work. Splitting goes through the recorder's own
split_bagfile service - the one thing the Python class does not expose - which is
what makes per-state files possible without per-state recorders.

include_unpublished_topics is on. A topic that nobody publishes yet (a controller
input waiting for a teleop node, say) is otherwise skipped in silence: rosbag2
takes the type and QoS from a publisher, and with none there is nothing for it to
subscribe to. With the flag such a topic is recorded from the first message on,
and shows up in the bag with a count of zero if it never speaks - so every lap
holds the same set of topics whether the robot was fully up or not.

The naming follows from one recorder per lap. The LAP folder is
<ddmmyyyy>_<n>_<task>, n being one past the highest the folder already holds.
Inside it every chunk carries the id of the state that wrote it, from the config:

    17082026_4_Normal2/         the lap, and which task it ran
        17082026_3.mcap         Place, id: 3
        17082026_2.mcap         Pick,  id: 2
        17082026_4.mcap         Drop,  id: 4
        states.json             which id was which state, and when
        metadata.yaml

rosbag2 numbers its own chunks <lap>_0, <lap>_1 ... and will not take a name from
us, so the renaming happens once at the end of the lap, when everything is closed
and metadata.yaml is written. metadata.yaml lists those file names and is patched
with the new ones, otherwise `ros2 bag play` finds nothing.

Every lap name goes into `written` the moment recording starts, before it can
possibly succeed. A lap that died halfway still left bytes on the disk, and
Cancel has to be able to delete those too.
"""

import json
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import yaml

DEFAULT_ROOT = Path.home() / "bags"
DATE_FORMAT = "%d%m%Y"
NAME_PATTERN = re.compile(r"^(?P<date>\d{8})_(?P<id>\d+)(?:_(?P<task>.+))?$")
FIRST_ID = 1
UNSAFE_IN_NAME = re.compile(r"[^A-Za-z0-9.-]+")  # a task name is free text in yaml

STORAGE_ID = "mcap"
SERIALIZATION = "cdr"
RECORDER_NODE = "state_machine_recorder"
CHUNK_MAP_FILE = "states.json"
METADATA_FILE = "metadata.yaml"  # rosbag2 writes it on close, holds the file names
ID_SEPARATOR = "-"  # two states in one chunk (a split that failed): 17082026_2-3

# How often the recorder looks for topics it has not subscribed to yet. rosbag2
# defaults to 100 ms, which is also how long a topic coming up mid-lap goes
# unrecorded - measured: a publisher appearing after 10 s of waiting lost its
# first 2 messages of 100. At 20 ms that window is a fifth of that.
POLLING_INTERVAL = timedelta(milliseconds=20)

# Buffer in RAM before writing. rosbag2 defaults to 100 MiB, which three raw
# camera streams fill in about a second; a big cache rides out the bursts. It is
# RAM, so keep it well under what the board has (free -g), and remember a cache
# is lost if the process is killed - it trades dropped messages under load for a
# bigger hole when something goes wrong.
MAX_CACHE_BYTES = 20 * 1024 * 1024 * 1024

# Refuse to start below this, and shout if a running lap crosses it. Three raw
# camera streams write faster than most people expect, and by the time recording
# dies of a full disk the bag it was writing is already unusable.
MIN_FREE_BYTES = 10 * 1024 * 1024 * 1024
SPACE_CHECK_PERIOD = 5.0  # seconds between disk checks while recording

SERVICE_TIMEOUT = 5.0  # the recorder needs a moment to advertise split_bagfile
CALL_TIMEOUT = 2.0
GRAPH_GRACE = 5.0  # see missing_topics(): a fresh node knows of nothing at first


class BagError(RuntimeError):
    """Recording could not start, or died on its own."""


def _canonical(topic):
    """The graph names topics absolutely, the config is allowed to leave the / out."""
    topic = str(topic).strip()
    return topic if topic.startswith("/") else f"/{topic}"


class _Ros:
    """Everything that needs rclpy: the split service, and reading the graph.

    Built lazily, so that without ROS on the path importing this module still
    works and the state machine runs with recording switched off.

    The split client is bound to ONE named recorder. That matters more than it
    looks: a leftover recorder from an earlier run answers the same service name,
    and there is no way to tell whose reply came back - splits would land in
    someone else's bag while ours never hears the request.
    """

    def __init__(self):
        self._rclpy = None
        self._node = None
        self._split_type = None
        self._client = None
        self._target = None
        self._owns_rclpy = False
        self._warned = False

    def ensure(self):
        """Costs about half a second, which is why it happens while Start waits."""
        if self._node is not None:
            return True

        try:
            import rclpy
            from rosbag2_interfaces.srv import SplitBagfile
        except ImportError as error:
            if not self._warned:  # once per run, not once per state
                print(f"[warn    ] no rclpy ({error}), recording is off")
                self._warned = True
            return False

        if not rclpy.ok():
            rclpy.init()
            self._owns_rclpy = True

        self._rclpy = rclpy
        self._node = rclpy.create_node(f"{RECORDER_NODE}_client")
        self._split_type = SplitBagfile
        return True

    def bind(self, node_name):
        """Point the split client at one recorder. The node itself is reused."""
        if not self.ensure() or self._target == node_name:
            return
        if self._client is not None:
            self._node.destroy_client(self._client)
        self._client = self._node.create_client(self._split_type,
                                                f"/{node_name}/split_bagfile")
        self._target = node_name

    def split(self):
        """Cut the bag at this instant. False on any kind of no."""
        if self._client is None:
            return False

        if not self._client.service_is_ready():
            # a recorder that has just come up takes a moment to advertise, and
            # every lap starts a new one, so waiting here is normal
            if not self._client.wait_for_service(timeout_sec=SERVICE_TIMEOUT):
                print(f"[warn    ] {self._target}/split_bagfile did not answer in "
                      f"{SERVICE_TIMEOUT}s")
                return False

        future = self._client.call_async(self._split_type.Request())
        self._rclpy.spin_until_future_complete(self._node, future,
                                               timeout_sec=CALL_TIMEOUT)
        if not future.done():
            print(f"[warn    ] {self._target}/split_bagfile timed out")
            return False
        return True

    def graph_topics(self):
        """Topics with at least one PUBLISHER right now, None if there is no rclpy.

        Publishers, not mere existence. A topic that only has subscribers is
        listed by `ros2 topic list` and by get_topic_names_and_types() all the
        same, and until include_unpublished_topics was turned on rosbag2 skipped
        exactly those. It still means nobody is sending anything, which is worth
        telling the operator about.
        """
        if self._node is None:
            return None
        return {name for name, _types in self._node.get_topic_names_and_types()
                if self._node.count_publishers(name) > 0}

    def close(self):
        if self._node is not None:
            self._node.destroy_node()
            self._node = None
            self._client = None
            self._target = None
        if self._owns_rclpy and self._rclpy is not None and self._rclpy.ok():
            self._rclpy.shutdown()
            self._owns_rclpy = False


class BagRecorder:
    def __init__(self, topics=(), root=DEFAULT_ROOT):
        self.configure(topics, root)
        self.written = []      # lap names started since the last reset, oldest first
        self.last_error = None
        self._recorder = None
        self._name = None
        self._chunks = []      # [{"chunk": 0, "state": "Pick", "id": 2, ...}]
        self._ros = _Ros()
        self._next_space_check = 0.0
        self._space_warned = False

    def configure(self, topics, root):
        self.topics = [str(topic).strip() for topic in (topics or [])]
        self.root = Path(root).expanduser()

    @property
    def enabled(self):
        """No topics in the config means this whole file does nothing."""
        return bool(self.topics)

    @property
    def recording(self):
        return self._recorder is not None

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
        self.written = []

    # ------------------------------------------------------------ recording
    def start_lap(self, task=None):
        """Build the recorder for a whole lap. Raises BagError if it cannot."""
        if not self.enabled:
            return None
        if self.recording:
            self.stop_lap()  # should not happen, but never leave one running

        self.root.mkdir(parents=True, exist_ok=True)
        self._check_space()
        if not self._ros.ensure():
            raise BagError("rclpy is not available, cannot record")

        try:
            import rosbag2_py
        except ImportError as error:
            raise BagError(f"rosbag2_py is not available: {error}") from error

        name = self.next_name(task)
        # a name of our own, so the split call cannot land on someone else's
        # recorder: every rosbag2 recorder is rosbag2_recorder by default
        node_name = f"{RECORDER_NODE}_{len(self.written)}_{int(time.time())}"
        self.written.append(name)  # remembered before it can succeed

        storage = rosbag2_py.StorageOptions(uri=str(self.root / name),
                                            storage_id=STORAGE_ID,
                                            max_cache_size=MAX_CACHE_BYTES)
        options = rosbag2_py.RecordOptions()
        options.topics = self.topics
        options.rmw_serialization_format = SERIALIZATION
        options.topic_polling_interval = POLLING_INTERVAL
        options.include_unpublished_topics = True  # see the module docstring
        options.start_paused = True  # nothing to record until Start is left
        options.disable_keyboard_controls = True  # the operator's stdin is ours

        try:
            recorder = rosbag2_py.Recorder(storage, options, "info", node_name)
            recorder.record()
            recorder.start_spin()
        except Exception as error:  # rosbag2_py raises plain RuntimeError
            raise BagError(f"cannot start recording: {error}") from error

        self._recorder = recorder
        self._name = name
        self._chunks = []
        self.last_error = None
        self._next_space_check = 0.0
        self._space_warned = False
        print(f"[bag     ] lap {name} <- {len(self.topics)} topic(s)")

        self._ros.bind(node_name)
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
        elif self._ros.split():
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
        already (start_paused), this is for pausing it again later."""
        if self.recording:
            self._recorder.pause()

    def resume(self):
        if self.recording:
            self._recorder.resume()

    def stop_lap(self):
        """Close the lap.

        stop() writes out whatever is still in the cache and returns when the bag
        is closed and metadata.yaml is on disk. With a large MAX_CACHE_BYTES that
        can take a moment; it is not optional, a bag closed any other way has no
        metadata.yaml and needs `ros2 bag reindex` before anything can read it.
        """
        if not self.recording:
            return

        recorder, name = self._recorder, self._name
        self._recorder = self._name = None
        try:
            recorder.stop()
        except Exception as error:
            print(f"[warn    ] {name} did not stop cleanly: {error}")

        print(f"[bag     ] stopped {name}, {len(self._chunks)} chunk(s)")
        self._finish_lap(name)

    def trouble(self):
        """Error text if recording has failed on its own, None while all is well.

        Called every tick. With the recorder inside this process there is no exit
        code to watch, so what is checked is the bag itself: a lap whose folder
        never appeared is a lap that never really started.
        """
        if not self.recording:
            return None
        if (self.root / self._name).exists():
            return None

        name = self._name
        self._recorder = self._name = None
        self.last_error = f"recording {name} left nothing on disk"
        return self.last_error

    def space_warning(self):
        """Said once, when a running lap drops under the free space floor.

        _check_space() only guards the start of a lap. Disk is consumed while the
        lap runs, and recording dying of a full disk takes the bag with it, so the
        operator needs to hear about it while there is still time to walk the
        machine to End and keep what has been written.
        """
        if not self.recording or self._space_warned:
            return None

        now = time.monotonic()
        if now < self._next_space_check:
            return None
        self._next_space_check = now + SPACE_CHECK_PERIOD

        try:
            free = shutil.disk_usage(self.root).free
        except OSError:
            return None
        if free >= MIN_FREE_BYTES:
            return None

        self._space_warned = True
        return (f"{free // (1024 * 1024)} MB left on {self.root}, under the "
                f"{MIN_FREE_BYTES // (1024 * 1024 * 1024)} GB floor: go to End "
                f"now to keep this lap")

    def missing_topics(self, grace=0.0):
        """Configured topics that nobody is publishing at this moment.

        These ARE recorded now (include_unpublished_topics), so this is no longer
        about losing the topic entirely - it is about the operator learning that a
        node is not up before spending a lap on it, since a topic nobody publishes
        records nothing.

        `grace` is for the one call right after the node is created: discovery is
        asynchronous and a fresh node knows of nothing for a moment. Measured on
        the robot, controllers appear after 0.65 s and cameras after 3.1 s, so
        asking straight away reports healthy topics as missing. Waiting only
        happens while something IS missing, and the answer comes back the instant
        the list is empty.
        """
        if not self.enabled:
            return []

        deadline = time.monotonic() + grace
        while True:
            present = self._ros.graph_topics()
            if present is None:
                return []  # no rclpy, no way to know, better silent than crying wolf
            missing = [topic for topic in self.topics
                       if _canonical(topic) not in present]
            if not missing or time.monotonic() >= deadline:
                return missing
            time.sleep(0.1)

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

    # ---------------------------------------------------- end of a lap
    def _finish_lap(self, lap):
        """Rename the chunks after their states, and say what stayed empty.

        Has to wait until the lap is closed: rosbag2 keeps the files open and only
        writes metadata.yaml when it stops, and metadata.yaml is where the names
        it will look for later are listed. Renaming without patching that file
        leaves a bag no tool can open.

        A lap that died has no metadata.yaml. Its chunks are left alone, numbered:
        that bag needs `ros2 bag reindex` first, and renaming underneath it would
        only make the reindex harder.
        """
        folder = self.root / lap
        meta_path = folder / METADATA_FILE
        try:
            text = meta_path.read_text()
            meta = yaml.safe_load(text)["rosbag2_bagfile_information"]
            paths = meta["relative_file_paths"]
        except (OSError, KeyError, TypeError, yaml.YAMLError) as error:
            print(f"[warn    ] {lap}: cannot read {METADATA_FILE} ({error}), "
                  f"chunks keep their numbers")
            return

        self._report_empty(lap, meta)

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

    def _report_empty(self, lap, meta):
        """Topics that ended the lap without a single message.

        They are in the bag either way now, which is what keeps every lap holding
        the same set of topics - but a count of zero still means nobody said
        anything on it for the whole run, and that is worth a line.
        """
        counts = {}
        for entry in meta.get("topics_with_message_count") or []:
            counts[entry["topic_metadata"]["name"]] = entry["message_count"]

        empty = [topic for topic in self.topics
                 if counts.get(_canonical(topic), 0) == 0]
        if empty:
            print(f"[warn    ] {lap}: not one message on {', '.join(empty)}")

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
                           f"{MIN_FREE_BYTES // (1024 * 1024 * 1024)} GB needed")


# One recorder for the whole program, the way CALLBACK_MAP and the request flags
# are one. main() fills it in from the config at startup.
RECORDER = BagRecorder()


def shutdown():
    """Close the recorder on the way out of the program.

    Not optional: the recorder lives in this process now, so leaving without
    stop() means the bag is never closed - no metadata.yaml, and whatever was in
    the cache is gone with the process.
    """
    RECORDER.stop_lap()
    RECORDER._ros.close()


def configure(topics, root):
    RECORDER.configure(topics, root)
    if RECORDER.enabled:
        print(f"[bag     ] {len(RECORDER.topics)} topic(s) -> {RECORDER.root}")
    else:
        print("[bag     ] no topic in the config, recording is off")
