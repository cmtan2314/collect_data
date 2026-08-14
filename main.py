"""State machine configured from a yaml file.

The chain follows the TASK, not the order the states are written in the file:

    Start -> [sequence of the task] -> End -> Start
    Cancel: from any state -> Cancel -> Start

Every state holds a table transitions[task] = name of the next state, e.g.

    Pick.transitions  = {"Normal": "Place",  "Normal2": "Drop"}
    Place.transitions = {"Normal": "End",    "Normal2": "Pick"}

A machine runs exactly one task. Switching task means calling create_machine()
again (which re-reads the config from disk); two tasks never run side by side.
That is also why a state may appear only once inside a task: otherwise Pick would
have two outgoing edges for the same event.

enter / progress / exit are not methods on the state, they are callback NAMES
resolved in CALLBACK_MAP at RUNTIME on every call, so register_callback() works
halfway through a run without reloading the config or rebuilding the machine.

Events: progress() never switches state itself, it returns an event for tick()
    Event.NEXT   -> transitions[task]
    Event.CANCEL -> the Cancel state
    Event.NONE   -> stay, run progress again next tick
"""

import threading
import time
from pathlib import Path

import yaml
from statemachine import State, StateMachine
from statemachine.factory import StateMachineMetaclass

import custom_callbacks
from callbacks import (DEFAULT_ENTER, DEFAULT_EXIT, DEFAULT_PROGRESS,
                       has_callback, resolve_callback)
from events import (SHUTDOWN, CancelRequest, Event, NextRequest, Request, ask)
from server import Server

CONFIG_PATH = Path(__file__).parent / "config" / "config_example.yaml"

START_STATE = "Start"
END_STATE = "End"
CANCEL_STATE = "Cancel"
FIXED_STATES = (START_STATE, END_STATE, CANCEL_STATE)

EVENT_NEXT = "next"
EVENT_CANCEL = "cancel"

KEY_TASK = "Task"  # reserved top-level key, every other top-level key is a state
KEY_SEQUENCE = "sequence"
KEY_ENTER = "enter"
KEY_PROGRESS = "progress"
KEY_EXIT = "exit"
KEY_ID = "id"

# callback field of a state -> default callback name when the config leaves it out
CALLBACK_KEYS = {
    KEY_ENTER: DEFAULT_ENTER,
    KEY_PROGRESS: DEFAULT_PROGRESS,
    KEY_EXIT: DEFAULT_EXIT,
}
DEFAULT_NAME = "default"  # shorthand: the default callback of that very field
DEFAULT_ID = 0


# ==================================================================== state
class StateContext:
    """Runtime half of a state: id, callback names, per-task transitions, scratch data."""

    def __init__(self, name, state_id, callbacks):
        if not name:
            raise ValueError("a state needs a name")
        self.name = name
        self.id = state_id
        self.callbacks = callbacks  # {enter: name, progress: name, exit: name}
        self.transitions = {}       # task -> name of the next state
        self.data = {}              # scratch space for callbacks, kept across ticks

    def link(self, task, next_state):
        """Wire this state -> next_state within one task."""
        current = self.transitions.get(task)
        if current is not None and current != next_state:
            raise ValueError(
                f"task '{task}': '{self.name}' goes to both '{current}' and "
                f"'{next_state}'. A state may appear only once in a sequence"
            )
        self.transitions[task] = next_state

    def next_state(self, task):
        next_name = self.transitions.get(task)
        if next_name is None:
            raise KeyError(f"'{self.name}' is not part of task '{task}'")
        return next_name

    def _run(self, key, default_name):
        return resolve_callback(self.callbacks[key], default_name)(self)

    def enter(self):
        print(f"[enter   ] {self.name}")
        self._run(KEY_ENTER, DEFAULT_ENTER)

    def exit(self):
        print(f"[exit    ] {self.name}")
        self._run(KEY_EXIT, DEFAULT_EXIT)

    def progress(self):
        event = self._run(KEY_PROGRESS, DEFAULT_PROGRESS)
        if event is None:
            event = Event.NEXT  # empty callback = nothing to do, move on
        if event != Event.NONE:
            # NONE happens on every tick while waiting, logging it would flood
            print(f"[progress] {self.name}: id={self.id} -> {event}")
        return event


# =================================================================== config
def load_config(path=CONFIG_PATH):
    """Read the yaml into (states, tasks); both still hold plain strings/numbers.

    states = {name: {enter, progress, exit, id}}
    tasks  = {name: [state, ...]}
    """
    if not path.exists():
        raise FileNotFoundError(f"config not found: {path}")

    raw = yaml.safe_load(path.read_text())
    if not raw:
        raise ValueError(f"{path}: empty config")

    raw_tasks = raw.pop(KEY_TASK, None)  # pop, so everything left is a state
    if not isinstance(raw_tasks, dict) or not raw_tasks:
        raise ValueError(f"{path}: missing '{KEY_TASK}:' section, or no task in it")

    states = {name: _read_state(path, name, spec) for name, spec in raw.items()}
    for required in FIXED_STATES:
        if required not in states:
            raise ValueError(f"{path}: missing state '{required}'")

    tasks = {name: _read_task(path, name, spec, states)
             for name, spec in raw_tasks.items()}
    return states, tasks


def _read_state(path, name, spec):
    if not isinstance(spec, dict):
        raise ValueError(
            f"{path}: state '{name}' must be a mapping, got "
            f"{type(spec).__name__}. Mind the space after ':' (enter: default)"
        )

    if spec.get(KEY_ID) is None:
        print(f"[config  ] {name}: no '{KEY_ID}', using {DEFAULT_ID}")
    state_cfg = {KEY_ID: spec.get(KEY_ID, DEFAULT_ID)}

    for key, default_name in CALLBACK_KEYS.items():
        callback_name = spec.get(key)  # left blank in yaml -> None -> treat as missing
        if callback_name is None:
            print(f"[config  ] {name}: no '{key}', using '{default_name}'")
            callback_name = default_name
        elif callback_name == DEFAULT_NAME:
            callback_name = default_name
        elif not has_callback(callback_name):
            print(f"[config  ] {name}.{key}: '{callback_name}' not registered yet, "
                  f"will be resolved at runtime")
        state_cfg[key] = callback_name

    return state_cfg


def _read_task(path, name, spec, states):
    if not isinstance(spec, dict) or KEY_SEQUENCE not in spec:
        raise ValueError(f"{path}: task '{name}' needs a '{KEY_SEQUENCE}:'")

    sequence = spec[KEY_SEQUENCE]
    if not isinstance(sequence, list) or not sequence:
        raise ValueError(f"{path}: task '{name}': '{KEY_SEQUENCE}' must be a "
                         f"non-empty list")

    for state_name in sequence:
        if state_name not in states:
            raise ValueError(f"{path}: task '{name}': no such state '{state_name}'")
        if state_name in FIXED_STATES:
            raise ValueError(f"{path}: task '{name}': '{state_name}' is added to the "
                             f"chain automatically, drop it from '{KEY_SEQUENCE}'")
        if sequence.count(state_name) > 1:
            raise ValueError(f"{path}: task '{name}': '{state_name}' appears "
                             f"{sequence.count(state_name)} times, only one allowed")

    return sequence


# ==================================================================== build
def build_contexts(states, tasks):
    """Create the StateContexts and wire transitions[task] for every task."""
    contexts = {name: StateContext(name, cfg[KEY_ID], cfg)
                for name, cfg in states.items()}

    for task, sequence in tasks.items():
        chain = [START_STATE] + sequence + [END_STATE, START_STATE]
        for src, dst in zip(chain, chain[1:]):
            contexts[src].link(task, dst)
        contexts[CANCEL_STATE].link(task, START_STATE)

    return contexts


def build_machine_class(contexts, task):
    """Build a StateMachine class for exactly ONE task.

    Only states belonging to the task (plus Start/End/Cancel) are included: a state
    outside the chain would be unreachable and the framework would reject it.
    """
    names = [name for name, context in contexts.items() if task in context.transitions]

    sm_states = {name: State(initial=(name == START_STATE)) for name in names}
    attrs = dict(sm_states)

    attrs[EVENT_NEXT] = _union(
        sm_states[name].to(sm_states[contexts[name].next_state(task)])
        for name in names
    )
    attrs[EVENT_CANCEL] = _union(
        sm_states[name].to(sm_states[CANCEL_STATE])
        for name in names if name != CANCEL_STATE
    )
    return StateMachineMetaclass("ConfiguredSM", (ConfiguredSMBase,), attrs)


def _union(transitions):
    """Fold several transitions into one event:  a.to(b) | c.to(d) | ..."""
    result = None
    for transition in transitions:
        result = transition if result is None else result | transition
    if result is None:
        raise ValueError("event has no transition")
    return result


class ConfiguredSMBase(StateMachine):
    """enter/exit dispatch through StateContext; progress runs inside tick()."""

    def __init__(self, contexts, task, listener=None):
        # assign before super(), on_enter_state fires right away for the initial state
        self.contexts = contexts
        self.task = task
        self.listener = listener  # called with every state change, e.g. the server
        super().__init__()

    @property
    def context(self):
        return self.contexts[self.current_state_value]

    def on_enter_state(self, state):
        if self.listener is not None:
            self.listener(state.id)
        self.contexts[state.id].enter()

    def on_exit_state(self, state):
        self.contexts[state.id].exit()

    def tick(self):
        """One loop: run progress of the current state, move on the event it returns."""
        context = self.context
        event = context.progress()

        match event:
            case Event.NONE:
                return  # not done, next tick runs progress of this state again
            case Event.CANCEL if context.name == CANCEL_STATE:
                print(f"[warn    ] already in {CANCEL_STATE}, ignoring cancel event")
            case Event.NEXT:
                self.send(EVENT_NEXT)
            case Event.CANCEL:
                self.send(EVENT_CANCEL)
            case _:
                print(f"[warn    ] {context.name}: event is '{event}', "
                      f"cannot handle it")


def create_machine(task, path=CONFIG_PATH, listener=None):
    """Read the config from disk and build a machine for one task.

    Call it again to reload the config at runtime or to switch to another task.
    """
    states, tasks = load_config(path)
    if task not in tasks:
        raise ValueError(f"{path}: no task '{task}', available: {list(tasks)}")

    contexts = build_contexts(states, tasks)
    print(f"[task    ] {task}: {' -> '.join(chain_of(contexts, task))}")
    return build_machine_class(contexts, task)(contexts, task, listener)


def chain_of(contexts, task):
    """Walk the transitions into the actual chain, handy for eyeballing a config."""
    chain = [START_STATE]
    while True:
        next_name = contexts[chain[-1]].next_state(task)
        if next_name == START_STATE:
            return chain + [START_STATE]
        chain.append(next_name)


# ====================================================================== run
# Three loops, none of them talks to another directly. They only meet through the
# one-shot flags in events.py and through SHUTDOWN:
#
#   machine thread   ticks the state machine every TICK_PERIOD
#   server thread    accepts GUI clients on PORT, turns lines into requests
#   main thread      reads the terminal (blocking, which is why it is not a thread)
TICK_PERIOD = 0.2    # seconds between two ticks
ABORT_DELAY = 10.0   # seconds without any client before the lap is cancelled
DEFAULT_TASK = "Normal2"
TERMINAL_HELP = "Enter = next, 'c' + Enter = cancel, 'q' + Enter = quit"


class Runner:
    """Holds the machine currently loaded, so it can be swapped while running.

    The tick thread reads it, a client thread replaces it on a TASK command, hence
    the lock. Swapping means building a brand new machine from the config on disk:
    a task is one chain, there is no such thing as switching task mid-chain.
    """

    def __init__(self, listener=None):
        self.listener = listener
        self.task = None
        self._machine = None
        self._lock = threading.Lock()
        self._abort_timer = None

    @property
    def machine(self):
        with self._lock:
            return self._machine

    @property
    def state(self):
        machine = self.machine
        return "-" if machine is None else machine.current_state_value

    def load(self, task):
        """Build the machine for `task` and make it the live one."""
        machine = create_machine(task, listener=self.listener)
        with self._lock:
            self._machine = machine
            self.task = task

        # no need to announce the state here: building the machine enters Start,
        # which already fired the listener
        NextRequest.take()  # drop requests aimed at the previous machine
        CancelRequest.take()

    # ------------------------------------------------- nobody is watching
    def on_clients_changed(self, count):
        """A disconnect does NOT touch the machine: it stays where it was, so a
        client that comes straight back finds the run untouched.

        Only if nobody is back within ABORT_DELAY is the lap aborted. That is a
        plain cancel request, so it takes the normal way out: Cancel runs the
        clean-up, then the machine ends up in Start. Rebuilding the machine would
        land on Start too, but it would skip the clean-up.
        """
        if count > 0:
            self._stop_abort_timer("client back")
            return
        self._start_abort_timer()

    def _start_abort_timer(self):
        self._stop_abort_timer()
        print(f"[machine ] no client left, cancelling in {ABORT_DELAY:.0f}s")
        self._abort_timer = threading.Timer(ABORT_DELAY, self._abort)
        self._abort_timer.daemon = True
        self._abort_timer.start()

    def _stop_abort_timer(self, reason=None):
        if self._abort_timer is None:
            return
        self._abort_timer.cancel()
        self._abort_timer = None
        if reason is not None:
            print(f"[machine ] cancel called off: {reason}")

    def _abort(self):
        self._abort_timer = None
        if SHUTDOWN.is_set() or self.machine is None:
            return
        print(f"[machine ] nobody came back, cancelling task '{self.task}'")
        CancelRequest.set(source="timeout")

    def run(self):
        """Machine thread: tick the live machine until someone asks to stop."""
        while not SHUTDOWN.is_set():
            machine = self.machine
            if machine is not None:
                machine.tick()
            time.sleep(TICK_PERIOD)
        print("[machine ] stopped")


def run_terminal():
    """Main thread: same three requests as the GUI, typed instead of clicked."""
    print(f"[input   ] {TERMINAL_HELP}")
    while not SHUTDOWN.is_set():
        match ask():
            case Request.ENTER:
                NextRequest.set()
            case Request.CANCEL:
                CancelRequest.set()
            case Request.QUIT:
                SHUTDOWN.set()
            case key:
                print(f"[input   ] unknown key '{key}'. {TERMINAL_HELP}")


def make_command_handler(runner):
    """Build the callback the server hands every incoming line to.

    Runs in a client thread, so it never touches a machine directly: it raises the
    request flags, reads the state name (a plain string), or asks the runner to
    load another task.
    """
    def handle_command(command):
        head, _, argument = command.strip().partition(" ")
        match head.upper():
            case "NEXT":
                NextRequest.set(source="tcp")
                return "OK NEXT"
            case "CANCEL":
                CancelRequest.set(source="tcp")
                return "OK CANCEL"
            case "STATUS":
                return f"STATE {runner.state}"
            case "TASK":
                return _load_task(runner, argument.strip())
            case _:
                return f"ERR unknown command '{command}'"

    return handle_command


def _load_task(runner, task):
    """TASK <name>: rebuild the machine from the config on disk.

    The GUI reads the same config to fill its task list, but it only sends the
    NAME: the config that counts is the one next to this file, on the machine that
    drives the robot.
    """
    if not task:
        return "ERR TASK needs a name"
    try:
        runner.load(task)
    except (OSError, ValueError, KeyError) as error:
        print(f"[warn    ] cannot load task '{task}': {error}")
        return f"ERR {error}"
    return f"OK TASK {task}"


def main(task=DEFAULT_TASK):
    custom_callbacks.register_all()

    # the two know each other, so the handler is wired in after the runner exists
    server = Server()
    runner = Runner(listener=server.broadcast_state)
    server.handle_command = make_command_handler(runner)
    server.on_clients_changed = runner.on_clients_changed
    runner.load(task)  # a task to start with; the GUI can push another one

    threading.Thread(target=server.serve_forever, name="server", daemon=True).start()
    threading.Thread(target=runner.run, name="machine", daemon=True).start()
    run_terminal()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass  # Ctrl-C outside of a prompt
    finally:
        SHUTDOWN.set()
        print("[input   ] quit")
