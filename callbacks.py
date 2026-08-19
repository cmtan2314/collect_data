"""CALLBACK_MAP: callback name (the string in the config) -> function to run.

Every callback shares one signature:

    def callback(state) -> event | None

    enter / exit : return value is ignored
    progress     : returns an Event. Returning None counts as Event.NEXT (nothing to do)

`state` is a StateContext, so a callback can read state.name / state.id and stash
per-state data in state.data (a tick counter for instance) instead of using globals.

This file holds the built-in callbacks: the three empty defaults, plus one set per
fixed state (Start / End / Cancel), since those three are part of the machine
itself. Application specific callbacks live in custom_callbacks.py.

CALLBACK_MAP is looked up at RUNTIME on every call, so register_callback() still
takes effect halfway through a run: no need to reload the config or rebuild the
machine.
"""

import bag
from events import CancelRequest, Event, NextRequest

DEFAULT_ENTER = "default_enter"
DEFAULT_PROGRESS = "default_progress"
DEFAULT_EXIT = "default_exit"

START_ENTER = "start_enter"
START_PROGRESS = "start_progress"
START_EXIT = "start_exit"

END_ENTER = "end_enter"
END_PROGRESS = "end_progress"
END_EXIT = "end_exit"

CANCEL_ENTER = "cancel_enter"
CANCEL_PROGRESS = "cancel_progress"
CANCEL_EXIT = "cancel_exit"

KEY_LAP = "lap"  # in Start.data: how many laps have been started


# --------------------------------------------------------------- default callbacks
def default_enter(state):
    """Empty. StateContext already prints the [enter] log line."""


def default_exit(state):
    """Empty."""


def default_progress(state):
    """Empty in the sense of "no work of its own": it just waits to be told.

    One NEXT request moves exactly one state, so a lap is walked step by step from
    the terminal or the GUI. A callback that has real work to do (custom_A) decides
    on its own instead of waiting here.

    Never blocks: this runs in the tick thread. NONE means "still waiting".
    """
    if CancelRequest.take():
        return Event.CANCEL
    if not NextRequest.take():
        return Event.NONE
    return Event.NEXT


# ----------------------------------------------------------------- Start / End / Cancel
# The three fixed states of the machine. Their chain is:
#   Start -> [task] -> End -> Start        Cancel -> Start
# None of them does robot work by default, they are the places to hook a lap
# counter, reporting, and the abort clean-up.
def start_enter(state):
    """A new lap begins here: bump the lap counter kept in Start's own data."""
    state.data[KEY_LAP] = state.data.get(KEY_LAP, 0) + 1
    print(f"[start   ] lap {state.data[KEY_LAP]}, waiting for next")


def start_progress(state):
    """The lap only begins once someone asks for it: Enter in the terminal, or
    NEXT from the GUI over TCP.

    Never blocks, this runs in the tick thread. NONE means "still waiting": the
    machine stays in Start and looks again next tick.
    """
    CancelRequest.take()  # a cancel from the previous lap has nothing to abort

    if not NextRequest.take():
        return Event.NONE

    # The lap does not begin while a topic has nobody publishing it. A lap run
    # like that looks perfectly healthy and produces a bag with holes in it, and
    # the holes are only found later, by whoever tries to use the data.
    missing = bag.RECORDER.missing_topics()
    if missing:
        print(f"[input   ] not leaving {state.name}, nothing publishes "
              f"{', '.join(missing)}")
        return Event.NONE
    return Event.NEXT


def start_exit(state):
    """Empty."""


def end_enter(state):
    """Empty. Report a finished lap from here: publish a result, close a bag..."""


def end_progress(state):
    """The lap is done, but going back to Start is a step like any other: it waits
    for its own NEXT. A cancel is pointless here, it is left in place and
    start_progress() drops it."""
    if not NextRequest.take():
        return Event.NONE
    return Event.NEXT


def end_exit(state):
    """Empty."""


def cancel_enter(state):
    """Clear the flag: the request is being served, it must not fire twice."""
    CancelRequest.take()
    print("[cancel  ] aborting the current lap")


def cancel_progress(state):
    """Clean-up lives here: stop the motors, go home, release the gripper.
    Returning NEXT sends the machine back to Start.

    Return Event.NONE instead to stay for another tick while a slow clean-up
    (homing for instance) is still running.
    """
    print("[cancel  ] stop motors, move home")  # TODO: real clean-up
    return Event.NEXT


def cancel_exit(state):
    """Empty."""


CALLBACK_MAP = {
    DEFAULT_ENTER: default_enter,
    DEFAULT_PROGRESS: default_progress,
    DEFAULT_EXIT: default_exit,

    START_ENTER: start_enter,
    START_PROGRESS: start_progress,
    START_EXIT: start_exit,

    END_ENTER: end_enter,
    END_PROGRESS: end_progress,
    END_EXIT: end_exit,

    CANCEL_ENTER: cancel_enter,
    CANCEL_PROGRESS: cancel_progress,
    CANCEL_EXIT: cancel_exit,
}


def register_callback(name, callback):
    """Register a callback, e.g. register_callback("custom_A", custom_A)."""
    if not name:
        raise ValueError("a callback needs a name")
    if not callable(callback):
        raise TypeError(f"'{name}': callback must be callable, got "
                        f"{type(callback).__name__}")
    if name in CALLBACK_MAP:
        print(f"[callback] overriding '{name}': {CALLBACK_MAP[name].__name__} -> "
              f"{callback.__name__}")
    CALLBACK_MAP[name] = callback


def has_callback(name):
    """Used to warn early while reading the config, never to reject it."""
    return name in CALLBACK_MAP


def resolve_callback(name, fallback):
    """Name -> function. Unknown names warn and fall back instead of raising:
    the config is allowed to name a callback that gets registered later."""
    if name in CALLBACK_MAP:
        return CALLBACK_MAP[name]

    print(f"[warn    ] callback '{name}' is not in CALLBACK_MAP, "
          f"falling back to '{fallback}'")
    return CALLBACK_MAP[fallback]
