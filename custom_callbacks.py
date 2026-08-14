"""Application specific callbacks, kept out of callbacks.py (which only has the
empty defaults).

To add one:
    1. write the function   def custom_X(state): ...
    2. add one line to register_all()
    3. put the name "custom_X" in the config, under enter / progress / exit
"""

from callbacks import register_callback
from events import CancelRequest, Event

CUSTOM_A = "custom_A"
CUSTOM_A_REQUIRED_TICKS = 3

# key in state.data, prefixed with the callback name so two callbacks cannot clash
KEY_CUSTOM_A_TICKS = "custom_A.ticks"


def custom_A(state):
    """Example of a progress callback spanning several ticks: it only returns NEXT
    after CUSTOM_A_REQUIRED_TICKS ticks.

    The counter lives in state.data, so every state using this callback keeps its
    own count.
    """
    if CancelRequest.take():
        state.data[KEY_CUSTOM_A_TICKS] = 0
        return Event.CANCEL

    ticks = state.data.get(KEY_CUSTOM_A_TICKS, 0) + 1
    state.data[KEY_CUSTOM_A_TICKS] = ticks
    print(f"[custom  ] {state.name}: custom_A {ticks}/{CUSTOM_A_REQUIRED_TICKS}")

    if ticks < CUSTOM_A_REQUIRED_TICKS:
        return Event.NONE

    state.data[KEY_CUSTOM_A_TICKS] = 0  # reset for the next lap
    return Event.NEXT

def cus_cut_ros_bag(state): 
    print ("exit rot ne ")


def register_all():
    """Call once at startup. register_callback() still works at any point later."""
    register_callback(CUSTOM_A, custom_A)
    register_callback("cus_cut_ros_bag" ,cus_cut_ros_bag )
