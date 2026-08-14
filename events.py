"""What the machine reacts to. Definitions only.

    Event    what progress() hands back to tick(). Internal to the machine.
    Request  what the operator asks for. The values ARE the terminal keys,
             a bare Enter is the empty string.

NextRequest / CancelRequest are one-shot flags. Anyone can set them (the terminal
thread, the TCP server thread, a ROS callback later); the machine thread takes
them. That is the only shared state between the threads, hence the lock.

SHUTDOWN is set once, by whoever wants the program to stop; every loop watches it.
"""

import threading

SHUTDOWN = threading.Event()


class Event:
    NEXT = "next"      # move to the next state of the current task
    CANCEL = "cancel"  # jump to the Cancel state
    NONE = "none"      # not finished, stay here, run progress again next tick


class Request:
    ENTER = ""   # go ahead
    CANCEL = "c"
    QUIT = "q"   # also what Ctrl-C / Ctrl-D turn into


class RequestFlag:
    """One-shot flag: set from any thread, taken exactly once."""

    def __init__(self, name):
        self.name = name
        self._lock = threading.Lock()
        self._raised = False

    def set(self, source="terminal"):
        with self._lock:
            self._raised = True
        print(f"[input   ] {self.name} ({source})")

    def take(self):
        """Read and clear in one go, so the request cannot fire twice."""
        with self._lock:
            raised, self._raised = self._raised, False
        return raised


NextRequest = RequestFlag("next")
CancelRequest = RequestFlag("cancel")


def ask(prompt=""):
    """Block until a line is typed. Returns the key, "" for a bare Enter.

    Ctrl-C and Ctrl-D read as QUIT instead of blowing up through the caller.
    """
    try:
        return input(prompt).strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()  # get off the prompt line
        return Request.QUIT
