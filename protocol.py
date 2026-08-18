"""Wire format between the state machine and its GUIs. Both sides import this file.

Every payload is one JSON object built by envelope(), so they all share the same
four fields no matter which topic they travel on:

    timestamp   ISO 8601 with timezone, when the message was built
    state       the state its sender believed the machine was in, null if unknown
    task        the task it believed was loaded, null if unknown
    info        free-form object. Nothing here is required, so new fields can be
                added during development without breaking a reader that predates
                them. Put anything experimental in here first.

On top of those, each topic adds its own. <BASE> is whatever the config puts in
its "Topic:" field, "state_machine" when it says nothing; see Topics below.

    topic                   dir   extra fields
    <BASE>/command          in    id, cmd [, task for cmd "task"]
    <BASE>/reply            out   id, ok, cmd [, error]
    <BASE>/state            out   -                    (info.id = id of the state)
    <BASE>/server           out   online
    <BASE>/clients/<id>     in    online

    {"timestamp": "2026-08-17T09:12:33.123+07:00", "state": "Pick",
     "task": "Normal2", "info": {"id": 2}}

"id" at the top level is chosen by the sender of a command and echoed in its
reply, so a GUI can tell which button an answer belongs to. The id a state carries
in the config is a different thing and lives in info.

state, server and the presence topics are published retained, so whoever connects
late is told how things stand without asking. reply is not: it is an event that
already happened, and a stale one would read as a fresh button press.

Presence works the same way in both directions: each side publishes online for
itself and registers offline as its last will, so the broker publishes that even
when the process is killed rather than closed. A killed process is noticed at
once, because closing its socket is what triggers the will; KEEPALIVE only comes
into play when the socket stays open but silent, a machine off the network.

Mind that a last will is built at connect time, so ITS timestamp is when the
sender connected, not when it died.
"""

import json
from datetime import datetime

BASE = "state_machine"  # used when the config names no other

BROKER_PORT = 1883
KEEPALIVE = 5  # seconds; how long the broker waits on a silent socket before the will
QOS = 1        # at least once: a lost "next" would leave the operator waiting
ENCODING = "utf-8"


class Command:
    """In a class so match/case can use them as value patterns (case Command.NEXT)."""

    NEXT = "next"
    CANCEL = "cancel"
    STATUS = "status"
    TASK = "task"


class Topics:
    """Every topic of one installation, hanging off a base read from the config.

    Two robots sharing a broker only need different bases to stop hearing each
    other. Both sides read the base from the same config file, so they cannot
    drift apart by accident.
    """

    def __init__(self, base=BASE):
        base = str(base or BASE).strip().strip("/")
        if not base:
            raise ValueError("topic base cannot be empty")
        for wildcard in ("+", "#"):
            if wildcard in base:
                raise ValueError(f"topic base '{base}' cannot contain '{wildcard}'")

        self.base = base
        self.command = f"{base}/command"
        self.reply = f"{base}/reply"
        self.state = f"{base}/state"
        self.server = f"{base}/server"
        self.presence_prefix = f"{base}/clients/"
        self.presence = f"{self.presence_prefix}+"  # what the server subscribes to

    def client(self, client_id):
        """The topic one GUI owns, where it publishes its own presence."""
        return self.presence_prefix + client_id

    def client_of(self, topic):
        """Which client a presence message came from, None if it is another topic."""
        if not topic.startswith(self.presence_prefix):
            return None
        return topic[len(self.presence_prefix):]

    def __str__(self):
        return f"{self.base}/#"


# ------------------------------------------------------------------- envelope
def timestamp():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def envelope(state=None, task=None, info=None, **fields):
    """The four common fields plus whatever the topic adds."""
    return {
        "timestamp": timestamp(),
        "state": state,
        "task": task,
        "info": dict(info or {}),
        **fields,
    }


def command(request_id, cmd, state=None, task=None, info=None, **fields):
    return envelope(state, task, info, id=request_id, cmd=cmd, **fields)


def ok(request_id, cmd, state=None, task=None, info=None, **fields):
    return envelope(state, task, info, id=request_id, ok=True, cmd=cmd, **fields)


def failed(request_id, cmd, error, state=None, task=None, info=None):
    return envelope(state, task, info, id=request_id, ok=False, cmd=cmd,
                    error=str(error))


def state_message(state, task, info=None):
    return envelope(state, task, info)


def presence(online, state=None, task=None, info=None):
    return envelope(state, task, info, online=bool(online))


def is_online(payload):
    return bool(payload.get("online"))


# ----------------------------------------------------------------- on the wire
def encode(payload):
    """dict -> bytes ready to publish."""
    return json.dumps(payload).encode(ENCODING)


def decode(raw):
    """bytes -> dict. Raises ValueError on anything that is not a JSON object."""
    try:
        payload = json.loads(raw.decode(ENCODING))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"not json: {error}") from error

    if not isinstance(payload, dict):
        raise ValueError(f"expected a json object, got {type(payload).__name__}")
    return payload


# --------------------------------------------------------------------- reading
def short_time(payload):
    """The timestamp as hh:mm:ss for a log line."""
    try:
        return datetime.fromisoformat(payload["timestamp"]).strftime("%H:%M:%S")
    except (KeyError, TypeError, ValueError):
        return "--:--:--"


def describe(payload):
    """One short line for a log or a status box."""
    head = short_time(payload)
    if "online" in payload:
        body = "online" if payload["online"] else "offline"
    elif "ok" in payload:
        body = f"ok: {payload.get('cmd')}" if payload["ok"] else \
               f"error: {payload.get('error')}"
    else:
        body = f"state {payload.get('state')} (task {payload.get('task')})"

    info = payload.get("info")
    return f"{head} {body}" + (f" {info}" if info else "")
