"""MQTT link between the state machine and the GUI. Needs paho-mqtt 2.x.

The topics and the payload format live in protocol.py; this file only carries
them. It owns no state machine logic either: an incoming command becomes a call
to the handler given by main(), and whatever main() publishes goes back out.

The state machine always talks to the broker running on ITS OWN machine, so the
address here is fixed. A GUI sitting elsewhere reaches the same broker over the
network, through the IP typed into its window.

Callbacks below run in paho's own network thread, never in the tick thread. That
is why they only raise the request flags from events.py and never touch a machine.
"""

import paho.mqtt.client as mqtt

import protocol
from protocol import KEEPALIVE, QOS

BROKER_HOST = "0.0.0.0"  # the broker on this machine; the GUI dials in from outside
BROKER_PORT = protocol.BROKER_PORT
SERVER_ID = "state-machine"


class MqttServer:
    def __init__(self, handle_command=None, topics=None, host=BROKER_HOST,
                 port=BROKER_PORT):
        # (payload: dict) -> reply dict. Assigned after the runner exists, since
        # the handler needs it and the runner needs the server to publish states.
        self.handle_command = handle_command
        # (count: int) -> None, called whenever a GUI comes or goes
        self.on_clients_changed = None
        self.topics = topics or protocol.Topics()
        self.host = host
        self.port = port

        self._online = set()  # client ids that said online and have not said otherwise
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                   client_id=SERVER_ID)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        # left with the broker to publish if this process dies without saying bye
        self._client.will_set(self.topics.server,
                              protocol.encode(protocol.presence(False)),
                              qos=QOS, retain=True)

    # ---------------------------------------------------------------- lifetime
    def start(self):
        """Connect and hand the socket to paho's thread. False if the broker is out."""
        try:
            self._client.connect(self.host, self.port, KEEPALIVE)
        except OSError as error:
            print(f"[server  ] no broker at {self.host}:{self.port}: {error}")
            print("[server  ] running blind, only the terminal can drive the machine")
            return False

        self._client.loop_start()
        return True

    def stop(self):
        """Say offline on the way out, so a GUI does not sit there waiting."""
        self._publish(self.topics.server, protocol.presence(False), retain=True)
        self._client.disconnect()  # flushes what is queued before closing
        self._client.loop_stop()

    # ---------------------------------------------------------------- outgoing
    def publish_state(self, payload):
        """Handed to the runner: one retained message per state change."""
        self._publish(self.topics.state, payload, retain=True)

    def _publish(self, topic, payload, retain=False):
        self._client.publish(topic, protocol.encode(payload), qos=QOS, retain=retain)

    # ---------------------------------------------------------------- incoming
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            print(f"[server  ] broker refused the connection: {reason_code}")
            return

        client.subscribe([(self.topics.command, QOS), (self.topics.presence, QOS)])
        self._publish(self.topics.server, protocol.presence(True), retain=True)
        print(f"[server  ] connected to the broker at {self.host}:{self.port}, "
              f"topics {self.topics}")

    def _on_message(self, client, userdata, message):
        try:
            payload = protocol.decode(message.payload)
        except ValueError as error:
            print(f"[warn    ] bad payload on {message.topic}: {error}")
            return

        if message.topic == self.topics.command:
            self._on_command(payload)
            return

        client_id = self.topics.client_of(message.topic)
        if client_id is not None:
            self._on_presence(client_id, protocol.is_online(payload))

    def _on_command(self, payload):
        if self.handle_command is None:
            reply = protocol.failed(payload.get("id"), payload.get("cmd"),
                                    "server not wired up yet")
        else:
            reply = self.handle_command(payload)
        self._publish(self.topics.reply, reply)

    def _on_presence(self, client_id, online):
        """Retained presence is replayed on every reconnect, so only report changes."""
        before = len(self._online)
        if online:
            self._online.add(client_id)
        else:
            self._online.discard(client_id)

        count = len(self._online)
        if count == before:
            return

        print(f"[server  ] {client_id} {'online' if online else 'offline'}, "
              f"{count} client(s) online")
        if self.on_clients_changed is not None:
            self.on_clients_changed(count)
