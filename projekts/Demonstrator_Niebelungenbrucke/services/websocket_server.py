from __future__ import annotations
from websockets.exceptions import ConnectionClosed
from websockets.asyncio.server import ServerConnection
import asyncio
import json
import logging
import threading
from collections.abc import Callable
from typing import Any

import websockets


class LiveWebSocketServer:
    """A WebSocket server that allows clients to subscribe to specific channels and receive real-time updates."""
    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        self.host = host
        self.port = int(port)
        self.clients: dict[ServerConnection, set[str]] = {}
        self.loop: asyncio.AbstractEventLoop | None = None
        self.command_callback: Callable[..., None] | None = None
        self.log = logging.getLogger("websocket")
        self.thread: threading.Thread | None = None
        self._tare_status: dict[str, Any] | None = None


    # Public Sync Methods
    def set_command_callback(self, callback: Callable[..., None]) -> None:
        """Sets a callback function to handle incoming commands from clients."""
        self.command_callback = callback


    def start(self) -> None:
        """Starts the WebSocket server in a separate thread."""
        def runner():
            """Runs the WebSocket server in a new asyncio event loop."""
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._run())

        self.thread = threading.Thread(target=runner, name="websocket", daemon=True)
        self.thread.start()


    def publish_sensor_channel(
        self,
        iso_timestamp: str,
        channel: str,
        unit: str,
        physical_quantity: str,
        value: float,
    ) -> None:
        """Publishes sensor data to a specific channel. The data is sent only to clients subscribed to that channel."""
        self._publish(
            {
                "header": [
                    {
                        "channel_name": "Timestamp",
                        "unit": "String - ISO 8601",
                        "physical_quantity": "Time",
                    },
                    {
                        "channel_name": channel,
                        "unit": unit,
                        "physical_quantity": physical_quantity,
                    },
                ],
                "data": {
                    "Timestamp": [iso_timestamp],
                    channel: [value],
                },
            },
            target_channel=channel
        )


    def publish_tare_done(self, zero_raw: float, enabled: bool | None = None) -> None:
        """Publishes a tare_done message to all connected clients."""
        payload: dict[str, Any] = {"type": "tare_done", "zero_raw": zero_raw}
        if enabled is not None:
            payload.update(
                {
                    "enabled": int(enabled),
                    "mode": "tared" if enabled else "raw",
                    "status": "TARED" if enabled else "RAW",
                    "status_code": int(enabled),
                    "raw_values": int(not enabled),
                }
            )
            self.publish_tare_status(enabled)
        self._publish(payload)

    def publish_tare_status(self, enabled: bool) -> None:
        """Publishes the current tared/raw measurement mode."""
        self._tare_status = {
            "type": "tare_status",
            "enabled": int(enabled),
            "mode": "tared" if enabled else "raw",
            "status": "TARED" if enabled else "RAW",
            "status_code": int(enabled),
            "raw_values": int(not enabled),
        }
        self._publish(self._tare_status)


    def close(self) -> None:
        """Stops the WebSocket server and closes all connections."""
        if not self.loop:
            return
        self.loop.call_soon_threadsafe(self.loop.stop)


    # Private Async Methods
    async def _handler(self, websocket: ServerConnection) -> None:
        """Handles incoming WebSocket connections and messages from clients."""
        self.clients[websocket] = set()
        self.log.info("WS client connected (%s)", len(self.clients))
        try:
            if self._tare_status is not None:
                await websocket.send(json.dumps(self._tare_status, separators=(",", ":")))
            async for message in websocket:
                try:
                    data = json.loads(message)
                    command = data.get("command")

                    # Intercept subscription commands and skip callback execution
                    if command in ("subscribe", "unsubscribe"):
                        channel = data.get("channel", None)
                        await self._handle_subscription(websocket, command, channel)
                        continue

                    # Forward all other commands to the global callback
                    if self.command_callback and command:
                        try:
                            self.command_callback(command, data)
                        except TypeError:
                            self.command_callback(command)

                except Exception:
                    self.log.exception("WS command error")

        except ConnectionClosed:
            pass

        except ConnectionResetError:
            pass

        except OSError:
            pass

        except Exception:
            self.log.exception("Unexpected WS client error")

        finally:
            self.clients.pop(websocket, None)
            self.log.info("WS client disconnected (%s)", len(self.clients))


    async def _handle_subscription(self, websocket: ServerConnection, command: str, channel: str | None = None) -> None:
        """Processes incoming subscription commands directly in the WebSocket thread."""
        if command == "subscribe":
            if channel is None:
                return

            self.clients[websocket].add(channel)
            self.log.info("Client subscribed to channel: %s", channel)

            ack = {"type": "subscription_ack", "channel": channel}
            await websocket.send(json.dumps(ack, ensure_ascii=False))

        elif command == "unsubscribe":
            if channel is None:
                self.clients[websocket].clear()
                self.log.info("Client unsubscribed from all channels")
            else:
                self.clients[websocket].discard(channel)
                self.log.info("Client unsubscribed from channel: %s", channel)


    async def _broadcast(self, data: dict[str, Any], target_channel: str | None = None) -> None:
        """Broadcasts data to all connected clients or a specific channel."""
        if not self.clients:
            return

        message = json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))
        dead_clients = []
        
        # Iterate over a copy of the keys since the dict can change dynamically
        for client in list(self.clients.keys()):
            # Skip clients that do not match the target channel if a filter is set
            if target_channel is not None and target_channel not in self.clients.get(client, set()):
                continue

            try:
                await asyncio.wait_for(
                    client.send(message),
                    timeout=2.0
                )
            except Exception:
                dead_clients.append(client)

        for client in dead_clients:
            self.clients.pop(client, None)


    async def _run(self) -> None:
        """Runs the WebSocket server and listens for incoming connections."""
        async with websockets.serve(self._handler, self.host, self.port):
            self.log.info("WebSocket server started on %s:%s", self.host, self.port)
            await asyncio.Future()


    def _publish(self, data: dict[str, Any], target_channel: str | None = None) -> None:
        """Publishes data to all connected clients or a specific channel in a thread-safe manner."""
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(self._broadcast(data, target_channel), self.loop)
