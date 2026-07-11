import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Set

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect


APP_VERSION = "1.0.0"
MAX_FRAME_SIZE_BYTES = int(os.getenv("MAX_FRAME_SIZE_BYTES", "120000"))
CAMERA_TIMEOUT_SEC = int(os.getenv("CAMERA_TIMEOUT_SEC", "10"))
CAMERA_TOKEN = os.getenv("CAMERA_TOKEN", "")
BACKEND_TOKEN = os.getenv("BACKEND_TOKEN", "")


def now_ms() -> int:
    return int(time.time() * 1000)


def token_for(prefix: str, device_id: str, fallback: str) -> str:
    key = f"{prefix}_{device_id}".upper().replace("-", "_")
    return os.getenv(key, fallback)


def token_ok(actual: str | None, expected: str) -> bool:
    return not expected or actual == expected


@dataclass
class DeviceRelay:
    latest_frame: bytes | None = None
    latest_frame_ms: int | None = None
    frame_count: int = 0
    dropped_frame_count: int = 0
    oversized_frame_count: int = 0
    camera_connected: bool = False
    camera_connected_ms: int | None = None
    camera_last_seen_ms: int | None = None
    subscribers: Set[asyncio.Queue[bytes]] = field(default_factory=set)


class RelayManager:
    def __init__(self):
        self.devices: Dict[str, DeviceRelay] = {}
        self.lock = asyncio.Lock()

    async def get(self, device_id: str) -> DeviceRelay:
        async with self.lock:
            if device_id not in self.devices:
                self.devices[device_id] = DeviceRelay()
            return self.devices[device_id]

    async def set_camera_connected(self, device_id: str, connected: bool) -> None:
        device = await self.get(device_id)
        async with self.lock:
            device.camera_connected = connected
            if connected:
                device.camera_connected_ms = now_ms()
                device.camera_last_seen_ms = now_ms()

    async def publish_frame(self, device_id: str, frame: bytes) -> bool:
        if len(frame) > MAX_FRAME_SIZE_BYTES:
            device = await self.get(device_id)
            async with self.lock:
                device.oversized_frame_count += 1
            return False
        if frame[:2] != b"\xff\xd8":
            return False

        device = await self.get(device_id)
        async with self.lock:
            t = now_ms()
            device.latest_frame = frame
            device.latest_frame_ms = t
            device.camera_last_seen_ms = t
            device.camera_connected = True
            device.frame_count += 1
            subscribers = list(device.subscribers)

        for queue in subscribers:
            if queue.full():
                try:
                    queue.get_nowait()
                    device.dropped_frame_count += 1
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(frame)
            except asyncio.QueueFull:
                pass
        return True

    async def add_subscriber(self, device_id: str) -> asyncio.Queue[bytes]:
        device = await self.get(device_id)
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
        async with self.lock:
            device.subscribers.add(queue)
            if device.latest_frame:
                queue.put_nowait(device.latest_frame)
        return queue

    async def remove_subscriber(self, device_id: str, queue: asyncio.Queue[bytes]) -> None:
        device = await self.get(device_id)
        async with self.lock:
            device.subscribers.discard(queue)

    async def health(self) -> dict:
        t = now_ms()
        async with self.lock:
            devices = {}
            for device_id, d in self.devices.items():
                age_ms = None if d.camera_last_seen_ms is None else t - d.camera_last_seen_ms
                online = bool(d.camera_connected and age_ms is not None and age_ms <= CAMERA_TIMEOUT_SEC * 1000)
                devices[device_id] = {
                    "camera_connected": d.camera_connected,
                    "camera_online": online,
                    "subscribers": len(d.subscribers),
                    "frame_count": d.frame_count,
                    "dropped_frame_count": d.dropped_frame_count,
                    "oversized_frame_count": d.oversized_frame_count,
                    "latest_frame_ms": d.latest_frame_ms,
                    "latest_frame_age_ms": age_ms,
                    "latest_frame_size_bytes": len(d.latest_frame) if d.latest_frame else 0,
                }
            return {
                "status": "ok",
                "version": APP_VERSION,
                "connected_cameras": sum(1 for d in devices.values() if d["camera_online"]),
                "connected_subscribers": sum(d["subscribers"] for d in devices.values()),
                "devices": devices,
            }


app = FastAPI(title="VisionBot Camera Relay", version=APP_VERSION)
manager = RelayManager()


@app.get("/camera/health")
async def camera_health():
    return await manager.health()


@app.websocket("/camera/ws/push/{device_id}")
async def camera_push(websocket: WebSocket, device_id: str, token: str | None = Query(default=None)):
    expected = token_for("CAMERA_TOKEN", device_id, CAMERA_TOKEN)
    if not token_ok(token, expected):
        await websocket.close(code=1008)
        return

    await websocket.accept()
    await manager.set_camera_connected(device_id, True)
    print(f"[relay] camera connected: {device_id}")
    try:
        while True:
            frame = await websocket.receive_bytes()
            await manager.publish_frame(device_id, frame)
    except WebSocketDisconnect:
        print(f"[relay] camera disconnected: {device_id}")
    except Exception as exc:
        print(f"[relay] camera error {device_id}: {exc}")
    finally:
        await manager.set_camera_connected(device_id, False)


@app.websocket("/camera/ws/subscribe/{device_id}")
async def camera_subscribe(websocket: WebSocket, device_id: str, token: str | None = Query(default=None)):
    expected = token_for("BACKEND_TOKEN", device_id, BACKEND_TOKEN)
    if not token_ok(token, expected):
        await websocket.close(code=1008)
        return

    await websocket.accept()
    queue = await manager.add_subscriber(device_id)
    print(f"[relay] subscriber connected: {device_id}")
    try:
        while True:
            frame = await queue.get()
            await websocket.send_bytes(frame)
    except WebSocketDisconnect:
        print(f"[relay] subscriber disconnected: {device_id}")
    except Exception as exc:
        print(f"[relay] subscriber error {device_id}: {exc}")
    finally:
        await manager.remove_subscriber(device_id, queue)
