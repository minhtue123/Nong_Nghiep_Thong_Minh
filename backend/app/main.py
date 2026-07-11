import json
import os
import re
import threading
import time
import unicodedata
from io import BytesIO
from pathlib import Path
import shutil
import base64
from collections import deque
from typing import Any, Generator
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator
import paho.mqtt.client as mqtt
import websocket


APP_VERSION = "2.0.0-agri-aiot"

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
BASE_TOPIC = os.getenv("MQTT_BASE_TOPIC", "agri")
COMMAND_ACK_TIMEOUT_S = float(os.getenv("COMMAND_ACK_TIMEOUT_S", "3.0"))
COMMAND_ACK_OK_STATUSES = {"accepted", "executed", "ok"}
ROBOT_OFFLINE_TIMEOUT_MS = int(os.getenv("ROBOT_OFFLINE_TIMEOUT_MS", "5000"))
CAMERA_STALE_MS = int(os.getenv("CAMERA_STALE_MS", "2000"))
CAMERA_OFFLINE_MS = int(os.getenv("CAMERA_OFFLINE_MS", "5000"))
CAMERA_MAX_FRAME_BYTES = int(os.getenv("CAMERA_MAX_FRAME_BYTES", "250000"))
CAMERA_DISPLAY_ENHANCE = os.getenv("CAMERA_DISPLAY_ENHANCE", "1") == "1"
CAMERA_DISPLAY_JPEG_QUALITY = int(os.getenv("CAMERA_DISPLAY_JPEG_QUALITY", "48"))
CAMERA_MJPEG_INTERVAL_MS = int(os.getenv("CAMERA_MJPEG_INTERVAL_MS", "100"))
CAMERA_AI_OVERLAY_INTERVAL_MS = int(os.getenv("CAMERA_AI_OVERLAY_INTERVAL_MS", "300"))
BACKOFF_SCHEDULE_S = [1, 2, 5, 10, 20, 30]

AI_ENABLE_YOLO = os.getenv("AI_ENABLE_YOLO", "1") == "1"
AI_YOLO_MODEL = os.getenv("AI_YOLO_MODEL", "yolo11n.onnx")
AI_CONF_THRESHOLD = float(os.getenv("AI_CONF_THRESHOLD", "0.25"))
AI_YOLO_IMGSZ = int(os.getenv("AI_YOLO_IMGSZ", "320"))
AI_DETECT_INTERVAL_S = float(os.getenv("AI_DETECT_INTERVAL_S", "0.20"))
AI_ENABLE_VLM = os.getenv("AI_ENABLE_VLM", "1") == "1"
AI_VLM_MODEL = os.getenv("AI_VLM_MODEL", "ggml-org/SmolVLM-500M-Instruct-GGUF")
AI_VLM_MAX_NEW_TOKENS = int(os.getenv("AI_VLM_MAX_NEW_TOKENS", "48"))
AI_VLM_PROVIDER = os.getenv("AI_VLM_PROVIDER", "llama_server").strip().lower()
AI_VLM_OPENAI_BASE_URL = os.getenv("AI_VLM_OPENAI_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
AI_VLM_TIMEOUT_S = float(os.getenv("AI_VLM_TIMEOUT_S", "25"))
AI_MODEL_DIR = Path(os.getenv("AI_MODEL_DIR", "models"))
AI_DEVICE = os.getenv("AI_DEVICE", "auto").strip().lower()  # auto/cpu/cuda
CAMERA_RELAY_WS = os.getenv("CAMERA_RELAY_WS", "").strip()
CAMERA_RELAY_TOKEN = os.getenv("CAMERA_RELAY_TOKEN", "").strip()
CAMERA_PUSH_TOKEN = os.getenv("CAMERA_PUSH_TOKEN", "").strip()

DETECTOR_PRESETS = [
    # Keep this list intentionally small and stable. Heavy/non-realtime models caused
    # stream lag and confusing benchmark numbers on CPU-only laptops.
    {"id": "yolo11n.onnx", "label": "ONNX — YOLO11n realtime", "family": "yolo", "speed": "realtime", "recommended_imgsz": 320, "note": "khuyến nghị: nhanh nhất, ổn nhất cho realtime CPU"},
    {"id": "yolo11n.onnx", "label": "ONNX — YOLO11n cân bằng", "family": "yolo", "speed": "balanced", "recommended_imgsz": 320, "note": "giữ 320 vì yolo11n.onnx export cố định input 320"},
    {"id": "yolov8n.onnx", "label": "ONNX — YOLOv8n fallback", "family": "yolo", "speed": "fallback", "recommended_imgsz": 416, "note": "fallback nhẹ nếu YOLO11n lỗi"},
    {"id": "torchvision:ssdlite320_mobilenet_v3_large", "label": "SSD MobileNetV3 320 — không YOLO", "family": "ssd_mobilenet", "speed": "fast", "recommended_imgsz": 320, "note": "detector deep-learning khác YOLO, bbox COCO, CPU"},
]
VLM_PRESETS = [
    {"id": "ggml-org/SmolVLM-500M-Instruct-GGUF", "label": "SmolVLM 500M GGUF — llama-server", "params": "0.5B", "bbox": "không trực tiếp"},
    {"id": "HuggingFaceTB/SmolVLM2-500M-Video-Instruct", "label": "SmolVLM2 500M — nhẹ nhất", "params": "0.5B", "bbox": "không trực tiếp"},
    {"id": "HuggingFaceTB/SmolVLM-500M-Instruct", "label": "SmolVLM 500M — ổn định", "params": "0.5B", "bbox": "không trực tiếp"},
]


app = FastAPI(title="AgriVision Backend API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev only. Tighten this for production.
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

robots: dict[str, dict[str, Any]] = {}
events: list[dict[str, Any]] = []
mqtt_connected = False
mqtt_state_lock = threading.Lock()
mqtt_state: dict[str, Any] = {
    "state": "starting",
    "connected": False,
    "host": MQTT_HOST,
    "port": MQTT_PORT,
    "attempts": 0,
    "reconnect_delay_s": 0,
    "last_connect_ms": None,
    "last_disconnect_ms": None,
    "last_error": None,
}

ack_lock = threading.Lock()
pending_cmd_acks: dict[tuple[str, int], dict[str, Any]] = {}
latest_cmd_acks: dict[str, list[dict[str, Any]]] = {}


def now_ms() -> int:
    return int(time.time() * 1000)


def backoff_delay(attempt: int) -> int:
    return BACKOFF_SCHEDULE_S[min(max(attempt, 0), len(BACKOFF_SCHEDULE_S) - 1)]


def set_mqtt_state(state: str, *, connected: bool | None = None, error: str | None = None, reconnect_delay_s: int | None = None) -> None:
    with mqtt_state_lock:
        mqtt_state["state"] = state
        if connected is not None:
            mqtt_state["connected"] = connected
        if reconnect_delay_s is not None:
            mqtt_state["reconnect_delay_s"] = reconnect_delay_s
        if error is not None:
            mqtt_state["last_error"] = error
        if connected is True:
            mqtt_state["last_connect_ms"] = now_ms()
            mqtt_state["last_error"] = None
            mqtt_state["reconnect_delay_s"] = 0
        elif connected is False:
            mqtt_state["last_disconnect_ms"] = now_ms()


def mqtt_state_snapshot() -> dict[str, Any]:
    with mqtt_state_lock:
        snap = dict(mqtt_state)
    if snap.get("last_connect_ms"):
        snap["last_connect_age_ms"] = now_ms() - int(snap["last_connect_ms"])
    if snap.get("last_disconnect_ms"):
        snap["last_disconnect_age_ms"] = now_ms() - int(snap["last_disconnect_ms"])
    return snap


COMMAND_SEQ_MAX = 2_147_483_647


def command_seq() -> int:
    """Return a positive sequence safe for ESP32/ArduinoJson uint32 handling."""
    seq = now_ms() % COMMAND_SEQ_MAX
    return seq if seq > 0 else 1


def normalize_command_seq(value: Any) -> int:
    try:
        seq = int(value or 0)
    except Exception:
        seq = 0
    if seq <= 0:
        return command_seq()
    if seq >= COMMAND_SEQ_MAX:
        seq = seq % COMMAND_SEQ_MAX
    return seq if seq > 0 else 1


def preferred_ai_device() -> str:
    """Return cuda only when explicitly available; otherwise safe CPU fallback."""
    if AI_DEVICE in {"cpu", "none"}:
        return "cpu"
    if AI_DEVICE in {"cuda", "gpu", "auto"}:
        try:
            import torch
            if torch.cuda.is_available():
                return "cuda"
        except Exception:
            pass
    return "cpu"


def ai_runtime_info() -> dict[str, Any]:
    info = {"requested_device": AI_DEVICE, "selected_device": preferred_ai_device(), "cuda_available": False, "gpu_name": None}
    try:
        import torch
        info["cuda_available"] = bool(torch.cuda.is_available())
        if torch.cuda.is_available():
            info["gpu_name"] = torch.cuda.get_device_name(0)
        info["torch_version"] = getattr(torch, "__version__", None)
    except Exception as exc:
        info["torch_error"] = str(exc)
    return info


class CameraSession:
    def __init__(self, device_id: str):
        self.device_id = device_id
        self.running = False
        self.thread: threading.Thread | None = None
        self.latest_jpeg: bytes | None = None
        self.latest_frame_ms: int | None = None
        self.frame_count = 0
        self.dropped_frames = 0
        self.frame_times_ms: deque[int] = deque(maxlen=180)
        self.last_error: str | None = None
        self.connected = False
        self.state = "idle"
        self.source = "none"
        self.connection_attempts = 0
        self.reconnect_delay_s = 0
        self.last_connect_ms: int | None = None
        self.last_disconnect_ms: int | None = None
        self.lock = threading.Lock()

    def _fps_locked(self) -> float:
        if len(self.frame_times_ms) < 2:
            return 0.0
        span_ms = self.frame_times_ms[-1] - self.frame_times_ms[0]
        if span_ms <= 0:
            return 0.0
        return round((len(self.frame_times_ms) - 1) * 1000.0 / span_ms, 2)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            size = len(self.latest_jpeg) if self.latest_jpeg else 0
            age = None if self.latest_frame_ms is None else now_ms() - self.latest_frame_ms
            if age is None:
                camera_status = "no_frame"
            elif age <= CAMERA_STALE_MS:
                camera_status = "online"
            elif age <= CAMERA_OFFLINE_MS:
                camera_status = "stale"
            else:
                camera_status = "offline"
            return {
                "running": self.running,
                "connected": self.connected,
                "state": self.state,
                "source": self.source,
                "camera_status": camera_status,
                "connection_attempts": self.connection_attempts,
                "reconnect_delay_s": self.reconnect_delay_s,
                "last_connect_ms": self.last_connect_ms,
                "last_disconnect_ms": self.last_disconnect_ms,
                "frame_count": self.frame_count,
                "latest_frame_seq": self.frame_count,
                "dropped_frames": self.dropped_frames,
                "stream_fps": self._fps_locked(),
                "latest_frame_ms": self.latest_frame_ms,
                "latest_frame_age_ms": age,
                "latest_frame_size_bytes": size,
                "latest_frame_kb": round(size / 1024.0, 1) if size else 0,
                "last_error": self.last_error,
            }

    def latest_frame(self) -> bytes | None:
        with self.lock:
            return self.latest_jpeg

    def mark_connecting(self, source: str, reconnect_delay_s: int = 0) -> None:
        with self.lock:
            self.state = "connecting"
            self.source = source
            self.connected = False
            self.connection_attempts += 1
            self.reconnect_delay_s = reconnect_delay_s

    def mark_connected(self, source: str) -> None:
        with self.lock:
            self.state = "connected"
            self.source = source
            self.connected = True
            self.reconnect_delay_s = 0
            self.last_connect_ms = now_ms()
            self.last_error = None

    def mark_disconnected(self, source: str, error: str | None = None, reconnect_delay_s: int = 0) -> None:
        with self.lock:
            self.state = "backoff" if reconnect_delay_s else "disconnected"
            self.source = source
            self.connected = False
            self.reconnect_delay_s = reconnect_delay_s
            self.last_disconnect_ms = now_ms()
            if error:
                self.last_error = error

    def set_frame(self, data: bytes, source: str = "unknown") -> bool:
        if len(data) > CAMERA_MAX_FRAME_BYTES:
            with self.lock:
                self.dropped_frames += 1
                self.last_error = f"frame_too_large:{len(data)}>{CAMERA_MAX_FRAME_BYTES}"
            return False
        t = now_ms()
        with self.lock:
            self.latest_jpeg = data
            self.latest_frame_ms = t
            self.frame_count += 1
            self.frame_times_ms.append(t)
            self.connected = True
            self.state = "streaming"
            self.source = source
            self.last_error = None
        return True

    def set_error(self, error: str, state: str = "error", reconnect_delay_s: int = 0) -> None:
        with self.lock:
            self.last_error = error
            self.connected = False
            self.state = state
            self.reconnect_delay_s = reconnect_delay_s

    def stop(self) -> None:
        self.running = False
        with self.lock:
            self.connected = False
            self.state = "stopped"
            self.reconnect_delay_s = 0


camera_sessions: dict[str, CameraSession] = {}
runtime_lock = threading.Lock()
vlm_streams: dict[str, dict[str, Any]] = {}
voice_latches: dict[str, dict[str, Any]] = {}


class DriveCommand(BaseModel):
    seq: int = Field(default_factory=command_seq)
    cmd: str | None = None
    left: float | None = None
    right: float | None = None
    ttl_ms: int = 500
    mode: str = "manual"

    @field_validator("seq", mode="before")
    @classmethod
    def _normalize_seq(cls, value: Any) -> int:
        return normalize_command_seq(value)

    @field_validator("ttl_ms")
    @classmethod
    def _ttl_bounds(cls, value: int) -> int:
        # SDS P0 deadman switch: robot must stop if command stream is stale.
        return max(300, min(int(value), 500))


class ServoCommand(BaseModel):
    seq: int = Field(default_factory=command_seq)
    angle: int

    @field_validator("seq", mode="before")
    @classmethod
    def _normalize_seq(cls, value: Any) -> int:
        return normalize_command_seq(value)


class StopCommand(BaseModel):
    seq: int = Field(default_factory=command_seq)
    reason: str = "backend_stop"

    @field_validator("seq", mode="before")
    @classmethod
    def _normalize_seq(cls, value: Any) -> int:
        return normalize_command_seq(value)


class AgriPumpCommand(BaseModel):
    seq: int = Field(default_factory=command_seq)
    cmd: str = Field("off", pattern="^(on|off|auto)$")
    duration_ms: int = 0
    reason: str = "dashboard"

    @field_validator("seq", mode="before")
    @classmethod
    def _normalize_seq(cls, value: Any) -> int:
        return normalize_command_seq(value)

    @field_validator("duration_ms")
    @classmethod
    def _duration_bounds(cls, value: int) -> int:
        return max(0, min(int(value), 120000))


class AIConfigUpdate(BaseModel):
    enable_yolo: bool | None = None
    yolo_model: str | None = None
    yolo_imgsz: int | None = None
    conf_threshold: float | None = None
    detect_interval_s: float | None = None
    enable_vlm: bool | None = None
    vlm_model: str | None = None


class AIAskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=600)


class VLMStreamStartRequest(BaseModel):
    instruction: str = Field("Phía trước là gì? Hãy mô tả ngắn gọn và đưa ra lời khuyên an toàn cho robot.", min_length=1, max_length=600)
    interval_ms: int = 1500

    @field_validator("interval_ms")
    @classmethod
    def _interval_bounds(cls, value: int) -> int:
        return max(500, min(int(value), 10000))


class VoiceCommandRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=300)


def agri_message_kind(data: dict[str, Any]) -> str:
    schema = str(data.get("schema") or "").lower()
    kind = str(data.get("kind") or data.get("type") or "").lower()
    if kind in {"state", "telemetry", "heartbeat", "event"}:
        return kind
    if "telemetry" in schema:
        return "telemetry"
    if "heartbeat" in schema:
        return "heartbeat"
    if "event" in schema:
        return "event"
    if "state" in schema:
        return "state"
    if any(k in data for k in ["soil_moisture_percent", "air_temperature_c", "air_humidity_percent", "water_level_percent"]):
        return "telemetry"
    return "telemetry"


def topic_for(device_id: str, suffix: str) -> str:
    return f"{BASE_TOPIC}/{device_id}/{suffix}"


def mqtt_is_ready() -> bool:
    return mqtt_connected and mqtt_client.is_connected()


def publish_mqtt_or_503(topic: str, payload: dict[str, Any], qos: int = 1, timeout_s: float = 2.0) -> dict[str, Any]:
    if not mqtt_is_ready():
        raise HTTPException(
            status_code=503,
            detail={
                "error": "mqtt_not_connected",
                "message": "Backend is not connected to the MQTT broker, so the command was not sent.",
                "mqtt_connected_flag": mqtt_connected,
                "mqtt_client_connected": mqtt_client.is_connected(),
            },
        )

    try:
        info = mqtt_client.publish(topic, json.dumps(payload), qos=qos, retain=False)
    except Exception as exc:
        raise HTTPException(status_code=503, detail={"error": "mqtt_publish_exception", "message": str(exc), "topic": topic, "payload": payload}) from exc

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=503, detail={"error": "mqtt_publish_rejected", "message": mqtt.error_string(info.rc), "rc": info.rc, "topic": topic, "payload": payload})

    if qos > 0:
        try:
            info.wait_for_publish(timeout=timeout_s)
        except Exception as exc:
            raise HTTPException(status_code=504, detail={"error": "mqtt_publish_wait_exception", "message": str(exc), "mid": info.mid, "topic": topic, "payload": payload}) from exc

        if not info.is_published():
            raise HTTPException(status_code=504, detail={"error": "mqtt_publish_timeout", "message": f"MQTT publish was queued but not acknowledged within {timeout_s:.1f}s.", "mid": info.mid, "topic": topic, "payload": payload})

    return {"ok": True, "topic": topic, "payload": payload, "mqtt": {"qos": qos, "mid": info.mid, "published": info.is_published(), "rc": info.rc}}


def register_pending_cmd_ack(device_id: str, seq: int) -> tuple[tuple[str, int], threading.Event]:
    key = (device_id, int(seq))
    event = threading.Event()
    with ack_lock:
        if key in pending_cmd_acks:
            raise HTTPException(status_code=409, detail={"error": "duplicate_pending_command_seq", "device_id": device_id, "seq": seq})
        pending_cmd_acks[key] = {"event": event, "ack": None, "created_ms": now_ms()}
    return key, event


def clear_pending_cmd_ack(key: tuple[str, int]) -> None:
    with ack_lock:
        pending_cmd_acks.pop(key, None)


def pending_command_ack_count() -> int:
    with ack_lock:
        return len(pending_cmd_acks)


def pending_command_ack_snapshot(device_id: str) -> list[dict[str, Any]]:
    with ack_lock:
        return [{"device_id": did, "seq": seq, "created_ms": item.get("created_ms")} for (did, seq), item in pending_cmd_acks.items() if did == device_id]


def remember_cmd_ack(device_id: str, ack: dict[str, Any]) -> None:
    try:
        seq = int(ack.get("seq") or 0)
    except Exception:
        seq = 0

    ack["_received_ms"] = now_ms()
    latest = latest_cmd_acks.setdefault(device_id, [])
    latest.append(ack)
    del latest[:-100:]

    robot = robots.setdefault(device_id, {"device_id": device_id, "online": True, "state": {}, "telemetry": {}, "events": [], "last_seen_ms": now_ms()})
    robot["online"] = True
    robot["last_seen_ms"] = now_ms()
    robot["last_cmd_ack"] = ack
    robot["cmd_acks"] = latest[-20:]

    if seq <= 0:
        return
    key = (device_id, seq)
    with ack_lock:
        pending = pending_cmd_acks.get(key)
        if pending is not None:
            pending["ack"] = ack
            pending["event"].set()


def publish_command_and_wait_robot_ack(device_id: str, command_name: str, topic: str, payload: dict[str, Any], qos: int = 1, publish_timeout_s: float = 2.0, ack_timeout_s: float = COMMAND_ACK_TIMEOUT_S) -> dict[str, Any]:
    try:
        seq = int(payload.get("seq") or 0)
    except Exception:
        seq = 0
    if seq <= 0:
        raise HTTPException(status_code=400, detail={"error": "missing_command_seq", "payload": payload})
    assert_robot_command_allowed(device_id, command_name)

    pending_key, ack_event = register_pending_cmd_ack(device_id, seq)
    try:
        publish_result = publish_mqtt_or_503(topic, payload, qos=qos, timeout_s=publish_timeout_s)
        ack_received = ack_event.wait(timeout=ack_timeout_s)
        with ack_lock:
            pending = pending_cmd_acks.get(pending_key, {})
            robot_ack = pending.get("ack")

        if not ack_received or not robot_ack:
            raise HTTPException(status_code=504, detail={"error": "robot_ack_timeout", "message": f"Command reached the MQTT broker, but robot did not publish cmd_ack within {ack_timeout_s:.1f}s.", "device_id": device_id, "seq": seq, "command": command_name, "topic": topic, "payload": payload, "mqtt": publish_result.get("mqtt")})

        ack_status = str(robot_ack.get("status", "")).lower()
        if ack_status not in COMMAND_ACK_OK_STATUSES:
            raise HTTPException(status_code=409, detail={"error": "robot_rejected_command", "device_id": device_id, "seq": seq, "command": command_name, "payload": payload, "mqtt": publish_result.get("mqtt"), "robot_ack": robot_ack})

        return {"ok": True, "device_id": device_id, "seq": seq, "command": command_name, "topic": topic, "payload": payload, "mqtt": publish_result.get("mqtt"), "robot_ack": robot_ack}
    finally:
        clear_pending_cmd_ack(pending_key)


def safe_json(payload: bytes) -> dict[str, Any]:
    try:
        return json.loads(payload.decode("utf-8"))
    except Exception:
        return {"raw": payload.decode("utf-8", errors="replace")}


def upsert_robot(device_id: str, data: dict[str, Any], kind: str) -> None:
    r = robots.setdefault(device_id, {"device_id": device_id, "online": False, "state": {}, "telemetry": {}, "events": [], "last_seen_ms": None})
    r["last_seen_ms"] = now_ms()
    r["last_message_kind"] = kind
    if kind == "state":
        r["state"] = data
        r["online"] = bool(data.get("online", True))
        for key in ["ip", "http_base", "stream_url", "robot_id", "firmware", "camera_ready", "mqtt_connected"]:
            if key in data:
                r[key] = data[key]
    elif kind == "telemetry":
        r["telemetry"] = data
        r["online"] = True
        network = data.get("network") if isinstance(data.get("network"), dict) else {}
        camera = data.get("camera") if isinstance(data.get("camera"), dict) else {}
        if "ip" in network:
            r["ip"] = network["ip"]
        if "stream_url" in camera:
            r["stream_url"] = camera["stream_url"]
        if "ready" in camera:
            r["camera_ready"] = camera["ready"]
        if "mqtt_connected" in network:
            r["mqtt_connected"] = network["mqtt_connected"]
    elif kind == "heartbeat":
        r["heartbeat"] = data
        r["online"] = True
        if "mqtt_connected" in data:
            r["mqtt_connected"] = bool(data.get("mqtt_connected"))
    elif kind == "event":
        r["events"].append(data)
        r["events"] = r["events"][-100:]
        events.append(data)
        del events[:-500]


def upsert_camera_presence(device_id: str, session: CameraSession) -> None:
    snap = session.snapshot()
    r = robots.setdefault(device_id, {"device_id": device_id, "online": False, "state": {}, "telemetry": {}, "events": [], "last_seen_ms": None})
    r["last_seen_ms"] = now_ms()
    r["last_message_kind"] = "camera_frame"
    r["online"] = True
    r["camera_ready"] = True
    r["camera_session"] = snap
    state = r.setdefault("state", {})
    state["camera_ready"] = True
    state["camera_status"] = snap.get("camera_status")
    state["camera_source"] = snap.get("source")
    state["camera_frame_count"] = snap.get("frame_count")
    state["latest_frame_age_ms"] = snap.get("latest_frame_age_ms")


def refresh_robot_liveness() -> None:
    now = now_ms()
    for robot in robots.values():
        last_seen = robot.get("last_seen_ms")
        if last_seen is None:
            robot["online"] = False
            robot["liveness"] = "no_heartbeat"
            robot["last_seen_age_ms"] = None
            continue
        age = now - int(last_seen)
        robot["last_seen_age_ms"] = age
        if age > ROBOT_OFFLINE_TIMEOUT_MS:
            robot["online"] = False
            robot["liveness"] = "offline"
        else:
            robot["liveness"] = "online" if robot.get("online", True) else "reported_offline"
        session = camera_sessions.get(str(robot.get("device_id") or ""))
        if session:
            robot["camera_session"] = session.snapshot()


def assert_robot_command_allowed(device_id: str, command_name: str) -> None:
    refresh_robot_liveness()
    robot = robots.get(device_id)
    if robot is None:
        raise HTTPException(status_code=404, detail={"error": "robot_not_found", "device_id": device_id})
    if command_name == "stop":
        return
    if not robot.get("online"):
        raise HTTPException(status_code=409, detail={"error": "robot_offline", "device_id": device_id, "last_seen_age_ms": robot.get("last_seen_age_ms"), "message": "Robot is offline/stale, command was not sent."})
    if robot.get("mqtt_connected") is False:
        raise HTTPException(status_code=409, detail={"error": "robot_mqtt_offline", "device_id": device_id, "message": "Robot reports MQTT offline, command was not sent."})


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None):
    global mqtt_connected
    mqtt_connected = True
    set_mqtt_state("connected", connected=True)
    client.subscribe(f"{BASE_TOPIC}/+/state", qos=1)
    client.subscribe(f"{BASE_TOPIC}/+/heartbeat", qos=0)
    client.subscribe(f"{BASE_TOPIC}/+/telemetry", qos=0)
    client.subscribe(f"{BASE_TOPIC}/+/event", qos=0)
    client.subscribe(f"{BASE_TOPIC}/+/cmd_ack", qos=1)
    client.publish(f"{BASE_TOPIC}/backend/status", json.dumps({"online": True, "state": "ready", "ts_ms": now_ms()}), qos=1, retain=True)
    print(f"[MQTT] connected to {MQTT_HOST}:{MQTT_PORT}")
    print(f"[MQTT] subscribed {BASE_TOPIC}/+/state heartbeat telemetry event cmd_ack")


def on_disconnect(client: mqtt.Client, userdata, reason_code, properties=None):
    global mqtt_connected
    mqtt_connected = False
    set_mqtt_state("disconnected", connected=False, error=str(reason_code))
    print(f"[MQTT] disconnected: {reason_code}")


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    parts = msg.topic.split("/")
    if len(parts) < 3:
        return
    device_id = parts[1]
    kind = parts[2]
    data = safe_json(msg.payload)
    if isinstance(data, dict):
        data["_topic"] = msg.topic
        data["_received_ms"] = now_ms()
    if kind in {"state", "heartbeat", "telemetry", "event"}:
        upsert_robot(device_id, data, kind)
        print(f"[MQTT] {kind} from {device_id}")
    elif kind == "cmd_ack":
        remember_cmd_ack(device_id, data)
        print(f"[MQTT] cmd_ack from {device_id}: seq={data.get('seq')} status={data.get('status')}")


mqtt_client = mqtt.Client(client_id="visionbot-backend", protocol=mqtt.MQTTv5)
if MQTT_USERNAME:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message


def mqtt_loop():
    attempt = 0
    while True:
        try:
            delay = backoff_delay(attempt)
            with mqtt_state_lock:
                mqtt_state["attempts"] = attempt + 1
            set_mqtt_state("connecting", connected=False, reconnect_delay_s=0)
            print(f"[MQTT] connecting to {MQTT_HOST}:{MQTT_PORT} attempt={attempt + 1}")
            mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            mqtt_client.loop_forever()
            attempt = 0
        except Exception as e:
            delay = backoff_delay(attempt)
            set_mqtt_state("reconnecting", connected=False, error=str(e), reconnect_delay_s=delay)
            print(f"[MQTT] connect/loop error: {e}; retry in {delay}s")
            time.sleep(delay)
            attempt += 1


def camera_relay_subscribe_url(device_id: str) -> str | None:
    if not CAMERA_RELAY_WS:
        return None
    encoded_device = quote(device_id, safe="")
    base = CAMERA_RELAY_WS.strip()
    if "{device_id}" in base:
        url = base.format(device_id=encoded_device)
    elif base.endswith("/camera/ws/subscribe"):
        url = f"{base}/{encoded_device}"
    elif base.endswith("/"):
        url = f"{base}camera/ws/subscribe/{encoded_device}"
    else:
        url = base
    if CAMERA_RELAY_TOKEN and "token=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{urlencode({'token': CAMERA_RELAY_TOKEN})}"
    return url


def camera_loop(session: CameraSession) -> None:
    print(f"[CAM] receiver thread started: {session.device_id}")
    ws = None
    attempt = 0
    while session.running:
        relay_url = camera_relay_subscribe_url(session.device_id)
        robot = robots.get(session.device_id)
        stream_url = relay_url or (robot.get("stream_url") if robot else None)
        source = "relay_subscribe" if relay_url else "lan_ws_pull"
        if not stream_url:
            delay = backoff_delay(attempt)
            session.set_error("missing_stream_url", state="backoff", reconnect_delay_s=delay)
            print(f"[CAM] missing stream url for {session.device_id}; retry in {delay}s")
            time.sleep(delay)
            attempt += 1
            continue
        try:
            session.mark_connecting(source)
            print(f"[CAM] connecting {session.device_id}: {stream_url}")
            ws = websocket.create_connection(stream_url, timeout=10)
            session.mark_connected(source)
            attempt = 0
            while session.running:
                data = ws.recv()
                if isinstance(data, bytes) and data[:2] == b"\xff\xd8":
                    session.set_frame(data, source=source)
        except Exception as e:
            delay = backoff_delay(attempt)
            session.mark_disconnected(source, error=str(e), reconnect_delay_s=delay)
            print(f"[CAM] {source} error for {session.device_id}: {e}; retry in {delay}s")
            time.sleep(delay)
            attempt += 1
        finally:
            try:
                if ws:
                    ws.close()
            except Exception:
                pass
    print(f"[CAM] receiver thread stopped: {session.device_id}")


def get_camera_session(device_id: str) -> CameraSession:
    if device_id not in camera_sessions:
        camera_sessions[device_id] = CameraSession(device_id)
    return camera_sessions[device_id]


def start_camera_session(device_id: str) -> CameraSession:
    session = get_camera_session(device_id)
    if session.running:
        return session
    session.running = True
    snap = session.snapshot()
    if str(snap.get("source") or "").startswith("backend_push_ws") and snap.get("latest_frame_size_bytes"):
        return session
    session.thread = threading.Thread(target=camera_loop, args=(session,), daemon=True)
    session.thread.start()
    return session


def camera_push_token_ok(token: str | None) -> bool:
    return not CAMERA_PUSH_TOKEN or token == CAMERA_PUSH_TOKEN


async def accept_camera_push(websocket: WebSocket, device_id: str, source: str, token: str | None = None) -> None:
    if not camera_push_token_ok(token):
        await websocket.close(code=1008)
        return

    await websocket.accept()
    session = get_camera_session(device_id)
    session.running = True
    session.mark_connected(source)
    upsert_camera_presence(device_id, session)
    last_presence_update_ms = now_ms()
    print(f"[CAM PUSH] connected: {device_id} source={source}")
    try:
        while True:
            data = await websocket.receive_bytes()
            if data[:2] == b"\xff\xd8":
                ok = session.set_frame(data, source=source)
                if ok:
                    t = now_ms()
                    if t - last_presence_update_ms >= 1000:
                        upsert_camera_presence(device_id, session)
                        last_presence_update_ms = t
                else:
                    print(f"[CAM PUSH] dropped frame from {device_id}: {len(data)} bytes")
    except WebSocketDisconnect:
        print(f"[CAM PUSH] disconnected: {device_id} source={source}")
    except Exception as exc:
        session.set_error(str(exc), state="error")
        print(f"[CAM PUSH] error {device_id}: {exc}")
    finally:
        session.mark_disconnected(source, error="push_ws_disconnected")


def mjpeg_generator(device_id: str) -> Generator[bytes, None, None]:
    session = get_camera_session(device_id)
    session.running = True
    last_seen_count = -1
    last_emit_ms = 0
    while session.running:
        with session.lock:
            frame = session.latest_jpeg
            count = session.frame_count
        t = now_ms()
        if frame is not None and count != last_seen_count and t - last_emit_ms >= CAMERA_MJPEG_INTERVAL_MS:
            last_seen_count = count
            last_emit_ms = t
            frame = enhance_display_jpeg(frame)
            yield b"--frame\r\nContent-Type: image/jpeg\r\nCache-Control: no-cache\r\n\r\n" + frame + b"\r\n"
        else:
            time.sleep(0.03)


# ----------------------------- AI layer -----------------------------

def _decode_jpeg_to_cv2(jpeg: bytes):
    import cv2
    import numpy as np
    arr = np.frombuffer(jpeg, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


def _encode_cv2_to_jpeg(img, quality: int = 80) -> bytes:
    import cv2
    ok, buf = cv2.imencode(".jpg", img, [int(cv2.IMWRITE_JPEG_QUALITY), int(quality)])
    if not ok:
        raise RuntimeError("cv2.imencode failed")
    return buf.tobytes()


def enhance_display_jpeg(jpeg: bytes) -> bytes:
    if not CAMERA_DISPLAY_ENHANCE:
        return jpeg
    try:
        import cv2
        img = _decode_jpeg_to_cv2(jpeg)
        if img is None:
            return jpeg
        h, w = img.shape[:2]
        if w > 320:
            scale = 320.0 / float(w)
            img = cv2.resize(img, (320, max(1, int(h * scale))), interpolation=cv2.INTER_AREA)
        dimmed = cv2.convertScaleAbs(img, alpha=0.68, beta=-42)
        return _encode_cv2_to_jpeg(dimmed, quality=CAMERA_DISPLAY_JPEG_QUALITY)
    except Exception:
        return jpeg


def analyze_leaf_health(jpeg: bytes) -> dict[str, Any]:
    """Lightweight leaf-color analysis used as context for VLM leaf diagnosis."""
    try:
        import cv2
        import numpy as np

        img = _decode_jpeg_to_cv2(jpeg)
        if img is None:
            raise RuntimeError("cannot decode jpeg")

        h, w = img.shape[:2]
        max_side = max(h, w)
        if max_side > 640:
            scale = 640.0 / float(max_side)
            img = cv2.resize(img, (max(1, int(w * scale)), max(1, int(h * scale))), interpolation=cv2.INTER_AREA)
            h, w = img.shape[:2]

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        blur_score = float(cv2.Laplacian(gray, cv2.CV_64F).var())
        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
        hue, sat, val = hsv[:, :, 0], hsv[:, :, 1], hsv[:, :, 2]

        green = (hue >= 25) & (hue <= 95) & (sat >= 35) & (val >= 35)
        yellow = (hue >= 15) & (hue < 35) & (sat >= 45) & (val >= 45)
        brown = (hue >= 3) & (hue < 25) & (sat >= 35) & (val >= 25) & (val <= 190)
        dark_spots = (val < 70) & (sat >= 30)
        leaf_mask = green | yellow | brown

        total_px = max(1, h * w)
        leaf_px = int(np.count_nonzero(leaf_mask))
        plant_bbox = None
        if leaf_px:
            ys, xs = np.where(leaf_mask)
            x1, x2 = int(xs.min()), int(xs.max())
            y1, y2 = int(ys.min()), int(ys.max())
            pad_x = max(2, int((x2 - x1 + 1) * 0.04))
            pad_y = max(2, int((y2 - y1 + 1) * 0.04))
            x1 = max(0, x1 - pad_x)
            y1 = max(0, y1 - pad_y)
            x2 = min(w - 1, x2 + pad_x)
            y2 = min(h - 1, y2 + pad_y)
            plant_bbox = [x1, y1, x2, y2]
        if leaf_px < max(300, int(total_px * 0.03)):
            return {
                "enabled": True,
                "width": w,
                "height": h,
                "leaf_visible": False,
                "leaf_coverage": round(leaf_px / total_px, 3),
                "blur_score": round(blur_score, 1),
                "image_quality": "blurry" if blur_score < 80 else "usable",
                "symptoms": ["khong thay la ro trong khung hinh"],
                "health_score": None,
                "plant_bbox_xyxy": plant_bbox,
                "summary_vi": "Chua thay la ro trong khung hinh; nen dua la vao giua camera va giu yen may.",
            }

        yellow_ratio = float(np.count_nonzero(yellow & leaf_mask)) / leaf_px
        brown_ratio = float(np.count_nonzero(brown & leaf_mask)) / leaf_px
        spot_ratio = float(np.count_nonzero((brown | dark_spots) & leaf_mask)) / leaf_px
        green_ratio = float(np.count_nonzero(green & leaf_mask)) / leaf_px
        leaf_coverage = leaf_px / total_px
        plant_like = green_ratio >= 0.05 or (green_ratio >= 0.03 and leaf_coverage >= 0.10)
        if not plant_like:
            return {
                "enabled": True,
                "width": w,
                "height": h,
                "leaf_visible": False,
                "leaf_coverage": round(leaf_coverage, 3),
                "green_ratio": round(green_ratio, 3),
                "yellow_ratio": round(yellow_ratio, 3),
                "brown_ratio": round(brown_ratio, 3),
                "spot_ratio": round(spot_ratio, 3),
                "plant_bbox_xyxy": None,
                "plant_bbox_norm": None,
                "blur_score": round(blur_score, 1),
                "image_quality": "blurry" if blur_score < 80 else "usable",
                "symptoms": ["khong thay vung cay/la du ro"],
                "health_score": None,
                "summary_vi": "Chua thay vung cay/la du ro; dua cay vao giua camera de he thong ve khung.",
            }

        spot_mask = ((brown | dark_spots) & leaf_mask).astype("uint8") * 255
        contours, _ = cv2.findContours(spot_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        spot_count = sum(1 for c in contours if cv2.contourArea(c) >= 8)

        symptoms: list[str] = []
        if blur_score < 80:
            symptoms.append("hinh hoi mo")
        if yellow_ratio >= 0.18:
            symptoms.append("vang la nhieu")
        elif yellow_ratio >= 0.08:
            symptoms.append("co vung vang nhe")
        if spot_ratio >= 0.10 or spot_count >= 12:
            symptoms.append("nhieu dom/nau den")
        elif spot_ratio >= 0.04 or spot_count >= 4:
            symptoms.append("co dom nghi benh")
        if brown_ratio >= 0.08:
            symptoms.append("co vung nau/kho")
        if not symptoms:
            symptoms.append("mau la kha deu")

        penalty = yellow_ratio * 45.0 + spot_ratio * 70.0 + brown_ratio * 55.0
        if blur_score < 80:
            penalty += 10.0
        health_score = max(0, min(100, int(round(100.0 - penalty))))
        if health_score >= 75:
            severity = "nhe hoac chua ro"
        elif health_score >= 50:
            severity = "can theo doi"
        else:
            severity = "nghi ngo cao"

        summary = (
            f"La chiem {leaf_coverage:.0%} khung hinh, xanh {green_ratio:.0%}, "
            f"vang {yellow_ratio:.0%}, dom/nau den {spot_ratio:.0%}, diem suc khoe ~{health_score}/100."
        )
        return {
            "enabled": True,
            "width": w,
            "height": h,
            "leaf_visible": True,
            "leaf_coverage": round(leaf_coverage, 3),
            "plant_bbox_xyxy": plant_bbox,
            "plant_bbox_norm": [round(plant_bbox[0] / w, 4), round(plant_bbox[1] / h, 4), round(plant_bbox[2] / w, 4), round(plant_bbox[3] / h, 4)] if plant_bbox else None,
            "green_ratio": round(green_ratio, 3),
            "yellow_ratio": round(yellow_ratio, 3),
            "brown_ratio": round(brown_ratio, 3),
            "spot_ratio": round(spot_ratio, 3),
            "spot_count": int(spot_count),
            "blur_score": round(blur_score, 1),
            "image_quality": "blurry" if blur_score < 80 else "usable",
            "symptoms": symptoms,
            "severity": severity,
            "health_score": health_score,
            "summary_vi": summary,
        }
    except Exception as exc:
        return {"enabled": True, "error": str(exc)}


def plant_detection_from_leaf_analysis(jpeg: bytes, leaf_analysis: dict[str, Any] | None = None) -> dict[str, Any]:
    leaf = leaf_analysis if isinstance(leaf_analysis, dict) else analyze_leaf_health(jpeg)
    bbox = leaf.get("plant_bbox_xyxy") if isinstance(leaf, dict) else None
    width = int(leaf.get("width") or 0) if isinstance(leaf, dict) else 0
    height = int(leaf.get("height") or 0) if isinstance(leaf, dict) else 0
    detections: list[dict[str, Any]] = []
    if leaf.get("leaf_visible") and bbox and width > 0 and height > 0:
        coverage = float(leaf.get("leaf_coverage") or 0.0)
        confidence = max(0.35, min(0.98, 0.45 + coverage))
        detections.append({
            "label": "plant",
            "label_vi": "cay/la",
            "confidence": round(confidence, 3),
            "bbox_xyxy": [round(float(v), 1) for v in bbox],
            "bbox_norm": [round(float(bbox[0]) / width, 4), round(float(bbox[1]) / height, 4), round(float(bbox[2]) / width, 4), round(float(bbox[3]) / height, 4)],
            "source": "leaf_color_mask",
        })
    return {
        "enabled": True,
        "model": "plant-color-mask",
        "backend": "opencv",
        "width": width,
        "height": height,
        "detections": detections,
        "detections_count": len(detections),
        "leaf_analysis": leaf,
        "error": None if detections else "no_plant_region",
        "created_ms": now_ms(),
    }


def draw_plant_overlay(jpeg: bytes) -> bytes:
    result = plant_detection_from_leaf_analysis(jpeg)
    detections = result.get("detections") or []
    if not detections:
        return jpeg
    try:
        import cv2
        img = _decode_jpeg_to_cv2(jpeg)
        if img is None:
            return jpeg
        for det in detections:
            x1, y1, x2, y2 = [int(v) for v in det["bbox_xyxy"]]
            label = f"{det.get('label_vi') or 'cay/la'} {det['confidence']:.2f}"
            cv2.rectangle(img, (x1, y1), (x2, y2), (20, 190, 70), 2)
            label_w = max(70, len(label) * 8)
            cv2.rectangle(img, (x1, max(0, y1 - 22)), (min(img.shape[1] - 1, x1 + label_w), y1), (20, 150, 60), -1)
            cv2.putText(img, label, (x1 + 4, max(14, y1 - 6)), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 255, 255), 1, cv2.LINE_AA)
        return _encode_cv2_to_jpeg(img, quality=58)
    except Exception:
        return jpeg


def _is_onnx_model(model_name: str) -> bool:
    return str(model_name).lower().endswith(".onnx")


def _is_torchvision_detector(model_name: str) -> bool:
    return str(model_name).lower().startswith("torchvision:")

def _torchvision_model_key(model_name: str) -> str:
    return str(model_name).split(":", 1)[1].strip().lower()

def _load_torchvision_detector(model_name: str):
    """Load a non-YOLO detector from torchvision.

    These models are not ONNX in this dev path, but they are real deep-learning
    detectors with bbox + labels. They are useful for comparing non-YOLO behavior
    on CPU.
    """
    import torch
    key = _torchvision_model_key(model_name)
    if key == "ssdlite320_mobilenet_v3_large":
        from torchvision.models.detection import ssdlite320_mobilenet_v3_large, SSDLite320_MobileNet_V3_Large_Weights
        weights = SSDLite320_MobileNet_V3_Large_Weights.DEFAULT
        model = ssdlite320_mobilenet_v3_large(weights=weights)
    elif key == "fasterrcnn_mobilenet_v3_large_320_fpn":
        from torchvision.models.detection import fasterrcnn_mobilenet_v3_large_320_fpn, FasterRCNN_MobileNet_V3_Large_320_FPN_Weights
        weights = FasterRCNN_MobileNet_V3_Large_320_FPN_Weights.DEFAULT
        model = fasterrcnn_mobilenet_v3_large_320_fpn(weights=weights)
    elif key == "fasterrcnn_mobilenet_v3_large_fpn":
        from torchvision.models.detection import fasterrcnn_mobilenet_v3_large_fpn, FasterRCNN_MobileNet_V3_Large_FPN_Weights
        weights = FasterRCNN_MobileNet_V3_Large_FPN_Weights.DEFAULT
        model = fasterrcnn_mobilenet_v3_large_fpn(weights=weights)
    elif key == "retinanet_resnet50_fpn":
        from torchvision.models.detection import retinanet_resnet50_fpn, RetinaNet_ResNet50_FPN_Weights
        weights = RetinaNet_ResNet50_FPN_Weights.DEFAULT
        model = retinanet_resnet50_fpn(weights=weights)
    elif key == "fcos_resnet50_fpn":
        from torchvision.models.detection import fcos_resnet50_fpn, FCOS_ResNet50_FPN_Weights
        weights = FCOS_ResNet50_FPN_Weights.DEFAULT
        model = fcos_resnet50_fpn(weights=weights)
    else:
        raise ValueError(f"unsupported torchvision detector: {model_name}")
    model.eval()
    model.to(torch.device(preferred_ai_device()))
    categories = list(getattr(weights, "meta", {}).get("categories", []))
    return model, categories

def _model_metric_key(kind: str, model: str, imgsz: int | None = None) -> str:
    if imgsz:
        return f"{kind}:{model}@{imgsz}"
    return f"{kind}:{model}"

def _update_metric(bucket: dict[str, Any], key: str, latency_ms: float, count_delta: int = 1, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    item = bucket.setdefault(key, {"count": 0, "last_ms": None, "avg_ms": None, "min_ms": None, "max_ms": None, "approx_fps": 0.0})
    n = int(item.get("count") or 0)
    avg = float(item.get("avg_ms") or latency_ms)
    new_n = n + count_delta
    new_avg = latency_ms if n == 0 else ((avg * n) + latency_ms) / new_n
    item.update({
        "count": new_n,
        "last_ms": round(float(latency_ms), 1),
        "avg_ms": round(float(new_avg), 1),
        "min_ms": round(float(latency_ms if item.get("min_ms") is None else min(float(item["min_ms"]), latency_ms)), 1),
        "max_ms": round(float(latency_ms if item.get("max_ms") is None else max(float(item["max_ms"]), latency_ms)), 1),
        "approx_fps": round(1000.0 / new_avg, 2) if new_avg > 0 else 0.0,
        "updated_ms": now_ms(),
    })
    if extra:
        item.update(extra)
    return item


def _onnx_target_path(model_name: str, imgsz: int) -> Path:
    stem = Path(model_name).stem
    # keep one ONNX per imgsz because fixed-shape export is faster than dynamic on CPU
    return AI_MODEL_DIR / "onnx" / f"{stem}-{int(imgsz)}.onnx"


def _resolved_onnx_imgsz(resolved_model: str | None, fallback: int) -> int:
    """Extract the fixed export size from names like yolo11n-320.onnx.

    Ultralytics ONNX exports in this project are fixed-shape for CPU speed.
    If the UI changes imgsz while a previous ONNX model is still loaded, passing
    the new size to a fixed old ONNX file causes:
      INVALID_ARGUMENT: Got 416 Expected 320
    This guard keeps overlay stable instead of breaking the video stream.
    """
    try:
        name = Path(str(resolved_model or "")).stem
        tail = name.rsplit("-", 1)[-1]
        if tail.isdigit():
            return int(tail)
    except Exception:
        pass
    return int(fallback)


def _export_onnx_if_needed(model_name: str, imgsz: int) -> str:
    """Return a local model path. If model_name is *.onnx and missing, export from matching *.pt.

    Example: yolo11n.onnx -> export yolo11n.pt to models/onnx/yolo11n-416.onnx.
    Fixed imgsz + simplify keeps CPU inference predictable.
    """
    name = str(model_name).strip()
    if not _is_onnx_model(name):
        return name

    path = Path(name)
    if path.exists():
        return str(path)

    target = _onnx_target_path(name, imgsz)
    if target.exists():
        return str(target)

    target.parent.mkdir(parents=True, exist_ok=True)
    from ultralytics import YOLO
    base_pt = Path(name).with_suffix(".pt").name
    exporter = YOLO(base_pt)
    exported = exporter.export(format="onnx", imgsz=int(imgsz), simplify=True, dynamic=False, opset=12)
    exported_path = Path(str(exported))
    if not exported_path.exists():
        # Ultralytics commonly exports beside the .pt with same basename.
        exported_path = Path(base_pt).with_suffix(".onnx")
    if exported_path.exists() and exported_path.resolve() != target.resolve():
        shutil.copy2(exported_path, target)
    if not target.exists():
        raise RuntimeError(f"ONNX export finished but target file was not found: {target}")
    return str(target)



def _clean_vlm_text(raw: str) -> str:
    """Best-effort cleanup for chat-template model output.

    Some VLM decoders echo the prompt/chat template. Keep the final assistant-like
    answer compact for the frontend chat bubble, while preserving raw in JSON.
    """
    text = (raw or "").strip()
    for sep in ["Assistant:", "assistant", "<|assistant|>", "ASSISTANT:"]:
        if sep in text:
            text = text.split(sep)[-1].strip()
    # If model echoes a JSON object, extract the user-facing answer when possible.
    if '"answer_vi"' in text:
        try:
            start = text.find("{")
            end = text.rfind("}")
            if start >= 0 and end > start:
                obj = json.loads(text[start:end + 1])
                if isinstance(obj, dict) and obj.get("answer_vi"):
                    text = str(obj["answer_vi"]).strip()
        except Exception:
            pass
    for fence in ["```json", "```"]:
        text = text.replace(fence, "").strip()
    # Avoid huge chat bubbles on weak machines.
    if len(text) > 1200:
        text = text[-1200:].strip()
    return text or "Mình chưa tạo được câu trả lời rõ ràng từ VLM."


def _pick_label(raw: str, labels: list[str], default: str = "uncertain", priority: list[str] | None = None) -> str:
    text = (raw or "").lower().strip()
    text = text.replace("-", "_").replace("slow down", "slow_down").replace("speed up", "speed_up")
    for ch in ".,:;()[]{}":
        text = text.replace(ch, " ")
    hits = []
    for label in labels:
        if f" {label.lower()} " in f" {text} ":
            hits.append(label)
    if priority:
        for label in priority:
            if label in hits:
                return label
    return hits[0] if len(hits) == 1 else default


def _has_any(text: str, words: list[str]) -> bool:
    low = (text or "").lower()
    return any(w in low for w in words)


def _caption_has_person(text: str) -> bool:
    return _has_any(text, ["person", "people", "man", "woman", "child", "human", "người"])


def _caption_has_vehicle(text: str) -> bool:
    return _has_any(text, ["car", "truck", "bus", "motorcycle", "bike", "bicycle", "vehicle", "xe"])


def _caption_has_obstacle(text: str) -> bool:
    return _has_any(text, ["obstacle", "chair", "box", "wall", "door", "table", "bench", "blocked", "barrier", "vật cản"])


def _normalize_robot_action(path_status: str, action: str, caption: str, detector_labels: set[str] | None = None) -> str:
    detector_labels = detector_labels or set()
    risky_labels = {"person", "car", "truck", "bus", "motorcycle", "bicycle", "dog", "cat", "chair", "bench"}
    if path_status == "blocked":
        return "stop"
    if detector_labels & risky_labels:
        return "slow_down"
    if _caption_has_person(caption) or _caption_has_vehicle(caption) or _caption_has_obstacle(caption):
        return "slow_down"
    if path_status in {"crowded", "uncertain"}:
        return "slow_down"
    if path_status == "clear" and action in {"stop", "turn"}:
        return "go"
    return action if action in {"go", "slow_down", "stop", "turn", "speed_up"} else "slow_down"


def _build_robot_answer_vi(caption: str, path_status: str, action: str, question: str | None = None) -> str:
    caption = _clean_vlm_text(caption).strip()
    if not caption:
        caption = "khung hình chưa đủ rõ để nhận diện chắc chắn"

    status_vi = {
        "clear": "Lối phía trước có vẻ khá thoáng.",
        "crowded": "Phía trước hơi đông hoặc có nhiều vật thể gần robot.",
        "blocked": "Phía trước có dấu hiệu bị chắn, robot không nên đi thẳng.",
        "uncertain": "Khung hình chưa đủ chắc chắn, robot nên thận trọng.",
    }.get(path_status, "Khung hình chưa đủ chắc chắn, robot nên thận trọng.")

    advice_vi = {
        "go": "Có thể đi tiếp chậm rãi và tiếp tục quan sát.",
        "slow_down": "Nên giảm tốc, giữ khoảng cách và quan sát thêm trước khi tiến.",
        "stop": "Nên dừng lại để tránh va chạm.",
        "turn": "Nên rẽ hoặc đổi hướng để tránh vùng phía trước.",
        "speed_up": "Chỉ nên tăng tốc nhẹ nếu người điều khiển xác nhận đường thật sự trống.",
    }.get(action, "Nên đi chậm và quan sát thêm.")

    extra = ""
    if question:
        extra = f"\n\nTrả lời câu hỏi: {question.strip()}\nMình dựa trên frame hiện tại; nếu hình mờ hoặc robot đang di chuyển nhanh thì nên kiểm tra lại bằng Video raw."

    return f"Phía trước: {caption}\n\nTình trạng: {status_vi}\n\nLời khuyên: {advice_vi}{extra}"


def _answer_needs_vi_fallback(answer: str | None) -> bool:
    text = (answer or "").strip().lower()
    if not text:
        return True
    vi_marks = "ăâđêôơưáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ"
    if any(ch in text for ch in vi_marks):
        return False
    english_robot_phrases = (
        "the robot",
        "the image",
        "not using",
        "esp32",
        "camera",
        "visible",
        "person",
        "foreground",
        "holding",
        "cell phone",
        "object",
        "people",
        "dark background",
        "no visible",
    )
    return any(phrase in text for phrase in english_robot_phrases)


def _fallback_robot_answer_vi(answer: str | None, detections: dict[str, Any], safety: dict[str, Any]) -> str:
    dets = detections.get("detections", []) if isinstance(detections, dict) else []
    names: list[str] = []
    for det in dets[:5]:
        label = det.get("label") or det.get("name") or "vật thể"
        conf = det.get("confidence") or det.get("conf")
        if isinstance(conf, (int, float)):
            names.append(f"{label} {round(conf * 100)}%")
        else:
            names.append(str(label))

    if names:
        front = f"Detector đang thấy: {', '.join(names)}."
    else:
        front = "Khung hình hiện tại khá tối hoặc mờ, detector chưa nhận diện được vật thể rõ ràng."

    action = (safety or {}).get("safe_action") or "slow_down"
    advice = {
        "go": "Có thể đi tiếp rất chậm nếu người điều khiển xác nhận đường phía trước trống.",
        "slow_down": "Nên đi chậm, giữ khoảng cách và quan sát thêm trước khi tiến.",
        "stop": "Nên dừng lại ngay để tránh va chạm.",
        "turn": "Nên rẽ hoặc đổi hướng nếu phía trước không an toàn.",
        "speed_up": "Chỉ nên tăng tốc nhẹ khi người điều khiển xác nhận đường thật sự trống.",
    }.get(action, "Nên đi chậm và quan sát thêm.")

    note = ""
    if answer:
        note = " VLM trả lời chưa đúng ngữ cảnh camera robot, nên hệ thống ưu tiên detector và luật an toàn."

    return f"Phía trước: {front}\n\nTình trạng: Chưa đủ chắc chắn để kết luận đường hoàn toàn trống.{note}\n\nLời khuyên: {advice}"


def _scene_answer_vi(scene: dict[str, Any] | None, detections: dict[str, Any], safety: dict[str, Any]) -> str:
    answer = scene.get("answer_vi") if isinstance(scene, dict) else None
    if _answer_needs_vi_fallback(answer):
        answer = _fallback_robot_answer_vi(answer, detections, safety)
        if isinstance(scene, dict):
            scene["raw_answer_vi"] = scene.get("answer_vi")
            scene["answer_vi"] = answer
            scene["vi_fallback"] = True
    return str(answer or "")


def _robot_chat_prompt(question: str | None = None) -> str:
    user_question = (question or "").strip()
    task = user_question[:600] if user_question else "Hãy phân tích khung hình hiện tại cho người đang điều khiển robot."
    return (
        "You are the vision assistant for a small ESP32-CAM robot.\n"
        "Look at the front-camera image and answer in natural Vietnamese.\n"
        "Do not output JSON, markdown tables, code blocks, or raw debug fields.\n"
        "Use this exact friendly structure:\n"
        "Phía trước: one short sentence describing the scene, main objects, people, vehicles, or obstacles.\n"
        "Tình trạng: choose clear, crowded, blocked, or uncertain, then explain briefly.\n"
        "Lời khuyên: choose go, slow_down, stop, or turn, then give a safe driving suggestion.\n"
        "If the image is blurry or uncertain, say you are not sure and recommend slow_down.\n"
        f"User question: {task}"
    )

def _leaf_chat_prompt(leaf_metrics: dict[str, Any] | None = None, question: str | None = None) -> str:
    task = (question or "").strip()[:600] or "Phan tich suc khoe la cay trong khung hinh hien tai."
    metrics = leaf_metrics if isinstance(leaf_metrics, dict) else {}
    metric_summary = metrics.get("summary_vi") or "Chua co thong so mau la."
    symptoms = ", ".join(str(x) for x in metrics.get("symptoms", [])[:6]) if isinstance(metrics.get("symptoms"), list) else "khong ro"
    return (
        "You are an agricultural leaf-health vision assistant.\n"
        "Look only at the visible plant leaves in this ESP32-CAM image.\n"
        "Answer in Vietnamese. Do not output JSON, markdown tables, code blocks, or raw debug fields.\n"
        "Focus on leaf disease symptoms: yellowing, brown/black spots, dry edges, mold-like areas, holes, and whether the image is too blurry.\n"
        "Do not give a definitive medical/agronomy diagnosis from one frame; state confidence and what to check next.\n"
        "Use this exact structure:\n"
        "Tong quan: one short sentence about leaf visibility and image quality.\n"
        "Dau hieu: describe visible healthy/diseased signs and likely severity.\n"
        "Khuyen nghi: practical next steps such as retake a closer image, isolate affected leaves, reduce overhead watering, improve airflow, or inspect underside.\n"
        f"Computer vision quick metrics: {metric_summary} Symptoms: {symptoms}.\n"
        f"User question: {task}"
    )


def _leaf_answer_needs_fallback(answer: str | None) -> bool:
    if _answer_needs_vi_fallback(answer):
        return True
    text = (answer or "").strip().lower()
    plain = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    robot_words_plain = [
        "phia truoc",
        "tinh trang",
        "loi khuyen",
        "duong trong",
        "vat can",
        "di tiep",
        "dung lai",
        "robot",
        "lai xe",
    ]
    leaf_words_plain = [
        "tong quan",
        "dau hieu",
        "khuyen nghi",
        " la ",
        " cay ",
        " benh",
        " dom",
        " vang",
        " nau",
        " kho",
        " moc",
    ]
    if any(w in plain for w in robot_words_plain):
        return True
    if not any(w in f" {plain} " for w in leaf_words_plain):
        return True
    robot_words = ["robot", "vat can", "vật cản", "phia truoc", "phía trước", "di tiep", "đi tiếp", "dung lai", "dừng lại"]
    leaf_words = ["la", "lá", "cay", "cây", "benh", "bệnh", "dom", "đốm", "vang", "vàng", "nau", "nâu"]
    return any(w in text for w in robot_words) and not any(w in text for w in leaf_words)


def _build_leaf_answer_vi(scene: dict[str, Any] | None, leaf_metrics: dict[str, Any]) -> str:
    answer = scene.get("answer_vi") if isinstance(scene, dict) else None
    if not _leaf_answer_needs_fallback(answer):
        return str(answer)

    symptom_labels = {
        "khong thay la ro trong khung hinh": "không thấy lá rõ trong khung hình",
        "hinh hoi mo": "hình hơi mờ",
        "vang la nhieu": "vàng lá nhiều",
        "co vung vang nhe": "có vùng vàng nhẹ",
        "nhieu dom/nau den": "nhiều đốm nâu/đen",
        "co dom nghi benh": "có đốm nghi bệnh",
        "co vung nau/kho": "có vùng nâu hoặc khô mép",
        "mau la kha deu": "màu lá khá đều",
    }
    raw_symptoms = leaf_metrics.get("symptoms") if isinstance(leaf_metrics, dict) else []
    symptoms = [symptom_labels.get(str(s), str(s)) for s in raw_symptoms] if isinstance(raw_symptoms, list) else []
    symptom_text = ", ".join(symptoms[:5]) or "chưa có dấu hiệu rõ"

    if not leaf_metrics.get("leaf_visible", True):
        overview = "Tổng quan: Chưa thấy lá đủ rõ trong khung hình."
        signs = f"Dấu hiệu: {symptom_text}."
        advice = "Khuyến nghị: Đưa lá vào giữa camera, giữ camera đứng yên và chụp gần hơn dưới ánh sáng đều rồi phân tích lại."
    else:
        health = leaf_metrics.get("health_score")
        severity = leaf_metrics.get("severity") or "chưa rõ"
        overview = f"Tổng quan: Ảnh dùng được để xem nhanh lá, điểm sức khỏe ước tính khoảng {health}/100." if health is not None else "Tổng quan: Ảnh dùng được để xem nhanh lá."
        signs = f"Dấu hiệu: {symptom_text}; mức độ {severity}."
        advice = "Khuyến nghị: Chụp thêm ảnh cận cảnh mặt trên và mặt dưới lá. Nếu đốm nâu/đen lan rộng, nên tách lá bệnh, tránh tưới lên lá và tăng thông thoáng."

    fallback = f"{overview}\n\n{signs}\n\n{advice}"
    if isinstance(scene, dict):
        scene["raw_answer_vi"] = scene.get("answer_vi")
        scene["answer_vi"] = fallback
        scene["vi_fallback"] = True
    return fallback


class VisionAI:
    def __init__(self):
        self.yolo = None
        self.yolo_error: str | None = None
        self.yolo_backend: str | None = None
        self.yolo_resolved_model: str | None = None
        self.torchvision_categories: list[str] = []
        self.vlm_loaded = False
        self.vlm_error: str | None = None
        self.processor = None
        self.vlm_model = None
        self.vlm_family: str | None = None
        self.lock = threading.Lock()
        self.cache: dict[str, Any] = {"detections": [], "frame_ms": None, "created_ms": 0, "error": None}
        self.detect_count = 0
        self.detect_error_count = 0
        self.detect_latency_ms: deque[float] = deque(maxlen=80)
        self.last_detect_ms: float | None = None
        self.last_detect_created_ms: int | None = None
        self.last_detect_objects = 0
        self.vlm_latency_ms: deque[float] = deque(maxlen=20)
        self.last_vlm_ms: float | None = None
        self.last_vlm_created_ms: int | None = None
        self.detector_metrics: dict[str, dict[str, Any]] = {}
        self.vlm_metrics: dict[str, dict[str, Any]] = {}

    def _load_yolo(self):
        if self.yolo is not None or self.yolo_error:
            return
        try:
            if _is_torchvision_detector(AI_YOLO_MODEL):
                model, categories = _load_torchvision_detector(AI_YOLO_MODEL)
                self.yolo = model
                self.torchvision_categories = categories
                self.yolo_resolved_model = AI_YOLO_MODEL
                self.yolo_backend = "torchvision-cpu"
            else:
                from ultralytics import YOLO
                resolved_model = _export_onnx_if_needed(AI_YOLO_MODEL, AI_YOLO_IMGSZ)
                self.yolo = YOLO(resolved_model)
                self.yolo_resolved_model = resolved_model
                self.yolo_backend = "onnxruntime-cpu" if _is_onnx_model(resolved_model) else "pytorch-cpu"
        except Exception as exc:
            self.yolo_error = str(exc)

    def detect(self, jpeg: bytes, force: bool = False) -> dict[str, Any]:
        now = now_ms()
        with self.lock:
            if (not force) and self.cache.get("created_ms") and (now - self.cache["created_ms"] < AI_DETECT_INTERVAL_S * 1000):
                return dict(self.cache)

        if not AI_ENABLE_YOLO:
            return {"enabled": False, "detections": [], "error": "AI_ENABLE_YOLO=0"}

        self._load_yolo()
        if self.yolo_error:
            return {"enabled": True, "detections": [], "error": self.yolo_error}

        try:
            img = _decode_jpeg_to_cv2(jpeg)
            if img is None:
                raise RuntimeError("cannot decode jpeg")
            h, w = img.shape[:2]
            detections: list[dict[str, Any]] = []
            t0 = time.perf_counter()

            if _is_torchvision_detector(AI_YOLO_MODEL):
                import torch
                from PIL import Image
                from torchvision.transforms.functional import to_tensor
                rgb = img[:, :, ::-1]
                image = Image.fromarray(rgb)
                # Keep input modest on CPU. Detection models may still resize internally,
                # but this avoids feeding very large frames if the camera is upgraded later.
                scale = min(1.0, float(AI_YOLO_IMGSZ) / max(float(w), float(h))) if AI_YOLO_IMGSZ else 1.0
                infer_w, infer_h = w, h
                if scale < 1.0:
                    infer_w, infer_h = max(1, int(w * scale)), max(1, int(h * scale))
                    image = image.resize((infer_w, infer_h))
                tensor = to_tensor(image)
                with torch.no_grad():
                    pred = self.yolo([tensor])[0]
                boxes = pred.get("boxes", [])
                scores = pred.get("scores", [])
                labels = pred.get("labels", [])
                inv_scale = 1.0 / scale if scale > 0 else 1.0
                for box, score, label_id in zip(boxes, scores, labels):
                    conf = float(score.item())
                    if conf < AI_CONF_THRESHOLD:
                        continue
                    x1, y1, x2, y2 = [float(v) * inv_scale for v in box.tolist()]
                    cls_id = int(label_id.item())
                    label = self.torchvision_categories[cls_id] if 0 <= cls_id < len(self.torchvision_categories) else str(cls_id)
                    detections.append({
                        "label": str(label),
                        "confidence": round(conf, 3),
                        "bbox_xyxy": [round(x1, 1), round(y1, 1), round(x2, 1), round(y2, 1)],
                        "bbox_norm": [round(x1 / w, 4), round(y1 / h, 4), round(x2 / w, 4), round(y2 / h, 4)],
                    })
            else:
                predict_imgsz = _resolved_onnx_imgsz(self.yolo_resolved_model, AI_YOLO_IMGSZ) if _is_onnx_model(self.yolo_resolved_model or AI_YOLO_MODEL) else AI_YOLO_IMGSZ
                predict_kwargs = {"conf": AI_CONF_THRESHOLD, "imgsz": predict_imgsz, "verbose": False}
                if not _is_onnx_model(self.yolo_resolved_model or AI_YOLO_MODEL):
                    predict_kwargs["device"] = 0 if preferred_ai_device() == "cuda" else "cpu"
                results = self.yolo.predict(img, **predict_kwargs)
                for result in results:
                    names = result.names
                    for box in result.boxes:
                        x1, y1, x2, y2 = [float(v) for v in box.xyxy[0].tolist()]
                        cls_id = int(box.cls[0].item())
                        conf = float(box.conf[0].item())
                        detections.append({
                            "label": str(names.get(cls_id, cls_id)),
                            "confidence": round(conf, 3),
                            "bbox_xyxy": [round(x1, 1), round(y1, 1), round(x2, 1), round(y2, 1)],
                            "bbox_norm": [round(x1 / w, 4), round(y1 / h, 4), round(x2 / w, 4), round(y2 / h, 4)],
                        })

            latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
            metric_key = _model_metric_key("detector", AI_YOLO_MODEL, AI_YOLO_IMGSZ)
            out = {"enabled": True, "model": AI_YOLO_MODEL, "resolved_model": self.yolo_resolved_model, "backend": self.yolo_backend, "conf_threshold": AI_CONF_THRESHOLD, "imgsz": AI_YOLO_IMGSZ, "width": w, "height": h, "detections": detections, "detections_count": len(detections), "inference_ms": latency_ms, "inference_fps": round(1000.0 / latency_ms, 2) if latency_ms > 0 else 0.0, "metric_key": metric_key, "created_ms": now_ms(), "error": None}
            with self.lock:
                self.cache = dict(out)
                self.detect_count += 1
                self.detect_latency_ms.append(latency_ms)
                self.last_detect_ms = latency_ms
                self.last_detect_created_ms = out["created_ms"]
                self.last_detect_objects = len(detections)
                out["model_metric"] = dict(_update_metric(self.detector_metrics, metric_key, latency_ms, extra={"model": AI_YOLO_MODEL, "imgsz": AI_YOLO_IMGSZ, "backend": self.yolo_backend, "last_objects": len(detections)}))
            return out
        except Exception as exc:
            with self.lock:
                self.detect_error_count += 1
            return {"enabled": True, "model": AI_YOLO_MODEL, "backend": self.yolo_backend, "resolved_model": self.yolo_resolved_model, "detections": [], "detections_count": 0, "error": str(exc)}

    def draw_overlay(self, jpeg: bytes) -> bytes:
        """Draw only the plant/leaf box for the AI overlay stream."""
        return draw_plant_overlay(jpeg)

    def _load_vlm(self):
        if AI_VLM_PROVIDER in {"llama", "llama_server", "openai"}:
            self.vlm_family = "llama_server"
            self.vlm_loaded = True
            return
        if self.vlm_loaded or self.vlm_error:
            return
        try:
            import torch
            from transformers import AutoProcessor
            model_name = str(AI_VLM_MODEL)
            self.processor = AutoProcessor.from_pretrained(model_name, trust_remote_code=True)
            device_name = preferred_ai_device()
            dtype = torch.float16 if device_name == "cuda" else torch.float32
            if model_name.lower().startswith("microsoft/florence-2"):
                from transformers import AutoModelForCausalLM
                self.vlm_model = AutoModelForCausalLM.from_pretrained(model_name, torch_dtype=dtype, trust_remote_code=True)
                self.vlm_model.to(torch.device(device_name))
                self.vlm_family = "florence2"
            else:
                try:
                    from transformers import AutoModelForImageTextToText as _AutoVlmModel
                except Exception:
                    from transformers import AutoModelForVision2Seq as _AutoVlmModel
                if device_name == "cuda":
                    self.vlm_model = _AutoVlmModel.from_pretrained(model_name, torch_dtype=dtype, device_map="auto", trust_remote_code=True)
                else:
                    self.vlm_model = _AutoVlmModel.from_pretrained(model_name, torch_dtype=dtype, trust_remote_code=True)
                    self.vlm_model.to(torch.device("cpu"))
                self.vlm_family = "chat_vlm"
            self.vlm_model.eval()
            self.vlm_loaded = True
        except Exception as exc:
            self.vlm_error = str(exc)

    def _analyze_scene_llama_server(self, jpeg: bytes, question: str | None = None, detector_labels: set[str] | None = None) -> dict[str, Any]:
        image_b64 = base64.b64encode(jpeg).decode("ascii")
        prompt = _robot_chat_prompt(question)
        payload = {
            "model": AI_VLM_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"}},
                    ],
                }
            ],
            "max_tokens": AI_VLM_MAX_NEW_TOKENS,
            "temperature": 0,
        }
        url = f"{AI_VLM_OPENAI_BASE_URL}/v1/chat/completions"
        t0 = time.perf_counter()
        try:
            req = Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(req, timeout=AI_VLM_TIMEOUT_S) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            self.vlm_error = f"llama_server_unavailable: {exc}"
            return {
                "enabled": True,
                "model": AI_VLM_MODEL,
                "family": "llama_server",
                "provider": AI_VLM_PROVIDER,
                "base_url": AI_VLM_OPENAI_BASE_URL,
                "timeout_s": AI_VLM_TIMEOUT_S,
                "error": self.vlm_error,
            }

        latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
        content = ""
        try:
            content = body["choices"][0]["message"]["content"]
        except Exception:
            content = json.dumps(body, ensure_ascii=False)[:1200]
        answer = _clean_vlm_text(content)
        path_status = _pick_label(answer, ["clear", "crowded", "blocked", "uncertain"], default="uncertain", priority=["blocked", "crowded", "uncertain", "clear"])
        action = _pick_label(answer, ["go", "slow_down", "stop", "turn", "speed_up"], default="slow_down", priority=["stop", "turn", "slow_down", "go", "speed_up"])
        action = _normalize_robot_action(path_status, action, answer, detector_labels=detector_labels)
        created = now_ms()
        metric_key = _model_metric_key("vlm", AI_VLM_MODEL)
        with self.lock:
            self.vlm_error = None
            self.vlm_loaded = True
            self.vlm_family = "llama_server"
            self.vlm_latency_ms.append(latency_ms)
            self.last_vlm_ms = latency_ms
            self.last_vlm_created_ms = created
            metric = dict(_update_metric(self.vlm_metrics, metric_key, latency_ms, extra={"model": AI_VLM_MODEL, "family": "llama_server", "path_status": path_status, "action": action}))
        return {
            "enabled": True,
            "model": AI_VLM_MODEL,
            "family": "llama_server",
            "provider": AI_VLM_PROVIDER,
            "base_url": AI_VLM_OPENAI_BASE_URL,
            "raw": content,
            "answer_vi": answer,
            "path_status": path_status,
            "action": action,
            "inference_ms": latency_ms,
            "inference_fps": round(1000.0 / latency_ms, 2) if latency_ms > 0 else 0.0,
            "metric_key": metric_key,
            "model_metric": metric,
            "created_ms": created,
        }

    def _vlm_generate_text(self, image, prompt: str, max_new_tokens: int | None = None) -> tuple[str, float]:
        import torch

        img = image.convert("RGB")
        img.thumbnail((384, 288))
        messages = [{"role": "user", "content": [{"type": "image"}, {"type": "text", "text": prompt}]}]
        text = self.processor.apply_chat_template(messages, add_generation_prompt=True)
        inputs = self.processor(text=text, images=[img], return_tensors="pt")
        device = next(self.vlm_model.parameters()).device
        inputs = {k: v.to(device) if hasattr(v, "to") else v for k, v in inputs.items()}
        t0 = time.perf_counter()
        with torch.no_grad():
            output_ids = self.vlm_model.generate(
                **inputs,
                max_new_tokens=max_new_tokens or AI_VLM_MAX_NEW_TOKENS,
                do_sample=False,
                repetition_penalty=1.2,
                no_repeat_ngram_size=4,
            )
        latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
        input_len = inputs["input_ids"].shape[1] if "input_ids" in inputs else 0
        new_tokens = output_ids[:, input_len:] if input_len else output_ids
        raw = self.processor.batch_decode(new_tokens, skip_special_tokens=True)[0]
        return _clean_vlm_text(raw), latency_ms

    def analyze_scene(self, jpeg: bytes, question: str | None = None, detector_labels: set[str] | None = None) -> dict[str, Any]:
        if not AI_ENABLE_VLM:
            return {"enabled": False, "error": "AI_ENABLE_VLM=0. Set AI_ENABLE_VLM=1 to enable VLM."}
        self._load_vlm()
        if self.vlm_family == "llama_server":
            return self._analyze_scene_llama_server(jpeg, question=question, detector_labels=detector_labels)
        if self.vlm_error:
            return {"enabled": True, "model": AI_VLM_MODEL, "error": self.vlm_error}
        try:
            import torch
            from PIL import Image
            image = Image.open(BytesIO(jpeg)).convert("RGB")
            created = None
            # Florence-2 has dedicated prompt tasks such as <CAPTION> and <OD>.
            # Use <OD> when the user asks about objects/bbox/obstacles so VLM can
            # return boxes; otherwise use caption-style answer.
            if self.vlm_family == "florence2":
                w, h = image.size
                q = (question or "").lower()
                want_od = any(x in q for x in ["vật", "object", "bbox", "khung", "cản", "người", "xe", "detect", "nhận dạng"])
                task_prompt = "<OD>" if want_od else "<CAPTION>"
                inputs = self.processor(text=task_prompt, images=image, return_tensors="pt")
                device = next(self.vlm_model.parameters()).device
                inputs = {k: v.to(device) if hasattr(v, "to") else v for k, v in inputs.items()}
                t0 = time.perf_counter()
                with torch.no_grad():
                    generated_ids = self.vlm_model.generate(
                        input_ids=inputs.get("input_ids"),
                        pixel_values=inputs.get("pixel_values"),
                        max_new_tokens=AI_VLM_MAX_NEW_TOKENS,
                        num_beams=3,
                    )
                latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                raw = self.processor.batch_decode(generated_ids, skip_special_tokens=False)[0]
                parsed = {}
                try:
                    parsed = self.processor.post_process_generation(raw, task=task_prompt, image_size=(w, h))
                except Exception:
                    parsed = {}
                detections = []
                od = parsed.get("<OD>") or parsed.get(task_prompt) or {}
                bboxes = od.get("bboxes") or []
                labels = od.get("labels") or []
                for box, label in zip(bboxes, labels):
                    x1, y1, x2, y2 = [float(v) for v in box]
                    detections.append({
                        "label": str(label),
                        "confidence": None,
                        "bbox_xyxy": [round(x1, 1), round(y1, 1), round(x2, 1), round(y2, 1)],
                        "bbox_norm": [round(x1 / w, 4), round(y1 / h, 4), round(x2 / w, 4), round(y2 / h, 4)],
                        "source": "florence2_od",
                    })
                caption = ""
                cap = parsed.get("<CAPTION>") or parsed.get(task_prompt)
                if isinstance(cap, str):
                    caption = cap
                elif isinstance(cap, dict):
                    caption = cap.get("caption") or str(cap)
                if detections:
                    names = ", ".join([d["label"] for d in detections[:10]])
                    answer = f"Florence-2 nhận dạng được: {names}. Nếu vật thể nằm gần giữa/đầu khung hình thì nên đi chậm hoặc dừng để kiểm tra."
                else:
                    answer = caption or _clean_vlm_text(raw)
                created = now_ms()
                metric_key = _model_metric_key("vlm", AI_VLM_MODEL)
                with self.lock:
                    self.vlm_latency_ms.append(latency_ms)
                    self.last_vlm_ms = latency_ms
                    self.last_vlm_created_ms = created
                    metric = dict(_update_metric(self.vlm_metrics, metric_key, latency_ms, extra={"model": AI_VLM_MODEL, "family": self.vlm_family, "last_objects": len(detections)}))
                return {"enabled": True, "model": AI_VLM_MODEL, "family": self.vlm_family, "raw": raw, "parsed": parsed, "detections": detections, "detections_count": len(detections), "answer_vi": answer, "inference_ms": latency_ms, "inference_fps": round(1000.0 / latency_ms, 2) if latency_ms > 0 else 0.0, "metric_key": metric_key, "model_metric": metric, "created_ms": created}

            caption_prompt = (
                "You are the vision assistant for a small robot front camera. "
                "Answer in Vietnamese, short and natural. Say what is visible ahead, "
                "whether the path looks clear/crowded/blocked/uncertain, and one safe action "
                "for the robot such as go, slow_down, stop, or turn. Do not use JSON."
            )
            if question:
                caption_prompt += f" User question context: {question[:500]}"

            raw_caption, latency_ms = self._vlm_generate_text(image, caption_prompt, max_new_tokens=min(AI_VLM_MAX_NEW_TOKENS, 48))
            path_status = _pick_label(raw_caption, ["clear", "crowded", "blocked", "uncertain"], default="uncertain", priority=["blocked", "crowded", "uncertain", "clear"])
            action = _pick_label(raw_caption, ["go", "slow_down", "stop", "turn", "speed_up"], default="slow_down", priority=["stop", "turn", "slow_down", "go", "speed_up"])
            action = _normalize_robot_action(path_status, action, raw_caption, detector_labels=detector_labels)
            answer = _build_robot_answer_vi(raw_caption, path_status, action, question=question)
            raw = raw_caption
            created = now_ms()
            metric_key = _model_metric_key("vlm", AI_VLM_MODEL)
            with self.lock:
                self.vlm_latency_ms.append(latency_ms)
                self.last_vlm_ms = latency_ms
                self.last_vlm_created_ms = created
                metric = dict(_update_metric(self.vlm_metrics, metric_key, latency_ms, extra={"model": AI_VLM_MODEL, "family": self.vlm_family, "path_status": path_status, "action": action}))
            debug = {
                "raw_caption": raw_caption,
                "path_status": path_status,
                "action": action,
                "caption_ms": latency_ms,
                "has_vehicle": _caption_has_vehicle(raw_caption),
                "has_person": _caption_has_person(raw_caption),
                "has_obstacle": _caption_has_obstacle(raw_caption),
            }
            return {"enabled": True, "model": AI_VLM_MODEL, "family": self.vlm_family, "raw": raw, "answer_vi": answer, "path_status": path_status, "action": action, "debug": debug, "inference_ms": latency_ms, "inference_fps": round(1000.0 / latency_ms, 2) if latency_ms > 0 else 0.0, "metric_key": metric_key, "model_metric": metric, "created_ms": created}
        except Exception as exc:
            return {"enabled": True, "model": AI_VLM_MODEL, "family": self.vlm_family, "error": str(exc)}


    def preload(self, load_detector: bool = True, load_vlm: bool = False) -> dict[str, Any]:
        """Load the currently selected detector/VLM once so the first UI click is not surprised by download/load latency.

        This intentionally preloads only the selected models. Preloading every VLM/detector on a CPU laptop
        would consume a lot of RAM/disk and can freeze WSL.
        """
        if load_detector and AI_ENABLE_YOLO:
            self._load_yolo()
        if load_vlm and AI_ENABLE_VLM:
            self._load_vlm()
        return self.status()


    def configure(self, update: AIConfigUpdate) -> dict[str, Any]:
        global AI_ENABLE_YOLO, AI_YOLO_MODEL, AI_CONF_THRESHOLD, AI_YOLO_IMGSZ, AI_DETECT_INTERVAL_S
        global AI_ENABLE_VLM, AI_VLM_MODEL
        with self.lock:
            yolo_changed = False
            vlm_changed = False

            if update.enable_yolo is not None and bool(update.enable_yolo) != AI_ENABLE_YOLO:
                AI_ENABLE_YOLO = bool(update.enable_yolo)
                yolo_changed = True
            if update.yolo_model is not None:
                model = str(update.yolo_model).strip()
                if model and model != AI_YOLO_MODEL:
                    AI_YOLO_MODEL = model
                    yolo_changed = True
            if update.yolo_imgsz is not None:
                imgsz = max(160, min(1280, int(update.yolo_imgsz)))
                if imgsz != AI_YOLO_IMGSZ:
                    AI_YOLO_IMGSZ = imgsz
                    yolo_changed = True
            if update.conf_threshold is not None:
                conf = max(0.05, min(0.95, float(update.conf_threshold)))
                if conf != AI_CONF_THRESHOLD:
                    AI_CONF_THRESHOLD = conf
                    yolo_changed = True
            if update.detect_interval_s is not None:
                AI_DETECT_INTERVAL_S = max(0.05, min(5.0, float(update.detect_interval_s)))

            if update.enable_vlm is not None and bool(update.enable_vlm) != AI_ENABLE_VLM:
                AI_ENABLE_VLM = bool(update.enable_vlm)
                vlm_changed = True
            if update.vlm_model is not None:
                model = str(update.vlm_model).strip()
                if model and model != AI_VLM_MODEL:
                    AI_VLM_MODEL = model
                    vlm_changed = True

            if yolo_changed:
                self.yolo = None
                self.yolo_error = None
                self.yolo_backend = None
                self.yolo_resolved_model = None
                self.torchvision_categories = []
                self.cache = {"detections": [], "frame_ms": None, "created_ms": 0, "error": None}
                self.last_detect_ms = None
                self.last_detect_created_ms = None
                self.last_detect_objects = 0
                self.detect_latency_ms.clear()

            if vlm_changed:
                self.vlm_loaded = False
                self.vlm_error = None
                self.processor = None
                self.vlm_model = None
                self.vlm_family = None
                self.last_vlm_ms = None
                self.last_vlm_created_ms = None
                self.vlm_latency_ms.clear()

        return self.status()

    def preset_config(self) -> dict[str, Any]:
        return {
            "detector_presets": DETECTOR_PRESETS,
            "vlm_presets": VLM_PRESETS,
            "quick_profiles": {
                "realtime": {"yolo_model": "yolo11n.onnx", "yolo_imgsz": 320, "conf_threshold": 0.25, "detect_interval_s": 0.20},
                "balanced": {"yolo_model": "yolo11n.onnx", "yolo_imgsz": 320, "conf_threshold": 0.25, "detect_interval_s": 0.30},
                "strong": {"yolo_model": "yolo11s.onnx", "yolo_imgsz": 512, "conf_threshold": 0.25, "detect_interval_s": 0.50},
                "mobilenet": {"yolo_model": "torchvision:ssdlite320_mobilenet_v3_large", "yolo_imgsz": 320, "conf_threshold": 0.30, "detect_interval_s": 0.50},
                "fasterrcnn": {"yolo_model": "torchvision:fasterrcnn_mobilenet_v3_large_320_fpn", "yolo_imgsz": 320, "conf_threshold": 0.35, "detect_interval_s": 1.00},
            },
            "notes": {
                "detectors": "Danh sách đã rút gọn để giữ stream ổn định: YOLO11n realtime, YOLO11n cân bằng, YOLOv8n fallback, SSD MobileNet.",
                "vlm": "VLM dùng cho hỏi đáp/nhận dạng theo frame. Florence-2 có thể trả bbox qua prompt <OD>; các VLM chat khác trả nhãn/vị trí tương đối."
            }
        }

    def status(self) -> dict[str, Any]:
        with self.lock:
            avg_yolo = round(sum(self.detect_latency_ms) / len(self.detect_latency_ms), 1) if self.detect_latency_ms else None
            avg_vlm = round(sum(self.vlm_latency_ms) / len(self.vlm_latency_ms), 1) if self.vlm_latency_ms else None
            detect_fps = round(1000.0 / avg_yolo, 2) if avg_yolo and avg_yolo > 0 else 0.0
            return {
                "yolo": {
                    "enabled": AI_ENABLE_YOLO,
                    "model": AI_YOLO_MODEL,
                    "resolved_model": self.yolo_resolved_model,
                    "backend": self.yolo_backend,
                    "device": preferred_ai_device(),
                    "conf_threshold": AI_CONF_THRESHOLD,
                    "imgsz": AI_YOLO_IMGSZ,
                    "detect_interval_s": AI_DETECT_INTERVAL_S,
                    "loaded": self.yolo is not None,
                    "error": self.yolo_error,
                    "detect_count": self.detect_count,
                    "detect_error_count": self.detect_error_count,
                    "last_inference_ms": self.last_detect_ms,
                    "avg_inference_ms": avg_yolo,
                    "approx_detect_fps": detect_fps,
                    "last_objects": self.last_detect_objects,
                    "last_created_ms": self.last_detect_created_ms,
                    "benchmark": self.detector_metrics.get(_model_metric_key("detector", AI_YOLO_MODEL, AI_YOLO_IMGSZ)),
                },
                "detector_benchmarks": dict(self.detector_metrics),
                "vlm": {
                    "enabled": AI_ENABLE_VLM,
                    "model": AI_VLM_MODEL,
                    "provider": AI_VLM_PROVIDER,
                    "openai_base_url": AI_VLM_OPENAI_BASE_URL if AI_VLM_PROVIDER in {"llama", "llama_server", "openai"} else None,
                    "timeout_s": AI_VLM_TIMEOUT_S,
                    "loaded": self.vlm_loaded,
                    "error": self.vlm_error,
                    "last_inference_ms": self.last_vlm_ms,
                    "avg_inference_ms": avg_vlm,
                    "last_created_ms": self.last_vlm_created_ms,
                    "max_new_tokens": AI_VLM_MAX_NEW_TOKENS,
                    "family": self.vlm_family,
                    "device": preferred_ai_device(),
                    "benchmark": self.vlm_metrics.get(_model_metric_key("vlm", AI_VLM_MODEL)),
                },
                "vlm_benchmarks": dict(self.vlm_metrics),
                "runtime": ai_runtime_info(),
            }


vision_ai = VisionAI()


def ai_mjpeg_generator(device_id: str) -> Generator[bytes, None, None]:
    session = get_camera_session(device_id)
    session.running = True
    last_seen_count = -1
    last_overlay_ms = 0
    while session.running:
        with session.lock:
            frame = session.latest_jpeg
            count = session.frame_count
        t = now_ms()
        if frame is not None and count != last_seen_count and t - last_overlay_ms >= CAMERA_AI_OVERLAY_INTERVAL_MS:
            last_seen_count = count
            last_overlay_ms = t
            try:
                frame = vision_ai.draw_overlay(frame)
            except Exception as exc:
                print(f"[AI] overlay error: {exc}")
            yield b"--frame\r\nContent-Type: image/jpeg\r\nCache-Control: no-cache\r\n\r\n" + frame + b"\r\n"
        else:
            time.sleep(0.03)


def build_ai_scene_result(device_id: str, frame: bytes, question: str | None = None, force_detect: bool = False) -> dict[str, Any]:
    detections = vision_ai.detect(frame, force=force_detect)
    dets = detections.get("detections", []) if isinstance(detections, dict) else []
    detector_labels = {d.get("label", "").lower() for d in dets}
    vlm_question = question
    if question:
        det_summary = ", ".join([f"{d.get('label')} {d.get('confidence')}" for d in dets[:8]]) or "không có detection rõ từ detector realtime"
        vlm_question = f"Trả lời hoàn toàn bằng tiếng Việt, không dùng tiếng Anh. Detector realtime đang thấy: {det_summary}. Người dùng hỏi: {question}. Hãy nói rõ phía trước có gì, có vật cản không, và robot nên đi hướng nào hoặc dừng lại."
    scene = vision_ai.analyze_scene(frame, question=vlm_question, detector_labels=detector_labels)
    labels = set(detector_labels)
    labels |= {d.get("label", "").lower() for d in scene.get("detections", [])} if isinstance(scene, dict) else set()
    risky = bool(labels & {"person", "car", "truck", "bus", "motorcycle", "bicycle", "dog", "cat", "chair", "bench"})
    safe_action = scene.get("action") if isinstance(scene, dict) and scene.get("action") else ("slow_down" if risky else "go")
    if risky and safe_action == "go":
        safe_action = "slow_down"
    safety = {"risk_detected": risky, "safe_action": safe_action, "rule": "detector/VLM risky label override" if risky else "VLM robot action"}
    answer = _scene_answer_vi(scene if isinstance(scene, dict) else None, detections, safety)
    return {"device_id": device_id, "question": question, "answer": answer, "detections": detections, "scene": scene, "safety": safety, "created_ms": now_ms()}


def vlm_stream_loop(device_id: str) -> None:
    while True:
        with runtime_lock:
            state = vlm_streams.get(device_id)
            if not state or not state.get("running"):
                return
            instruction = state.get("instruction") or "Trả lời bằng tiếng Việt. Phía trước là gì? Hãy mô tả ngắn gọn và đưa ra lời khuyên an toàn cho robot."
            interval_ms = int(state.get("interval_ms") or 1500)
            state["state"] = "running"
        try:
            session = start_camera_session(device_id)
            frame = session.latest_frame()
            if frame is None:
                raise RuntimeError("no_camera_frame")
            result = build_ai_scene_result(device_id, frame, question=instruction, force_detect=False)
            with runtime_lock:
                state = vlm_streams.get(device_id)
                if state:
                    state["last_result"] = result
                    state["last_answer"] = result.get("answer")
                    state["last_safety"] = result.get("safety")
                    state["last_error"] = None
                    state["last_created_ms"] = now_ms()
                    state["run_count"] = int(state.get("run_count") or 0) + 1
        except Exception as exc:
            with runtime_lock:
                state = vlm_streams.get(device_id)
                if state:
                    state["last_error"] = str(exc)
                    state["last_created_ms"] = now_ms()
        time.sleep(max(0.5, interval_ms / 1000.0))


def vlm_stream_snapshot(device_id: str) -> dict[str, Any]:
    with runtime_lock:
        state = dict(vlm_streams.get(device_id) or {"running": False, "state": "stopped"})
    created = state.get("last_created_ms")
    state["last_age_ms"] = None if created is None else now_ms() - int(created)
    return state


def _extract_voice_duration_s(t: str) -> float | None:
    m = re.search(r"(\d+(?:[,.]\d+)?)\s*(?:s|sec|secs|second|seconds|giay|giây)", t)
    if not m:
        return None
    try:
        value = float(m.group(1).replace(",", "."))
    except Exception:
        return None
    return max(0.5, min(10.0, value))


def parse_voice_intent(text: str) -> dict[str, Any]:
    t = " ".join(str(text or "").lower().strip().split())
    if not t:
        return {"intent": "unknown", "action": None}
    if any(x in t for x in ["dừng", "dung", "stop", "đứng lại", "dừng lại", "thôi", "ngừng"]):
        return {"intent": "stop", "action": "stop", "duration_s": 0, "duration_limited": True}

    duration_s = _extract_voice_duration_s(t)
    duration_limited = duration_s is not None
    if duration_s is None:
        duration_s = 0

    if any(x in t for x in ["tiến", "tien", "đi thẳng", "di thang", "thẳng", "forward"]):
        return {"intent": "drive", "action": "forward", "duration_s": duration_s, "duration_limited": duration_limited}
    if any(x in t for x in ["lùi", "lui", "back", "backward"]):
        return {"intent": "drive", "action": "backward", "duration_s": duration_s, "duration_limited": duration_limited}
    if any(x in t for x in ["trái", "trai", "rẽ trái", "re trai", "left"]):
        return {"intent": "drive", "action": "left", "duration_s": duration_s, "duration_limited": duration_limited}
    if any(x in t for x in ["phải", "phai", "rẽ phải", "re phai", "right"]):
        return {"intent": "drive", "action": "right", "duration_s": duration_s, "duration_limited": duration_limited}
    if any(x in t for x in ["bật đèn", "bat den", "tắt đèn", "tat den"]):
        return {"intent": "unsupported", "action": "light", "message": "Firmware hiện tại chưa có endpoint điều khiển đèn."}
    return {"intent": "unknown", "action": None}


def stop_voice_latch(device_id: str, reason: str = "voice_stop") -> None:
    with runtime_lock:
        state = voice_latches.setdefault(device_id, {})
        state.update({
            "running": False,
            "desired_motion": "stop",
            "state": "stopped",
            "remaining_ms": 0,
            "remaining_s": 0,
            "end_ms": None,
            "last_error": None,
            "last_reason": reason,
            "updated_ms": now_ms(),
        })


def voice_latch_loop(device_id: str) -> None:
    while True:
        with runtime_lock:
            state = voice_latches.get(device_id)
            if not state or not state.get("running"):
                return
            desired = state.get("desired_motion")
            end_ms = int(state.get("end_ms") or 0)
            duration_limited = bool(state.get("duration_limited"))
            remaining_ms = max(0, end_ms - now_ms()) if end_ms else 0
            state["remaining_ms"] = remaining_ms
            state["remaining_s"] = round(remaining_ms / 1000.0, 1) if duration_limited else None
        if not desired or desired == "stop":
            return
        if duration_limited and remaining_ms <= 0:
            try:
                publish_mqtt_or_503(topic_for(device_id, "cmd/drive"), {"seq": command_seq(), "cmd": "stop", "ttl_ms": 300, "mode": "manual", "source": "voice_timed_done"}, qos=0, timeout_s=0.5)
            except Exception:
                pass
            stop_voice_latch(device_id, reason="duration_done")
            return
        try:
            assert_robot_command_allowed(device_id, "drive")
            vlm_state = vlm_stream_snapshot(device_id)
            safe_action = (vlm_state.get("last_safety") or {}).get("safe_action")
            if desired == "forward" and safe_action == "stop":
                publish_mqtt_or_503(topic_for(device_id, "cmd/drive"), {"seq": command_seq(), "cmd": "stop", "ttl_ms": 300, "mode": "manual", "source": "voice_safety"}, qos=0, timeout_s=0.5)
                stop_voice_latch(device_id, reason="safety_stop")
                return
            hardware_desired = {"left": "right", "right": "left"}.get(desired, desired)
            publish_mqtt_or_503(topic_for(device_id, "cmd/drive"), {"seq": command_seq(), "cmd": hardware_desired, "ttl_ms": 500, "mode": "manual", "source": "voice_timed"}, qos=0, timeout_s=0.5)
            with runtime_lock:
                state = voice_latches.get(device_id)
                if state:
                    state["state"] = "publishing"
                    state["last_publish_ms"] = now_ms()
                    end_ms = int(state.get("end_ms") or 0)
                    duration_limited = bool(state.get("duration_limited"))
                    remaining_ms = max(0, end_ms - now_ms()) if end_ms else 0
                    state["remaining_ms"] = remaining_ms
                    state["remaining_s"] = round(remaining_ms / 1000.0, 1) if duration_limited else None
                    state["last_error"] = None
        except Exception as exc:
            with runtime_lock:
                state = voice_latches.get(device_id)
                if state:
                    state["state"] = "error"
                    state["running"] = False
                    state["last_error"] = str(exc)
                    state["updated_ms"] = now_ms()
            try:
                publish_mqtt_or_503(topic_for(device_id, "cmd/drive"), {"seq": command_seq(), "cmd": "stop", "ttl_ms": 300, "mode": "manual", "source": "voice_error_stop"}, qos=0, timeout_s=0.5)
            except Exception:
                pass
            return
        time.sleep(0.15)


def voice_latch_snapshot(device_id: str) -> dict[str, Any]:
    with runtime_lock:
        state = dict(voice_latches.get(device_id) or {"running": False, "state": "stopped", "desired_motion": "stop", "remaining_ms": 0, "remaining_s": 0})
    updated = state.get("updated_ms") or state.get("last_publish_ms")
    state["age_ms"] = None if updated is None else now_ms() - int(updated)
    if state.get("running") and state.get("duration_limited") and state.get("end_ms"):
        remaining_ms = max(0, int(state["end_ms"]) - now_ms())
        state["remaining_ms"] = remaining_ms
        state["remaining_s"] = round(remaining_ms / 1000.0, 1)
    else:
        state["remaining_ms"] = int(state.get("remaining_ms") or 0)
        state["remaining_s"] = round(state["remaining_ms"] / 1000.0, 1) if state.get("duration_limited") else None
    return state


@app.on_event("startup")
def startup():
    threading.Thread(target=mqtt_loop, daemon=True).start()


@app.get("/")
def api_root():
    return {"ok": True, "service": "AgriVision Backend API", "version": APP_VERSION, "frontend": "Run the separate frontend/ app with npm run dev."}


@app.get("/api/health")
def health():
    refresh_robot_liveness()
    mqtt_ready = mqtt_is_ready()
    return {"ok": True, "version": APP_VERSION, "backend_status": "ready" if mqtt_ready else "degraded", "mqtt_connected": mqtt_ready, "mqtt_state": mqtt_state_snapshot(), "mqtt_connected_flag": mqtt_connected, "mqtt_client_connected": mqtt_client.is_connected(), "mqtt_host": MQTT_HOST, "mqtt_port": MQTT_PORT, "robots": len(robots), "sensor_ws": {"enabled": True, "endpoint": "/ws/agri/sensors/{device_id}", "transport": "websocket"}, "pump_control": {"transport": "mqtt", "topic": f"{BASE_TOPIC}/{{device_id}}/cmd/pump", "ack_topic": f"{BASE_TOPIC}/{{device_id}}/cmd_ack"}, "camera_push": {"enabled": True, "endpoints": ["/ws/camera/{device_id}", "/camera/ws/push/{device_id}", "/api/robots/{device_id}/camera/push"], "token_required": bool(CAMERA_PUSH_TOKEN)}, "camera_relay_ws_configured": bool(CAMERA_RELAY_WS), "camera_stale_ms": CAMERA_STALE_MS, "camera_offline_ms": CAMERA_OFFLINE_MS, "robot_offline_timeout_ms": ROBOT_OFFLINE_TIMEOUT_MS, "camera_sessions": {k: v.snapshot() for k, v in camera_sessions.items()}, "command_ack_timeout_s": COMMAND_ACK_TIMEOUT_S, "pending_command_acks": pending_command_ack_count(), "ai": {"yolo_enabled": AI_ENABLE_YOLO, "yolo_model": AI_YOLO_MODEL, "yolo_imgsz": AI_YOLO_IMGSZ, "vlm_enabled": AI_ENABLE_VLM, "vlm_model": AI_VLM_MODEL, "vlm_provider": AI_VLM_PROVIDER, "status": vision_ai.status()}}


@app.get("/api/robots")
def list_robots():
    refresh_robot_liveness()
    return {"robots": list(robots.values()), "mqtt_connected": mqtt_is_ready(), "mqtt_state": mqtt_state_snapshot()}


@app.get("/api/robots/{device_id}")
def get_robot(device_id: str):
    refresh_robot_liveness()
    if device_id not in robots:
        raise HTTPException(status_code=404, detail="robot_not_found")
    data = dict(robots[device_id])
    if device_id in camera_sessions:
        data["camera_session"] = camera_sessions[device_id].snapshot()
    return data


@app.get("/api/agri/nodes")
def list_agri_nodes():
    refresh_robot_liveness()
    nodes = []
    for node in robots.values():
        telemetry = node.get("telemetry") if isinstance(node.get("telemetry"), dict) else {}
        state = node.get("state") if isinstance(node.get("state"), dict) else {}
        node_type = telemetry.get("node_type") or state.get("node_type") or node.get("node_type") or "unknown"
        is_agri = (
            node_type in {"agri_control", "agri_camera"}
            or bool(node.get("camera_session"))
            or any(k in telemetry for k in ["soil_moisture_percent", "air_temperature_c", "air_humidity_percent", "water_level_percent", "pump"])
        )
        if is_agri:
            nodes.append(node)
    for device_id, session in camera_sessions.items():
        if device_id in robots:
            continue
        snap = session.snapshot()
        nodes.append({
            "device_id": device_id,
            "online": bool(snap.get("connected")),
            "node_type": "agri_camera",
            "state": {
                "schema": "agri.state.v1",
                "device_id": device_id,
                "node_type": "agri_camera",
                "online": bool(snap.get("connected")),
                "camera_status": snap.get("camera_status"),
                "transport": "websocket",
            },
            "telemetry": {},
            "events": [],
            "camera_session": snap,
            "last_seen_ms": snap.get("latest_frame_ms"),
            "last_seen_age_ms": snap.get("latest_frame_age_ms"),
            "last_message_kind": "camera_frame" if snap.get("latest_frame_ms") else "camera_session",
            "liveness": snap.get("camera_status") or "no_frame",
        })
    return {"nodes": nodes, "mqtt_connected": mqtt_is_ready(), "mqtt_state": mqtt_state_snapshot()}


@app.get("/api/agri/nodes/{device_id}")
def get_agri_node(device_id: str):
    refresh_robot_liveness()
    if device_id not in robots:
        if device_id in camera_sessions:
            snap = camera_sessions[device_id].snapshot()
            return {
                "device_id": device_id,
                "online": bool(snap.get("connected")),
                "node_type": "agri_camera",
                "state": {
                    "schema": "agri.state.v1",
                    "device_id": device_id,
                    "node_type": "agri_camera",
                    "online": bool(snap.get("connected")),
                    "camera_status": snap.get("camera_status"),
                    "transport": "websocket",
                },
                "telemetry": {},
                "events": [],
                "camera_session": snap,
                "last_seen_ms": snap.get("latest_frame_ms"),
                "last_seen_age_ms": snap.get("latest_frame_age_ms"),
                "last_message_kind": "camera_frame" if snap.get("latest_frame_ms") else "camera_session",
                "liveness": snap.get("camera_status") or "no_frame",
            }
        raise HTTPException(status_code=404, detail="agri_node_not_found")
    data = dict(robots[device_id])
    if device_id in camera_sessions:
        data["camera_session"] = camera_sessions[device_id].snapshot()
    return data


@app.websocket("/ws/agri/sensors/{device_id}")
async def agri_sensor_ws(websocket: WebSocket, device_id: str):
    await websocket.accept()
    upsert_robot(device_id, {
        "schema": "agri.state.v1",
        "device_id": device_id,
        "node_type": "agri_control",
        "online": True,
        "transport": "websocket",
        "ws_connected": True,
        "connected_ms": now_ms(),
    }, "state")
    print(f"[AGRI WS] sensor connected: {device_id}")
    try:
        while True:
            data = await websocket.receive_json()
            if not isinstance(data, dict):
                continue
            data.setdefault("device_id", device_id)
            data.setdefault("node_type", "agri_control")
            data["transport"] = "websocket"
            data["ws_connected"] = True
            kind = agri_message_kind(data)
            upsert_robot(device_id, data, kind)
    except WebSocketDisconnect:
        print(f"[AGRI WS] sensor disconnected: {device_id}")
    except Exception as exc:
        print(f"[AGRI WS] sensor error {device_id}: {exc}")
    finally:
        upsert_robot(device_id, {
            "schema": "agri.state.v1",
            "device_id": device_id,
            "node_type": "agri_control",
            "online": False,
            "transport": "websocket",
            "ws_connected": False,
            "disconnected_ms": now_ms(),
        }, "state")


@app.post("/api/agri/nodes/{device_id}/pump")
def control_agri_pump(device_id: str, req: AgriPumpCommand):
    payload = {
        "seq": req.seq,
        "cmd": req.cmd,
        "duration_ms": req.duration_ms,
        "reason": req.reason,
        "source": "backend",
    }
    return publish_command_and_wait_robot_ack(device_id, "pump", topic_for(device_id, "cmd/pump"), payload, qos=1)


@app.post("/api/robots/{device_id}/camera/start")
def start_camera(device_id: str):
    session = start_camera_session(device_id)
    return {"ok": True, "device_id": device_id, "camera_session": session.snapshot()}


@app.websocket("/ws/camera/{device_id}")
async def camera_push_spec_ws(websocket: WebSocket, device_id: str, token: str | None = Query(default=None)):
    await accept_camera_push(websocket, device_id, source="backend_push_ws", token=token)


@app.websocket("/camera/ws/push/{device_id}")
async def camera_push_relay_compatible_ws(websocket: WebSocket, device_id: str, token: str | None = Query(default=None)):
    await accept_camera_push(websocket, device_id, source="backend_push_ws", token=token)


@app.websocket("/api/robots/{device_id}/camera/push")
async def camera_push_legacy_ws(websocket: WebSocket, device_id: str, token: str | None = Query(default=None)):
    await accept_camera_push(websocket, device_id, source="backend_push_ws_legacy", token=token)


@app.post("/api/robots/{device_id}/camera/stop")
def stop_camera(device_id: str):
    session = get_camera_session(device_id)
    session.stop()
    return {"ok": True, "device_id": device_id, "camera_session": session.snapshot()}


@app.get("/api/robots/{device_id}/video.mjpg")
def video_mjpg(device_id: str):
    return StreamingResponse(mjpeg_generator(device_id), media_type="multipart/x-mixed-replace; boundary=frame", headers={"Cache-Control": "no-cache"})


@app.get("/api/robots/{device_id}/ai/video.mjpg")
def ai_video_mjpg(device_id: str):
    return StreamingResponse(ai_mjpeg_generator(device_id), media_type="multipart/x-mixed-replace; boundary=frame", headers={"Cache-Control": "no-cache"})


@app.get("/api/robots/{device_id}/ai/status")
def ai_status(device_id: str):
    session = get_camera_session(device_id)
    return {"device_id": device_id, "camera_session": session.snapshot(), "ai": vision_ai.status(), "presets": vision_ai.preset_config(), "created_ms": now_ms()}


@app.get("/api/ai/config")
def ai_config_get():
    return {"ai": vision_ai.status(), "presets": vision_ai.preset_config(), "created_ms": now_ms()}


@app.post("/api/ai/config")
def ai_config_set(update: AIConfigUpdate):
    status = vision_ai.configure(update)
    return {"ok": True, "ai": status, "presets": vision_ai.preset_config(), "created_ms": now_ms()}


@app.post("/api/ai/preload")
def ai_preload(load_detector: bool = True, load_vlm: bool = False):
    status = vision_ai.preload(load_detector=load_detector, load_vlm=load_vlm)
    return {"ok": True, "ai": status, "presets": vision_ai.preset_config(), "created_ms": now_ms()}


@app.get("/api/robots/{device_id}/ai/detect")
def ai_detect(device_id: str, force: bool = False):
    session = start_camera_session(device_id)
    frame = session.latest_frame()
    if frame is None:
        raise HTTPException(status_code=503, detail={"error": "no_camera_frame", "camera_session": session.snapshot()})
    return {"device_id": device_id, **plant_detection_from_leaf_analysis(frame)}


@app.get("/api/robots/{device_id}/ai/analyze")
def ai_analyze(device_id: str):
    session = start_camera_session(device_id)
    frame = session.latest_frame()
    if frame is None:
        raise HTTPException(status_code=503, detail={"error": "no_camera_frame", "camera_session": session.snapshot()})
    leaf_analysis = analyze_leaf_health(frame)
    detections = plant_detection_from_leaf_analysis(frame, leaf_analysis)
    detector_labels = {d.get("label", "").lower() for d in detections.get("detections", [])}
    scene = vision_ai.analyze_scene(frame, question=_leaf_chat_prompt(leaf_analysis), detector_labels=detector_labels)
    answer = _build_leaf_answer_vi(scene if isinstance(scene, dict) else None, leaf_analysis)
    safety = {"risk_detected": False, "safe_action": "inspect_leaf", "rule": "leaf_health_analysis"}
    return {"device_id": device_id, "answer": answer, "detections": detections, "scene": scene, "leaf_analysis": leaf_analysis, "safety": safety, "created_ms": now_ms()}


@app.post("/api/robots/{device_id}/ai/ask")
def ai_ask(device_id: str, req: AIAskRequest):
    session = start_camera_session(device_id)
    frame = session.latest_frame()
    if frame is None:
        raise HTTPException(status_code=503, detail={"error": "no_camera_frame", "camera_session": session.snapshot()})
    detections = vision_ai.detect(frame, force=False)
    dets = detections.get("detections", []) if isinstance(detections, dict) else []
    det_summary = ", ".join([f"{d.get('label')} {d.get('confidence')}" for d in dets[:8]]) or "không có detection rõ từ detector realtime"
    vlm_question = f"Trả lời hoàn toàn bằng tiếng Việt, không dùng tiếng Anh. Detector realtime đang thấy: {det_summary}. Người dùng hỏi: {req.question}"
    detector_labels = {d.get("label", "").lower() for d in detections.get("detections", [])}
    scene = vision_ai.analyze_scene(frame, question=vlm_question, detector_labels=detector_labels)
    labels = set(detector_labels)
    labels |= {d.get("label", "").lower() for d in scene.get("detections", [])} if isinstance(scene, dict) else set()
    risky = bool(labels & {"person", "car", "truck", "bus", "motorcycle", "bicycle", "dog", "cat", "chair", "bench"})
    safe_action = scene.get("action") if isinstance(scene, dict) and scene.get("action") else ("slow_down" if risky else "go")
    if risky and safe_action == "go":
        safe_action = "slow_down"
    safety = {"risk_detected": risky, "safe_action": safe_action, "rule": "detector/VLM risky label override" if risky else "VLM robot action"}
    answer = _scene_answer_vi(scene if isinstance(scene, dict) else None, detections, safety)
    return {"device_id": device_id, "question": req.question, "answer": answer, "detections": detections, "scene": scene, "safety": safety, "created_ms": now_ms()}


@app.post("/api/robots/{device_id}/ai/vlm-stream/start")
def start_vlm_stream(device_id: str, req: VLMStreamStartRequest):
    start_camera_session(device_id)
    with runtime_lock:
        state = vlm_streams.setdefault(device_id, {})
        already_running = bool(state.get("running"))
        state.update({
            "running": True,
            "state": "starting" if not already_running else "running",
            "instruction": req.instruction,
            "interval_ms": req.interval_ms,
            "started_ms": state.get("started_ms") or now_ms(),
            "last_error": None,
        })
    if not already_running:
        threading.Thread(target=vlm_stream_loop, args=(device_id,), daemon=True).start()
    return {"ok": True, "device_id": device_id, "vlm_stream": vlm_stream_snapshot(device_id)}


@app.post("/api/robots/{device_id}/ai/vlm-stream/stop")
def stop_vlm_stream(device_id: str):
    with runtime_lock:
        state = vlm_streams.setdefault(device_id, {})
        state.update({"running": False, "state": "stopped", "stopped_ms": now_ms()})
    return {"ok": True, "device_id": device_id, "vlm_stream": vlm_stream_snapshot(device_id)}


@app.get("/api/robots/{device_id}/ai/vlm-stream/status")
def get_vlm_stream(device_id: str):
    return {"device_id": device_id, "vlm_stream": vlm_stream_snapshot(device_id)}


@app.post("/api/robots/{device_id}/control/voice")
def voice_command(device_id: str, req: VoiceCommandRequest):
    intent = parse_voice_intent(req.text)
    if intent["intent"] in {"unknown", "unsupported", "duration_required"}:
        return {"ok": False, "device_id": device_id, "text": req.text, "intent": intent, "voice": voice_latch_snapshot(device_id)}
    if intent["action"] == "stop":
        stop_voice_latch(device_id, reason="voice_stop")
        payload = {"seq": command_seq(), "cmd": "stop", "ttl_ms": 300, "mode": "manual", "source": "voice"}
        result = publish_command_and_wait_robot_ack(device_id, "drive", topic_for(device_id, "cmd/drive"), payload, qos=0, ack_timeout_s=1.0)
        return {"ok": True, "device_id": device_id, "text": req.text, "intent": intent, "voice": voice_latch_snapshot(device_id), "command": result}

    desired = intent["action"]
    duration_s = float(intent.get("duration_s") or 0)
    duration_limited = bool(intent.get("duration_limited"))
    started_ms = now_ms()
    end_ms = started_ms + int(duration_s * 1000) if duration_limited else None
    assert_robot_command_allowed(device_id, "drive")
    with runtime_lock:
        state = voice_latches.setdefault(device_id, {})
        already_running = bool(state.get("running"))
        state.update({
            "running": True,
            "state": "starting" if not already_running else "publishing",
            "desired_motion": desired,
            "duration_s": duration_s,
            "duration_limited": duration_limited,
            "started_ms": started_ms,
            "end_ms": end_ms,
            "remaining_ms": max(0, end_ms - now_ms()) if end_ms else 0,
            "remaining_s": round(max(0, end_ms - now_ms()) / 1000.0, 1) if end_ms else None,
            "last_text": req.text,
            "updated_ms": now_ms(),
            "last_error": None,
        })
    if not already_running:
        threading.Thread(target=voice_latch_loop, args=(device_id,), daemon=True).start()
    return {"ok": True, "device_id": device_id, "text": req.text, "intent": intent, "voice": voice_latch_snapshot(device_id)}


@app.post("/api/robots/{device_id}/control/voice/stop")
def stop_voice_command(device_id: str):
    stop_voice_latch(device_id, reason="api_stop")
    payload = {"seq": command_seq(), "cmd": "stop", "ttl_ms": 300, "mode": "manual", "source": "voice_stop_api"}
    result = publish_command_and_wait_robot_ack(device_id, "drive", topic_for(device_id, "cmd/drive"), payload, qos=0, ack_timeout_s=1.0)
    return {"ok": True, "device_id": device_id, "voice": voice_latch_snapshot(device_id), "command": result}


@app.get("/api/robots/{device_id}/control/voice/status")
def get_voice_status(device_id: str):
    return {"device_id": device_id, "voice": voice_latch_snapshot(device_id)}


@app.post("/api/robots/{device_id}/control/drive")
def drive(device_id: str, command: DriveCommand):
    payload = command.model_dump(exclude_none=True)
    return publish_command_and_wait_robot_ack(device_id, "drive", topic_for(device_id, "cmd/drive"), payload, qos=0)


@app.post("/api/robots/{device_id}/control/servo")
def servo(device_id: str, command: ServoCommand):
    payload = command.model_dump()
    return publish_command_and_wait_robot_ack(device_id, "servo", topic_for(device_id, "cmd/servo"), payload, qos=0)


@app.post("/api/robots/{device_id}/control/stop")
def stop(device_id: str, command: StopCommand):
    payload = command.model_dump()
    return publish_command_and_wait_robot_ack(device_id, "stop", topic_for(device_id, "cmd/stop"), payload, qos=0)


@app.post("/api/robots/{device_id}/control/mode/{mode}")
def set_mode(device_id: str, mode: str):
    if mode not in {"idle", "manual", "ai", "estop"}:
        raise HTTPException(status_code=400, detail="invalid_mode")
    payload = {"seq": command_seq(), "mode": mode}
    return publish_command_and_wait_robot_ack(device_id, "mode", topic_for(device_id, "cmd/mode"), payload, qos=0)


@app.get("/api/robots/{device_id}/cmd_acks")
def get_robot_cmd_acks(device_id: str):
    return {"device_id": device_id, "cmd_acks": latest_cmd_acks.get(device_id, [])[-100:], "pending": pending_command_ack_snapshot(device_id)}


@app.get("/api/events")
def get_events():
    return {"events": events[-200:]}
