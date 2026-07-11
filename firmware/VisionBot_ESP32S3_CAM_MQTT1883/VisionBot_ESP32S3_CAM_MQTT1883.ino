/*
  VisionBot ESP32-S3-CAM Firmware v1.3 Portable LAN
  ------------------------------------------------------
  Features:
    - Config portal displays device_id
    - Hard-coded demo Wi-Fi for exam/demo reliability
    - MQTT control/status/telemetry
    - WebSocket camera stream on port 86
    - Dynamic IP announce via MQTT state
    - Motor watchdog / command TTL
    - State + telemetry + basic security events

  Hardware mapping for ESP32-S3 camera boards:
    Camera pin map below targets a common ESP32-S3 OV2640/Freenove-style module.
    If your exact ESP32-S3 board uses a different camera pinout, change only the
    CAMERA MODEL block below.

    L298N IN1 -> GPIO1
    L298N IN2 -> GPIO2
    L298N IN3 -> GPIO42
    L298N IN4 -> GPIO41
    Servo signal -> GPIO40

  Required Arduino libraries:
    - WiFiManager by tzapu
    - PubSubClient by Nick O'Leary
    - WebSockets by Markus Sattler
    - ArduinoJson by Benoit Blanchon
    - esp32-camera / ESP32 board package

  Demo note:
    This build targets a portable LAN demo. Set Windows Mobile Hotspot to:
      SSID: ThinhVip
      Password: Thinh123
    Windows hotspot normally gives the laptop IP 192.168.137.1.
    ESP uses plain MQTT port 1883. Start the broker first, then backend/frontend.
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClient.h>
#include <WebServer.h>
#include <WebSocketsServer.h>
#include <PubSubClient.h>
#include <WiFiManager.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#include <esp_camera.h>
#include <esp_system.h>
#include <time.h>

#if __has_include("esp_arduino_version.h")
#include "esp_arduino_version.h"
#endif

#ifndef ESP_ARDUINO_VERSION_MAJOR
#define ESP_ARDUINO_VERSION_MAJOR 2
#endif

// ================= FIRMWARE =================
#define FW_NAME    "visionbot-esp32s3cam"
#define FW_VERSION "1.3.2-esp32s3-mqtt1883-no-reset"

// ================= PORTABLE LAN DEMO DEFAULTS =================
// Set Windows Mobile Hotspot to these exact values for a no-portal demo.
// If the hotspot is unavailable, firmware falls back to the WiFiManager portal.
#define VISIONBOT_DEMO_WIFI_ENABLE 1
#define VISIONBOT_DEMO_WIFI_SSID "ThinhVip"
#define VISIONBOT_DEMO_WIFI_PASS "Thinh123"
#define VISIONBOT_DEMO_MQTT_HOST "192.168.137.1"
#define VISIONBOT_DEMO_MQTT_PORT 1883

// ================= CAMERA MODEL: ESP32-S3 OV2640 COMMON =================
// Tested pinouts differ by board vendor. This common S3-CAM/OV2640 map is a
// starting point; send the exact board model/photo if camera init fails.
#define PWDN_GPIO_NUM     -1
#define RESET_GPIO_NUM    -1
#define XCLK_GPIO_NUM     15
#define SIOD_GPIO_NUM      4
#define SIOC_GPIO_NUM      5

#define Y9_GPIO_NUM       16
#define Y8_GPIO_NUM       17
#define Y7_GPIO_NUM       18
#define Y6_GPIO_NUM       12
#define Y5_GPIO_NUM       10
#define Y4_GPIO_NUM        8
#define Y3_GPIO_NUM        9
#define Y2_GPIO_NUM       11
#define VSYNC_GPIO_NUM     6
#define HREF_GPIO_NUM      7
#define PCLK_GPIO_NUM     13

// ================= PIN MAPPING =================
#define IN1 1
#define IN2 2
#define IN3 42
#define IN4 41

#define SERVO_PIN 40

// ================= SERVO PWM =================
#define SERVO_CH   4
#define SERVO_FREQ 50
#define SERVO_RES  16

// ================= SERVICES =================
WebServer httpServer(80);
WebSocketsServer cameraWs = WebSocketsServer(86);
Preferences prefs;
WiFiClient mqttNetClient;
PubSubClient mqttClient(mqttNetClient);

// ================= DEVICE CONFIG =================
struct DeviceConfig {
  String robotId;
  String baseTopic;
  String mqttHost;
  uint16_t mqttPort;
  String mqttUser;
  String mqttPass;
  uint32_t telemetryIntervalMs;
};

DeviceConfig cfg;

// ================= DEVICE RUNTIME =================
String deviceId;
String setupApSsid;
String setupApPass;

bool cameraReady = false;
uint8_t cameraClientCount = 0;
unsigned long lastFrameMs = 0;
const unsigned long FRAME_INTERVAL_MS = 33;  // ~30 FPS target
const uint32_t CAMERA_XCLK_HZ = 15000000;    // XCLK 15 MHz: smoother in current OV3660 test
const framesize_t CAMERA_FRAME_SIZE_PSRAM = FRAMESIZE_QVGA;    // 320x240
const framesize_t CAMERA_FRAME_SIZE_NO_PSRAM = FRAMESIZE_QQVGA; // 160x120 fallback
const int CAMERA_JPEG_QUALITY_PSRAM = 22;    // Higher number = smaller/lower quality JPEG
const int CAMERA_JPEG_QUALITY_NO_PSRAM = 26;

bool mqttEverConnected = false;
unsigned long lastMqttAttemptMs = 0;
unsigned long mqttReconnectDelayMs = 1000;
const unsigned long MQTT_RECONNECT_MAX_MS = 8000;

uint8_t mqttConsecutiveFailures = 0;
const unsigned long MQTT_OFFLINE_LOG_EVERY_MS = 30000;
unsigned long lastMqttOfflineLogMs = 0;

unsigned long wifiDisconnectedSinceMs = 0;
const unsigned long WIFI_AUTO_PORTAL_AFTER_MS = 45000;

unsigned long lastTelemetryMs = 0;
unsigned long lastStateMs = 0;
const unsigned long STATE_INTERVAL_MS = 30000;

unsigned long bootMs = 0;
uint32_t securityEventCount = 0;
String lastSecurityEvent = "boot";

// ================= CONTROL STATE =================
enum RobotMode {
  MODE_IDLE,
  MODE_MANUAL,
  MODE_AI,
  MODE_ESTOP
};

RobotMode currentMode = MODE_IDLE;

unsigned long lastMotorCommandMs = 0;
unsigned long currentCommandTtlMs = 700;
bool motorActive = false;
String motorState = "stop";
int servoAngle = 90;
uint32_t lastCmdSeq = 0;
String lastCmdAckStatus = "none";
String lastCmdAckDetail = "none";

// ================= HELPERS =================
String jsonEscape(const String &s) {
  String out = "";
  out.reserve(s.length() + 8);
  for (size_t i = 0; i < s.length(); i++) {
    char c = s[i];
    if (c == '\"') out += "\\\"";
    else if (c == '\\') out += "\\\\";
    else if (c == '\n') out += "\\n";
    else if (c == '\r') out += "\\r";
    else if (c == '\t') out += "\\t";
    else out += c;
  }
  return out;
}

String q(const String &s) {
  return "\"" + jsonEscape(s) + "\"";
}

String boolStr(bool v) {
  return v ? "true" : "false";
}

String modeToString() {
  switch (currentMode) {
    case MODE_IDLE: return "idle";
    case MODE_MANUAL: return "manual";
    case MODE_AI: return "ai";
    case MODE_ESTOP: return "estop";
    default: return "unknown";
  }
}

void setModeFromString(const String &mode) {
  if (mode == "idle") currentMode = MODE_IDLE;
  else if (mode == "manual") currentMode = MODE_MANUAL;
  else if (mode == "ai") currentMode = MODE_AI;
  else if (mode == "estop") currentMode = MODE_ESTOP;
}

String resetReasonToString() {
  esp_reset_reason_t r = esp_reset_reason();
  switch (r) {
    case ESP_RST_POWERON: return "poweron";
    case ESP_RST_EXT: return "external";
    case ESP_RST_SW: return "software";
    case ESP_RST_PANIC: return "panic";
    case ESP_RST_INT_WDT: return "interrupt_watchdog";
    case ESP_RST_TASK_WDT: return "task_watchdog";
    case ESP_RST_WDT: return "other_watchdog";
    case ESP_RST_DEEPSLEEP: return "deepsleep";
    case ESP_RST_BROWNOUT: return "brownout";
    case ESP_RST_SDIO: return "sdio";
    default: return "unknown";
  }
}

String getIpString() {
  if (WiFi.status() == WL_CONNECTED) return WiFi.localIP().toString();
  return "0.0.0.0";
}

String getHttpBase() {
  return "http://" + getIpString();
}

String getStreamUrl() {
  return "ws://" + getIpString() + ":86/";
}

String mqttTopic(const String &suffix) {
  return cfg.baseTopic + "/" + deviceId + "/" + suffix;
}

long currentUnixTs() {
  time_t now;
  time(&now);
  if (now < 1700000000) return 0; // not synced
  return (long)now;
}

void generateDeviceId() {
  uint64_t mac = ESP.getEfuseMac();
  uint32_t last24 = (uint32_t)(mac & 0xFFFFFF);

  char buf[24];
  snprintf(buf, sizeof(buf), "VB-CAM-%06X", last24);
  deviceId = String(buf);

  char apBuf[32];
  snprintf(apBuf, sizeof(apBuf), "VisionBot-%06X", last24);
  setupApSsid = String(apBuf);
}

String generateRandomSetupPassword() {
  const char alphabet[] = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  String pass = "VB-";
  for (int i = 0; i < 10; i++) {
    uint32_t r = esp_random();
    pass += alphabet[r % (sizeof(alphabet) - 1)];
  }
  return pass;
}

void ensureSetupPassword() {
  prefs.begin("visionbot", false);
  setupApPass = prefs.getString("setup_pass", "");
  if (setupApPass.length() < 8) {
    setupApPass = generateRandomSetupPassword();
    prefs.putString("setup_pass", setupApPass);
  }
  prefs.end();
}

// ================= MOTOR =================
void motorStop() {
  digitalWrite(IN1, LOW);
  digitalWrite(IN2, LOW);
  digitalWrite(IN3, LOW);
  digitalWrite(IN4, LOW);

  motorActive = false;
  motorState = "stop";
}

void motorForward() {
  if (currentMode == MODE_ESTOP) {
    motorStop();
    return;
  }

  digitalWrite(IN1, HIGH);
  digitalWrite(IN2, LOW);
  digitalWrite(IN3, HIGH);
  digitalWrite(IN4, LOW);

  lastMotorCommandMs = millis();
  motorActive = true;
  motorState = "forward";
}

void motorBackward() {
  if (currentMode == MODE_ESTOP) {
    motorStop();
    return;
  }

  digitalWrite(IN1, LOW);
  digitalWrite(IN2, HIGH);
  digitalWrite(IN3, LOW);
  digitalWrite(IN4, HIGH);

  lastMotorCommandMs = millis();
  motorActive = true;
  motorState = "backward";
}

void motorLeft() {
  if (currentMode == MODE_ESTOP) {
    motorStop();
    return;
  }

  digitalWrite(IN1, LOW);
  digitalWrite(IN2, HIGH);
  digitalWrite(IN3, HIGH);
  digitalWrite(IN4, LOW);

  lastMotorCommandMs = millis();
  motorActive = true;
  motorState = "left";
}

void motorRight() {
  if (currentMode == MODE_ESTOP) {
    motorStop();
    return;
  }

  digitalWrite(IN1, HIGH);
  digitalWrite(IN2, LOW);
  digitalWrite(IN3, LOW);
  digitalWrite(IN4, HIGH);

  lastMotorCommandMs = millis();
  motorActive = true;
  motorState = "right";
}

void applyMotorCommand(const String &cmd, uint32_t seq = 0, unsigned long ttlMs = 700) {
  if (seq > 0) lastCmdSeq = seq;
  currentCommandTtlMs = constrain(ttlMs, 100UL, 3000UL);

  if (cmd == "forward") motorForward();
  else if (cmd == "backward") motorBackward();
  else if (cmd == "left") motorLeft();
  else if (cmd == "right") motorRight();
  else motorStop();
}

void applyDifferentialDrive(float left, float right, uint32_t seq, unsigned long ttlMs) {
  if (seq > 0) lastCmdSeq = seq;
  currentCommandTtlMs = constrain(ttlMs, 100UL, 3000UL);

  const float deadband = 0.08;

  if (fabs(left) < deadband && fabs(right) < deadband) {
    motorStop();
    return;
  }

  // This v1 firmware assumes L298N ENA/ENB jumpers are installed.
  // Therefore left/right speed is converted into direction only.
  // For true proportional speed, wire ENA/ENB to PWM-capable GPIO pins.
  if (left > deadband && right > deadband) {
    motorForward();
  } else if (left < -deadband && right < -deadband) {
    motorBackward();
  } else if (left < right) {
    motorLeft();
  } else if (left > right) {
    motorRight();
  } else {
    motorStop();
  }
}

// ================= SERVO =================
void setupServoPwm() {
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  ledcAttach(SERVO_PIN, SERVO_FREQ, SERVO_RES);
#else
  ledcSetup(SERVO_CH, SERVO_FREQ, SERVO_RES);
  ledcAttachPin(SERVO_PIN, SERVO_CH);
#endif
}

void servoWriteAngle(int angle) {
  angle = constrain(angle, 0, 180);
  servoAngle = angle;

  const int minUs = 500;
  const int maxUs = 2500;
  int pulseUs = map(angle, 0, 180, minUs, maxUs);

  uint32_t maxDuty = (1UL << SERVO_RES) - 1;
  uint32_t duty = (uint32_t)((pulseUs / 20000.0) * maxDuty);

#if ESP_ARDUINO_VERSION_MAJOR >= 3
  ledcWrite(SERVO_PIN, duty);
#else
  ledcWrite(SERVO_CH, duty);
#endif
}

// ================= CONFIG =================
void loadDeviceConfig() {
  prefs.begin("visionbot", true);

  cfg.robotId = prefs.getString("robot_id", deviceId);
  if (cfg.robotId.length() == 0) cfg.robotId = deviceId;

  cfg.baseTopic = prefs.getString("base_topic", "visionbot");
  cfg.mqttHost = prefs.getString("mqtt_host", "");
  cfg.mqttPort = prefs.getUShort("mqtt_port", 1883);
  cfg.mqttUser = prefs.getString("mqtt_user", "");
  cfg.mqttPass = prefs.getString("mqtt_pass", "");
  cfg.telemetryIntervalMs = prefs.getUInt("telemetry_ms", 5000);

  prefs.end();

#if VISIONBOT_DEMO_WIFI_ENABLE
  cfg.baseTopic = "visionbot";
  cfg.mqttHost = VISIONBOT_DEMO_MQTT_HOST;
  cfg.mqttPort = VISIONBOT_DEMO_MQTT_PORT;
  cfg.mqttUser = "";
  cfg.mqttPass = "";
#endif

  cfg.telemetryIntervalMs = constrain(cfg.telemetryIntervalMs, 1000UL, 60000UL);
}

void saveDeviceConfig(
  const char *robotId,
  const char *baseTopic,
  const char *mqttHost,
  const char *mqttPort,
  const char *mqttUser,
  const char *mqttPass,
  const char *telemetryMs
) {
  prefs.begin("visionbot", false);

  prefs.putString("robot_id", String(robotId));
  prefs.putString("base_topic", String(baseTopic));
  prefs.putString("mqtt_host", String(mqttHost));
  prefs.putUShort("mqtt_port", (uint16_t)atoi(mqttPort));
  prefs.putString("mqtt_user", String(mqttUser));
  prefs.putString("mqtt_pass", String(mqttPass));
  prefs.putUInt("telemetry_ms", (uint32_t)atol(telemetryMs));

  prefs.end();

  loadDeviceConfig();
}

void resetAllConfigAndRestart() {
  motorStop();

  prefs.begin("visionbot", false);
  prefs.clear();
  prefs.end();

  WiFi.disconnect(true, true);

  delay(500);
  ESP.restart();
}

String getRecoveryPortalReason() {
  prefs.begin("visionbot", true);
  String reason = prefs.getString("portal_reason", "");
  prefs.end();
  return reason;
}

void setRecoveryPortalReason(const String &reason) {
  prefs.begin("visionbot", false);
  prefs.putString("portal_reason", reason);
  prefs.end();
}

void clearRecoveryPortalReason() {
  prefs.begin("visionbot", false);
  prefs.remove("portal_reason");
  prefs.end();
}

bool runConfigPortal(bool forcePortal, const String &reason) {
  loadDeviceConfig();

  char robotIdBuf[40];
  char baseTopicBuf[32];
  char mqttHostBuf[80];
  char mqttPortBuf[8];
  char mqttUserBuf[80];
  char mqttPassBuf[80];
  char telemetryMsBuf[12];

  strlcpy(robotIdBuf, cfg.robotId.c_str(), sizeof(robotIdBuf));
  strlcpy(baseTopicBuf, cfg.baseTopic.c_str(), sizeof(baseTopicBuf));
  strlcpy(mqttHostBuf, cfg.mqttHost.c_str(), sizeof(mqttHostBuf));
  snprintf(mqttPortBuf, sizeof(mqttPortBuf), "%u", cfg.mqttPort);
  strlcpy(mqttUserBuf, cfg.mqttUser.c_str(), sizeof(mqttUserBuf));
  strlcpy(mqttPassBuf, cfg.mqttPass.c_str(), sizeof(mqttPassBuf));
  snprintf(telemetryMsBuf, sizeof(telemetryMsBuf), "%lu", (unsigned long)cfg.telemetryIntervalMs);

  String deviceHtml =
    "<div style='padding:10px;border:1px solid #ccc;border-radius:8px;margin:8px 0;'>"
    "<b>VisionBot ESP32-S3-CAM</b><br>"
    "Device ID: <b>" + deviceId + "</b><br>"
    "Setup AP: <b>" + setupApSsid + "</b><br>"
    "Firmware: <b>" + String(FW_VERSION) + "</b><br>"
    "Reason: <b>" + reason + "</b><br>"
    "Camera stream after Wi-Fi: <b>ws://&lt;device_ip&gt;:86/</b><br>"
    "MQTT: <b>plain MQTT port 1883 for broker-first LAN demo</b><br>"
    "Default hotspot mode uses MQTT host <b>192.168.137.1</b>."
    "</div>";

  WiFiManagerParameter pDevice(deviceHtml.c_str());
  WiFiManagerParameter pRobotId("robot_id", "Robot ID / friendly name", robotIdBuf, sizeof(robotIdBuf));
  WiFiManagerParameter pBaseTopic("base_topic", "MQTT base topic", baseTopicBuf, sizeof(baseTopicBuf));
  WiFiManagerParameter pMqttHost("mqtt_host", "MQTT broker host/IP", mqttHostBuf, sizeof(mqttHostBuf));
  WiFiManagerParameter pMqttPort("mqtt_port", "MQTT broker port", mqttPortBuf, sizeof(mqttPortBuf));
  WiFiManagerParameter pMqttUser("mqtt_user", "MQTT username", mqttUserBuf, sizeof(mqttUserBuf));
  WiFiManagerParameter pMqttPass("mqtt_pass", "MQTT password", mqttPassBuf, sizeof(mqttPassBuf), "type='password'");
  WiFiManagerParameter pTelemetryMs("telemetry_ms", "Telemetry interval ms", telemetryMsBuf, sizeof(telemetryMsBuf));

  WiFiManager wm;
  wm.setTitle("VisionBot Setup");
  wm.setClass("invert");
  wm.setConfigPortalTimeout(300);
  wm.setConnectTimeout(30);
  wm.setDebugOutput(true);

  wm.addParameter(&pDevice);
  wm.addParameter(&pRobotId);
  wm.addParameter(&pBaseTopic);
  wm.addParameter(&pMqttHost);
  wm.addParameter(&pMqttPort);
  wm.addParameter(&pMqttUser);
  wm.addParameter(&pMqttPass);
  wm.addParameter(&pTelemetryMs);

  Serial.println();
  Serial.println("=== Wi-Fi / MQTT setup portal ===");
  Serial.print("Reason: ");
  Serial.println(reason);
  Serial.print("Mode: ");
  Serial.println(forcePortal ? "FORCED CONFIG PORTAL" : "AUTO CONNECT / PORTAL IF WI-FI FAILS");
  Serial.print("Device ID: ");
  Serial.println(deviceId);
  Serial.print("Setup AP SSID: ");
  Serial.println(setupApSsid);
  Serial.print("Setup AP password: ");
  Serial.println(setupApPass);
  Serial.print("Current MQTT host: ");
  Serial.println(cfg.mqttHost);
  Serial.println("If saved Wi-Fi fails, config portal opens automatically.");
  Serial.println("MQTT failures will NOT force portal/restart; robot keeps camera/HTTP alive and retries.");

  bool connected;
  if (forcePortal) {
    motorStop();
    mqttClient.disconnect();
    connected = wm.startConfigPortal(setupApSsid.c_str(), setupApPass.c_str());
  } else {
    connected = wm.autoConnect(setupApSsid.c_str(), setupApPass.c_str());
  }

  if (!connected) {
    Serial.println("WiFiManager failed or timed out. Restarting...");
    delay(1000);
    ESP.restart();
    return false;
  }

  saveDeviceConfig(
    pRobotId.getValue(),
    pBaseTopic.getValue(),
    pMqttHost.getValue(),
    pMqttPort.getValue(),
    pMqttUser.getValue(),
    pMqttPass.getValue(),
    pTelemetryMs.getValue()
  );

  if (forcePortal) {
    clearRecoveryPortalReason();
  }

  WiFi.setSleep(false);
  Serial.println("Wi-Fi connected.");
  Serial.println("Wi-Fi sleep disabled for lower camera-stream latency.");
  Serial.print("IP: ");
  Serial.println(WiFi.localIP());

  if (forcePortal) {
    Serial.println("Config portal finished. Restarting to reload Wi-Fi/MQTT cleanly...");
    delay(1000);
    ESP.restart();
  }

  return true;
}

bool connectDemoHotspot() {
#if VISIONBOT_DEMO_WIFI_ENABLE
  Serial.println();
  Serial.println("=== Hardcoded laptop hotspot Wi-Fi ===");
  Serial.print("Trying SSID: ");
  Serial.println(VISIONBOT_DEMO_WIFI_SSID);
  Serial.print("MQTT broker default: ");
  Serial.print(VISIONBOT_DEMO_MQTT_HOST);
  Serial.print(":");
  Serial.println(VISIONBOT_DEMO_MQTT_PORT);

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.begin(VISIONBOT_DEMO_WIFI_SSID, VISIONBOT_DEMO_WIFI_PASS);

  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < 45000) {
    Serial.print(".");
    delay(500);
  }
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    loadDeviceConfig();
    Serial.println("Hardcoded hotspot connected.");
    Serial.println("Wi-Fi sleep disabled for lower camera-stream latency.");
    Serial.print("IP: ");
    Serial.println(WiFi.localIP());
    return true;
  }

  Serial.println("Hardcoded hotspot not found. Falling back to setup portal.");
#endif
  return false;
}

bool startConfigPortalIfNeeded() {
  String recoveryReason = getRecoveryPortalReason();
  if (recoveryReason.length() > 0) {
    if (recoveryReason.indexOf("mqtt_connect_failed") >= 0) {
      Serial.println("Ignoring old MQTT recovery portal reason; MQTT now retries without forcing portal.");
      clearRecoveryPortalReason();
      recoveryReason = "";
    } else {
      return runConfigPortal(true, recoveryReason);
    }
  }
  if (connectDemoHotspot()) {
    return true;
  }
  return runConfigPortal(false, "boot");
}

void openConfigPortalForRecovery(const String &reason) {
  Serial.println();
  Serial.println("!!! Scheduling recovery config portal !!!");
  Serial.println("Reason: " + reason);
  Serial.println("Restarting into clean setup mode so WiFiManager can own port 80.");
  setRecoveryPortalReason(reason);
  motorStop();
  mqttClient.disconnect();
  delay(1000);
  ESP.restart();
}

void resetMqttFailureTracker() {
  mqttConsecutiveFailures = 0;
}

void recordMqttFailureAndKeepRunning() {
  mqttConsecutiveFailures++;

  unsigned long now = millis();
  if (mqttConsecutiveFailures <= 3 || now - lastMqttOfflineLogMs >= MQTT_OFFLINE_LOG_EVERY_MS) {
    lastMqttOfflineLogMs = now;
    Serial.print("MQTT offline, retrying in background. Consecutive failures: ");
    Serial.println(mqttConsecutiveFailures);
    Serial.println("Robot stays online. Camera/HTTP still work; update MQTT host if this Wi-Fi is not laptop hotspot 192.168.137.x.");
  }
}

void maintainWifiRecoveryPortal() {
  if (WiFi.status() == WL_CONNECTED) {
    wifiDisconnectedSinceMs = 0;
    return;
  }

  if (motorActive) {
    motorStop();
  }

  unsigned long now = millis();
  if (wifiDisconnectedSinceMs == 0) {
    wifiDisconnectedSinceMs = now;
    Serial.println("Wi-Fi disconnected. Waiting before recovery portal...");
    return;
  }

  if (now - wifiDisconnectedSinceMs >= WIFI_AUTO_PORTAL_AFTER_MS) {
    openConfigPortalForRecovery("wifi_disconnected");
  }
}

// ================= CAMERA =================
bool initCamera() {
  camera_config_t config;

  config.ledc_channel = LEDC_CHANNEL_0;
  config.ledc_timer   = LEDC_TIMER_0;

  config.pin_d0       = Y2_GPIO_NUM;
  config.pin_d1       = Y3_GPIO_NUM;
  config.pin_d2       = Y4_GPIO_NUM;
  config.pin_d3       = Y5_GPIO_NUM;
  config.pin_d4       = Y6_GPIO_NUM;
  config.pin_d5       = Y7_GPIO_NUM;
  config.pin_d6       = Y8_GPIO_NUM;
  config.pin_d7       = Y9_GPIO_NUM;

  config.pin_xclk     = XCLK_GPIO_NUM;
  config.pin_pclk     = PCLK_GPIO_NUM;
  config.pin_vsync    = VSYNC_GPIO_NUM;
  config.pin_href     = HREF_GPIO_NUM;

#if ESP_ARDUINO_VERSION_MAJOR >= 3
  config.pin_sccb_sda = SIOD_GPIO_NUM;
  config.pin_sccb_scl = SIOC_GPIO_NUM;
#else
  config.pin_sscb_sda = SIOD_GPIO_NUM;
  config.pin_sscb_scl = SIOC_GPIO_NUM;
#endif

  config.pin_pwdn     = PWDN_GPIO_NUM;
  config.pin_reset    = RESET_GPIO_NUM;

  config.xclk_freq_hz = CAMERA_XCLK_HZ;
  config.pixel_format = PIXFORMAT_JPEG;

  if (psramFound()) {
    config.frame_size   = CAMERA_FRAME_SIZE_PSRAM;
    config.jpeg_quality = CAMERA_JPEG_QUALITY_PSRAM;
    config.fb_count     = 2;
  } else {
    config.frame_size   = CAMERA_FRAME_SIZE_NO_PSRAM;
    config.jpeg_quality = CAMERA_JPEG_QUALITY_NO_PSRAM;
    config.fb_count     = 1;
  }

  esp_err_t err = esp_camera_init(&config);

  if (err != ESP_OK) {
    Serial.printf("Camera init failed: 0x%x\n", err);
    return false;
  }

  sensor_t *s = esp_camera_sensor_get();
  if (s) {
    framesize_t frameSize = psramFound() ? CAMERA_FRAME_SIZE_PSRAM : CAMERA_FRAME_SIZE_NO_PSRAM;
    int jpegQuality = psramFound() ? CAMERA_JPEG_QUALITY_PSRAM : CAMERA_JPEG_QUALITY_NO_PSRAM;

    if (s->set_framesize) s->set_framesize(s, frameSize);
    if (s->set_quality) s->set_quality(s, jpegQuality);

    // Fixed low-latency camera profile. Keep it simple: no runtime camera tuning yet.
    if (s->set_brightness) s->set_brightness(s, 0);
    if (s->set_contrast) s->set_contrast(s, 0);
    if (s->set_saturation) s->set_saturation(s, -2);
    if (s->set_whitebal) s->set_whitebal(s, 1);
    if (s->set_exposure_ctrl) s->set_exposure_ctrl(s, 1);
    if (s->set_gain_ctrl) s->set_gain_ctrl(s, 1);
    if (s->set_raw_gma) s->set_raw_gma(s, 1);
    if (s->set_lenc) s->set_lenc(s, 1);
    if (s->set_bpc) s->set_bpc(s, 1);
    if (s->set_wpc) s->set_wpc(s, 1);
    if (s->set_hmirror) s->set_hmirror(s, 1);
    if (s->set_vflip) s->set_vflip(s, 1);
    if (s->set_colorbar) s->set_colorbar(s, 0);
  }

  Serial.println("Camera init OK.");
  Serial.printf("Camera profile: XCLK=%lu Hz, frame_interval=%lu ms, frame_size=%s, jpeg_quality=%d\n",
                (unsigned long)CAMERA_XCLK_HZ,
                FRAME_INTERVAL_MS,
                psramFound() ? "QVGA" : "QQVGA",
                psramFound() ? CAMERA_JPEG_QUALITY_PSRAM : CAMERA_JPEG_QUALITY_NO_PSRAM);
  return true;
}

void cameraWsEvent(uint8_t num, WStype_t type, uint8_t *payload, size_t length) {
  switch (type) {
    case WStype_CONNECTED:
      cameraClientCount++;
      Serial.printf("[CAM WS] client %u connected. clients=%u\n", num, cameraClientCount);
      lastSecurityEvent = "camera_ws_connected";
      break;

    case WStype_DISCONNECTED:
      if (cameraClientCount > 0) cameraClientCount--;
      Serial.printf("[CAM WS] client %u disconnected. clients=%u\n", num, cameraClientCount);
      lastSecurityEvent = "camera_ws_disconnected";
      break;

    default:
      break;
  }
}

void streamCameraFrame() {
  if (!cameraReady) return;
  if (cameraClientCount == 0) return;

  unsigned long now = millis();
  if (now - lastFrameMs < FRAME_INTERVAL_MS) return;
  lastFrameMs = now;

  camera_fb_t *fb = esp_camera_fb_get();
  if (!fb) {
    Serial.println("Camera capture failed");
    return;
  }

  if (fb->format == PIXFORMAT_JPEG) {
    cameraWs.broadcastBIN(fb->buf, fb->len);
  }

  esp_camera_fb_return(fb);
}

// ================= JSON PAYLOADS =================
String makeStatePayload(bool online) {
  String p = "{";
  p += "\"schema\":\"visionbot.state.v1\",";
  p += "\"device_id\":" + q(deviceId) + ",";
  p += "\"robot_id\":" + q(cfg.robotId) + ",";
  p += "\"type\":\"esp32s3cam_robot\",";
  p += "\"online\":" + boolStr(online) + ",";
  p += "\"ts\":" + String(currentUnixTs()) + ",";
  p += "\"uptime_ms\":" + String(millis()) + ",";
  p += "\"firmware\":" + q(String(FW_NAME) + "-" + FW_VERSION) + ",";
  p += "\"ip\":" + q(getIpString()) + ",";
  p += "\"http_base\":" + q(getHttpBase()) + ",";
  p += "\"stream_url\":" + q(getStreamUrl()) + ",";
  p += "\"wifi_ssid\":" + q(WiFi.SSID()) + ",";
  p += "\"wifi_rssi_dbm\":" + String(WiFi.RSSI()) + ",";
  p += "\"mqtt_connected\":" + boolStr(mqttClient.connected()) + ",";
  p += "\"camera_ready\":" + boolStr(cameraReady) + ",";
  p += "\"camera_clients\":" + String(cameraClientCount) + ",";
  p += "\"camera_xclk_hz\":" + String(CAMERA_XCLK_HZ) + ",";
  p += "\"camera_frame_interval_ms\":" + String(FRAME_INTERVAL_MS) + ",";
  p += "\"camera_frame_size\":" + q(psramFound() ? String("QVGA") : String("QQVGA")) + ",";
  p += "\"camera_jpeg_quality\":" + String(psramFound() ? CAMERA_JPEG_QUALITY_PSRAM : CAMERA_JPEG_QUALITY_NO_PSRAM) + ",";
  p += "\"mode\":" + q(modeToString()) + ",";
  p += "\"motor_state\":" + q(motorState) + ",";
  p += "\"servo_angle\":" + String(servoAngle) + ",";
  p += "\"last_cmd_seq\":" + String(lastCmdSeq) + ",";
  p += "\"last_cmd_ack_status\":" + q(lastCmdAckStatus) + ",";
  p += "\"last_cmd_ack_detail\":" + q(lastCmdAckDetail) + ",";
  p += "\"free_heap_bytes\":" + String(ESP.getFreeHeap()) + ",";
  p += "\"min_free_heap_bytes\":" + String(ESP.getMinFreeHeap()) + ",";
  p += "\"free_psram_bytes\":" + String(ESP.getFreePsram()) + ",";
  p += "\"reset_reason\":" + q(resetReasonToString()) + ",";
  p += "\"mqtt_plain\":" + boolStr(true) + ",";
  p += "\"last_security_event\":" + q(lastSecurityEvent);
  p += "}";
  return p;
}

String makeTelemetryPayload() {
  unsigned long lastCmdAge = motorActive ? (millis() - lastMotorCommandMs) : 0;

  String p = "{";
  p += "\"schema\":\"visionbot.telemetry.v1\",";
  p += "\"device_id\":" + q(deviceId) + ",";
  p += "\"robot_id\":" + q(cfg.robotId) + ",";
  p += "\"ts\":" + String(currentUnixTs()) + ",";
  p += "\"uptime_ms\":" + String(millis()) + ",";
  p += "\"network\":{";
  p += "\"wifi_connected\":" + boolStr(WiFi.status() == WL_CONNECTED) + ",";
  p += "\"ip\":" + q(getIpString()) + ",";
  p += "\"rssi_dbm\":" + String(WiFi.RSSI()) + ",";
  p += "\"mqtt_connected\":" + boolStr(mqttClient.connected());
  p += "},";
  p += "\"camera\":{";
  p += "\"ready\":" + boolStr(cameraReady) + ",";
  p += "\"clients\":" + String(cameraClientCount) + ",";
  p += "\"stream_url\":" + q(getStreamUrl()) + ",";
  p += "\"xclk_hz\":" + String(CAMERA_XCLK_HZ) + ",";
  p += "\"frame_size\":" + q(psramFound() ? String("QVGA") : String("QQVGA")) + ",";
  p += "\"jpeg_quality\":" + String(psramFound() ? CAMERA_JPEG_QUALITY_PSRAM : CAMERA_JPEG_QUALITY_NO_PSRAM) + ",";
  p += "\"frame_interval_ms\":" + String(FRAME_INTERVAL_MS);
  p += "},";
  p += "\"control\":{";
  p += "\"mode\":" + q(modeToString()) + ",";
  p += "\"motor_state\":" + q(motorState) + ",";
  p += "\"motor_active\":" + boolStr(motorActive) + ",";
  p += "\"last_cmd_seq\":" + String(lastCmdSeq) + ",";
  p += "\"last_cmd_ack_status\":" + q(lastCmdAckStatus) + ",";
  p += "\"last_cmd_ack_detail\":" + q(lastCmdAckDetail) + ",";
  p += "\"last_cmd_age_ms\":" + String(lastCmdAge) + ",";
  p += "\"cmd_ttl_ms\":" + String(currentCommandTtlMs) + ",";
  p += "\"servo_angle\":" + String(servoAngle);
  p += "},";
  p += "\"system\":{";
  p += "\"free_heap_bytes\":" + String(ESP.getFreeHeap()) + ",";
  p += "\"min_free_heap_bytes\":" + String(ESP.getMinFreeHeap()) + ",";
  p += "\"free_psram_bytes\":" + String(ESP.getFreePsram()) + ",";
  p += "\"reset_reason\":" + q(resetReasonToString());
  p += "},";
  p += "\"security\":{";
  p += "\"mqtt_plain\":" + boolStr(true) + ",";
  p += "\"event_count\":" + String(securityEventCount) + ",";
  p += "\"last_event\":" + q(lastSecurityEvent);
  p += "}";
  p += "}";
  return p;
}

// ================= MQTT =================
void publishSecurityEvent(const String &eventType, const String &detail, const String &severity = "info") {
  lastSecurityEvent = eventType;
  securityEventCount++;

  if (!mqttClient.connected()) return;

  String p = "{";
  p += "\"schema\":\"visionbot.event.v1\",";
  p += "\"device_id\":" + q(deviceId) + ",";
  p += "\"robot_id\":" + q(cfg.robotId) + ",";
  p += "\"ts\":" + String(currentUnixTs()) + ",";
  p += "\"uptime_ms\":" + String(millis()) + ",";
  p += "\"severity\":" + q(severity) + ",";
  p += "\"type\":" + q(eventType) + ",";
  p += "\"detail\":" + q(detail);
  p += "}";

  mqttClient.publish(mqttTopic("event").c_str(), p.c_str(), false);
}

void publishState(bool retain = true) {
  if (!mqttClient.connected()) return;
  String p = makeStatePayload(true);
  mqttClient.publish(mqttTopic("state").c_str(), p.c_str(), retain);
}

void publishTelemetry() {
  if (!mqttClient.connected()) return;
  String p = makeTelemetryPayload();
  mqttClient.publish(mqttTopic("telemetry").c_str(), p.c_str(), false);
}

void publishCmdAck(const String &command, uint32_t seq, const String &status, const String &detail) {
  lastCmdAckStatus = status;
  lastCmdAckDetail = detail;

  if (!mqttClient.connected()) return;

  String p = "{";
  p += "\"schema\":\"visionbot.cmd_ack.v1\",";
  p += "\"device_id\":" + q(deviceId) + ",";
  p += "\"robot_id\":" + q(cfg.robotId) + ",";
  p += "\"ts\":" + String(currentUnixTs()) + ",";
  p += "\"uptime_ms\":" + String(millis()) + ",";
  p += "\"seq\":" + String(seq) + ",";
  p += "\"command\":" + q(command) + ",";
  p += "\"status\":" + q(status) + ",";
  p += "\"detail\":" + q(detail) + ",";
  p += "\"mode\":" + q(modeToString()) + ",";
  p += "\"motor_state\":" + q(motorState) + ",";
  p += "\"motor_active\":" + boolStr(motorActive) + ",";
  p += "\"servo_angle\":" + String(servoAngle) + ",";
  p += "\"last_cmd_seq\":" + String(lastCmdSeq) + ",";
  p += "\"firmware\":" + q(String(FW_NAME) + "-" + FW_VERSION);
  p += "}";

  bool ok = mqttClient.publish(mqttTopic("cmd_ack").c_str(), p.c_str(), false);
  Serial.println("[MQTT ACK] " + command + " seq=" + String(seq) + " status=" + status + " publish=" + String(ok ? "ok" : "failed"));
}

void handleMqttDriveCommand(const String &payload) {
  StaticJsonDocument<512> doc;
  DeserializationError err = deserializeJson(doc, payload);

  if (err) {
    publishCmdAck("drive", 0, "error", "invalid_json");
    publishSecurityEvent("invalid_drive_payload", payload, "warn");
    return;
  }

  uint32_t seq = doc["seq"] | 0;
  unsigned long ttlMs = doc["ttl_ms"] | 300;

  if (currentMode == MODE_ESTOP) {
    motorStop();
    publishCmdAck("drive", seq, "rejected", "mode_estop");
    publishSecurityEvent("drive_rejected_estop", "drive command rejected because mode=estop", "warn");
    publishTelemetry();
    return;
  }

  if (doc["mode"].is<const char*>()) {
    String requestedMode = String((const char*)doc["mode"]);
    if (requestedMode == "manual" || requestedMode == "ai") {
      currentMode = requestedMode == "manual" ? MODE_MANUAL : MODE_AI;
    }
  }

  String detail = "";

  if (doc["cmd"].is<const char*>()) {
    String cmd = String((const char*)doc["cmd"]);
    if (!(cmd == "forward" || cmd == "backward" || cmd == "left" || cmd == "right" || cmd == "stop")) {
      motorStop();
      if (seq > 0) lastCmdSeq = seq;
      publishCmdAck("drive", seq, "rejected", "invalid_drive_cmd=" + cmd);
      publishSecurityEvent("drive_rejected_invalid_cmd", "cmd=" + cmd, "warn");
      publishTelemetry();
      return;
    }
    applyMotorCommand(cmd, seq, ttlMs);
    detail = "cmd=" + cmd + ",state=" + motorState;
  } else {
    float left = doc["left"] | 0.0;
    float right = doc["right"] | 0.0;
    applyDifferentialDrive(left, right, seq, ttlMs);
    detail = "left=" + String(left, 2) + ",right=" + String(right, 2) + ",state=" + motorState;
  }

  publishCmdAck("drive", seq, "executed", detail);
  publishSecurityEvent("cmd_drive", "executed seq=" + String(seq) + " state=" + motorState);
  publishTelemetry();
}

void handleMqttServoCommand(const String &payload) {
  StaticJsonDocument<256> doc;
  DeserializationError err = deserializeJson(doc, payload);

  if (err) {
    publishCmdAck("servo", 0, "error", "invalid_json");
    publishSecurityEvent("invalid_servo_payload", payload, "warn");
    return;
  }

  uint32_t seq = doc["seq"] | 0;
  int angle = doc["angle"] | servoAngle;

  if (angle < 0 || angle > 180) {
    if (seq > 0) lastCmdSeq = seq;
    publishCmdAck("servo", seq, "rejected", "angle_out_of_range=" + String(angle));
    publishSecurityEvent("servo_rejected_angle", "angle=" + String(angle), "warn");
    publishTelemetry();
    return;
  }

  if (seq > 0) lastCmdSeq = seq;

  servoWriteAngle(angle);

  publishCmdAck("servo", seq, "executed", "angle=" + String(servoAngle));
  publishSecurityEvent("cmd_servo", "servo angle=" + String(servoAngle));
  publishTelemetry();
}

void handleMqttStopCommand(const String &payload) {
  motorStop();
  currentMode = MODE_ESTOP;

  StaticJsonDocument<256> doc;
  DeserializationError err = deserializeJson(doc, payload);

  String reason = "stop_command";
  uint32_t seq = 0;

  if (!err) {
    reason = String((const char*)(doc["reason"] | "stop_command"));
    seq = doc["seq"] | 0;
  } else {
    reason = "invalid_json_but_estop_applied";
  }

  if (seq > 0) lastCmdSeq = seq;

  publishCmdAck("stop", seq, "executed", reason);
  publishSecurityEvent("cmd_stop", reason, "warn");
  publishTelemetry();
  publishState(true);
}

void handleMqttModeCommand(const String &payload) {
  StaticJsonDocument<256> doc;
  DeserializationError err = deserializeJson(doc, payload);

  if (err) {
    publishCmdAck("mode", 0, "error", "invalid_json");
    publishSecurityEvent("invalid_mode_payload", payload, "warn");
    return;
  }

  uint32_t seq = doc["seq"] | 0;
  String mode = String((const char*)(doc["mode"] | "idle"));

  if (!(mode == "idle" || mode == "manual" || mode == "ai" || mode == "estop")) {
    if (seq > 0) lastCmdSeq = seq;
    publishCmdAck("mode", seq, "rejected", "invalid_mode=" + mode);
    publishSecurityEvent("mode_rejected_invalid", "mode=" + mode, "warn");
    publishTelemetry();
    return;
  }

  if (seq > 0) lastCmdSeq = seq;

  if (mode == "idle") {
    motorStop();
    currentMode = MODE_IDLE;
  } else if (mode == "manual") {
    motorStop();
    currentMode = MODE_MANUAL;
  } else if (mode == "ai") {
    motorStop();
    currentMode = MODE_AI;
  } else if (mode == "estop") {
    motorStop();
    currentMode = MODE_ESTOP;
  }

  publishCmdAck("mode", seq, "executed", "mode=" + modeToString());
  publishSecurityEvent("cmd_mode", "mode=" + modeToString());
  publishTelemetry();
  publishState(true);
}

void mqttCallback(char *topicChars, byte *payloadBytes, unsigned int length) {
  String topic = String(topicChars);
  String payload;
  payload.reserve(length + 1);

  for (unsigned int i = 0; i < length; i++) {
    payload += (char)payloadBytes[i];
  }

  String driveTopic = mqttTopic("cmd/drive");
  String servoTopic = mqttTopic("cmd/servo");
  String stopTopic = mqttTopic("cmd/stop");
  String modeTopic = mqttTopic("cmd/mode");
  String resetTopic = mqttTopic("cmd/config/reset");

  if (topic == driveTopic) {
    handleMqttDriveCommand(payload);
  } else if (topic == servoTopic) {
    handleMqttServoCommand(payload);
  } else if (topic == stopTopic) {
    handleMqttStopCommand(payload);
  } else if (topic == modeTopic) {
    handleMqttModeCommand(payload);
  } else if (topic == resetTopic) {
    publishSecurityEvent("config_reset_requested", "MQTT reset command received", "warn");
    delay(500);
    resetAllConfigAndRestart();
  }
}

void setupMqttClient() {
  Serial.println("MQTT mode: plain TCP port 1883 for broker-first LAN demo.");
}

void mqttSubscribeCommands() {
  mqttClient.subscribe(mqttTopic("cmd/drive").c_str(), 1);
  mqttClient.subscribe(mqttTopic("cmd/servo").c_str(), 1);
  mqttClient.subscribe(mqttTopic("cmd/stop").c_str(), 1);
  mqttClient.subscribe(mqttTopic("cmd/mode").c_str(), 1);
  mqttClient.subscribe(mqttTopic("cmd/config/reset").c_str(), 1);
}


bool probeMqttTcpPort() {
  WiFiClient probeClient;
  probeClient.setTimeout(5000);

  Serial.print("MQTT TCP probe ");
  Serial.print(cfg.mqttHost);
  Serial.print(":");
  Serial.print(cfg.mqttPort);
  Serial.print(" ... ");

  bool ok = probeClient.connect(cfg.mqttHost.c_str(), cfg.mqttPort);
  if (ok) {
    Serial.println("OK (broker port reachable from ESP32-S3-CAM)");
    probeClient.stop();
    delay(250);
    return true;
  }

  Serial.println("FAILED (ESP32-S3-CAM cannot reach broker TCP port)");
  probeClient.stop();
  return false;
}

bool connectMqtt() {
  if (cfg.mqttHost.length() == 0) {
    Serial.println("MQTT broker is not configured.");
    return false;
  }

  if (!probeMqttTcpPort()) {
    return false;
  }

  mqttClient.setServer(cfg.mqttHost.c_str(), cfg.mqttPort);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setBufferSize(2048);
  mqttClient.setKeepAlive(30);
  mqttClient.setSocketTimeout(12);

  String clientId = deviceId;
  String stateTopic = mqttTopic("state");
  String willPayload = makeStatePayload(false);

  Serial.print("Connecting MQTT ");
  Serial.print(cfg.mqttHost);
  Serial.print(":");
  Serial.print(cfg.mqttPort);
  Serial.print(" as ");
  Serial.println(clientId);

  bool ok;

  if (cfg.mqttUser.length() > 0) {
    ok = mqttClient.connect(
      clientId.c_str(),
      cfg.mqttUser.c_str(),
      cfg.mqttPass.c_str(),
      stateTopic.c_str(),
      1,
      true,
      willPayload.c_str()
    );
  } else {
    ok = mqttClient.connect(
      clientId.c_str(),
      nullptr,
      nullptr,
      stateTopic.c_str(),
      1,
      true,
      willPayload.c_str()
    );
  }

  if (!ok) {
    Serial.print("MQTT connect failed. state=");
    Serial.println(mqttClient.state());
    return false;
  }

  mqttEverConnected = true;
  mqttReconnectDelayMs = 1000;
  resetMqttFailureTracker();

  mqttSubscribeCommands();

  publishState(true);
  publishTelemetry();
  publishSecurityEvent("mqtt_connected", "connected and commands subscribed");

  Serial.println("MQTT connected.");
  Serial.println("Subscribed topics:");
  Serial.println("  " + mqttTopic("cmd/drive"));
  Serial.println("  " + mqttTopic("cmd/servo"));
  Serial.println("  " + mqttTopic("cmd/stop"));
  Serial.println("  " + mqttTopic("cmd/mode"));
  Serial.println("  " + mqttTopic("cmd/config/reset"));

  return true;
}

void maintainMqtt() {
  if (WiFi.status() != WL_CONNECTED) {
    if (motorActive) {
      motorStop();
    }
    return;
  }

  if (mqttClient.connected()) {
    mqttClient.loop();
    return;
  }

  if (mqttEverConnected) {
    motorStop();
  }

  unsigned long now = millis();
  if (now - lastMqttAttemptMs < mqttReconnectDelayMs) return;

  lastMqttAttemptMs = now;

  bool ok = connectMqtt();

  if (!ok) {
    recordMqttFailureAndKeepRunning();
    mqttReconnectDelayMs = min(mqttReconnectDelayMs * 2, MQTT_RECONNECT_MAX_MS);
  }
}

// ================= HTTP SERVER =================
String buildRootPayload() {
  String p = "{";
  p += "\"name\":\"VisionBot ESP32-S3-CAM\",";
  p += "\"device_id\":" + q(deviceId) + ",";
  p += "\"robot_id\":" + q(cfg.robotId) + ",";
  p += "\"firmware\":" + q(String(FW_NAME) + "-" + FW_VERSION) + ",";
  p += "\"runtime\":\"headless\",";
  p += "\"message\":\"This device exposes JSON APIs and WebSocket camera stream only. Main UI must run on backend/frontend.\",";
  p += "\"health\":\"/api/health\",";
  p += "\"status\":\"/api/status\",";
  p += "\"state\":\"/api/state\",";
  p += "\"stream_url\":" + q(getStreamUrl()) + ",";
  p += "\"mqtt_state_topic\":" + q(mqttTopic("state"));
  p += "}";
  return p;
}

void handleRoot() {
  httpServer.send(200, "application/json", buildRootPayload());
}

void handleHealth() {
  String p = "{";
  p += "\"ok\":true,";
  p += "\"device_id\":" + q(deviceId) + ",";
  p += "\"uptime_ms\":" + String(millis()) + ",";
  p += "\"wifi_connected\":" + boolStr(WiFi.status() == WL_CONNECTED) + ",";
  p += "\"mqtt_connected\":" + boolStr(mqttClient.connected()) + ",";
  p += "\"camera_ready\":" + boolStr(cameraReady);
  p += "}";
  httpServer.send(200, "application/json", p);
}

void handleStatus() {
  httpServer.send(200, "application/json", makeTelemetryPayload());
}

void handleState() {
  httpServer.send(200, "application/json", makeStatePayload(true));
}

void handleStop() {
  motorStop();
  currentMode = MODE_ESTOP;
  publishSecurityEvent("http_stop", "local HTTP stop endpoint called", "warn");
  publishState(true);
  httpServer.send(200, "application/json", "{\"ok\":true,\"mode\":\"estop\",\"motor_state\":\"stop\"}");
}

void handleConfigReset() {
  String token = httpServer.arg("token");

  if (token != setupApPass) {
    publishSecurityEvent("config_reset_denied", "invalid local reset token", "warn");
    httpServer.send(403, "application/json", "{\"ok\":false,\"error\":\"invalid_token\"}");
    return;
  }

  publishSecurityEvent("config_reset_http", "local HTTP config reset", "warn");
  httpServer.send(200, "application/json", "{\"ok\":true,\"message\":\"resetting\"}");
  delay(500);
  resetAllConfigAndRestart();
}

void handleDebugMotor() {
  String cmd = httpServer.arg("cmd");
  applyMotorCommand(cmd, 0, 700);
  publishSecurityEvent("http_debug_motor", "cmd=" + cmd, "warn");
  httpServer.send(200, "application/json", "{\"ok\":true,\"cmd\":" + q(cmd) + ",\"motor_state\":" + q(motorState) + "}");
}

void handleDebugServo() {
  int angle = httpServer.arg("angle").toInt();
  servoWriteAngle(angle);
  publishSecurityEvent("http_debug_servo", "angle=" + String(servoAngle), "warn");
  httpServer.send(200, "application/json", "{\"ok\":true,\"servo_angle\":" + String(servoAngle) + "}");
}

void setupHttpServer() {
  httpServer.on("/", HTTP_GET, handleRoot);
  httpServer.on("/api/health", HTTP_GET, handleHealth);
  httpServer.on("/api/status", HTTP_GET, handleStatus);
  httpServer.on("/api/state", HTTP_GET, handleState);

  // Emergency stop is intentionally kept as a local HTTP endpoint.
  // Normal control should use MQTT through the backend.
  httpServer.on("/api/stop", HTTP_POST, handleStop);

  // Reset requires token shown in Serial Monitor as setup AP password.
  httpServer.on("/api/config/reset", HTTP_POST, handleConfigReset);

  httpServer.onNotFound([]() {
    httpServer.send(404, "application/json", "{\"ok\":false,\"error\":\"not_found\"}");
  });

  httpServer.begin();
  Serial.println("Headless HTTP API started on port 80.");
}

// ================= SERIAL FALLBACK =================
void handleSerialCommand(char c) {
  if (c == 'f') {
    currentMode = MODE_MANUAL;
    applyMotorCommand("forward");
  } else if (c == 'b') {
    currentMode = MODE_MANUAL;
    applyMotorCommand("backward");
  } else if (c == 'l') {
    currentMode = MODE_MANUAL;
    applyMotorCommand("left");
  } else if (c == 'r') {
    currentMode = MODE_MANUAL;
    applyMotorCommand("right");
  } else if (c == 's') {
    motorStop();
    currentMode = MODE_IDLE;
  } else if (c == 'e') {
    motorStop();
    currentMode = MODE_ESTOP;
  } else if (c == '0') {
    servoWriteAngle(0);
  } else if (c == '1') {
    servoWriteAngle(90);
  } else if (c == '2') {
    servoWriteAngle(180);
  } else if (c == 'x') {
    Serial.println("Serial config reset requested.");
    resetAllConfigAndRestart();
  } else if (c == '?') {
    Serial.println(makeTelemetryPayload());
  }
}

// ================= SETUP / LOOP =================
void setupPins() {
  pinMode(IN1, OUTPUT);
  pinMode(IN2, OUTPUT);
  pinMode(IN3, OUTPUT);
  pinMode(IN4, OUTPUT);
  motorStop();

  setupServoPwm();
  servoWriteAngle(90);
}

void printBootInfo() {
  Serial.println();
  Serial.println("======================================");
  Serial.println(" VisionBot ESP32-S3-CAM Firmware v1.3.2 MQTT 1883 No Reset");
  Serial.println("======================================");
  Serial.print("Device ID: ");
  Serial.println(deviceId);
  Serial.print("Robot ID: ");
  Serial.println(cfg.robotId);
  Serial.print("Firmware: ");
  Serial.print(FW_NAME);
  Serial.print("-");
  Serial.println(FW_VERSION);
  Serial.print("Reset reason: ");
  Serial.println(resetReasonToString());
  Serial.println();
}

void setup() {
  bootMs = millis();

  Serial.begin(115200);
  delay(1000);

  generateDeviceId();
  ensureSetupPassword();
  loadDeviceConfig();
  printBootInfo();

  setupPins();

  bool wifiOk = startConfigPortalIfNeeded();
  if (!wifiOk) return;

  cameraReady = initCamera();

  cameraWs.begin();
  cameraWs.onEvent(cameraWsEvent);
  Serial.println("Camera WebSocket started on port 86.");
  Serial.println("Camera stream URL: " + getStreamUrl());

  setupHttpServer();

  setupMqttClient();
  if (!connectMqtt()) {
    recordMqttFailureAndKeepRunning();
  }

  publishSecurityEvent("boot", "device booted");
  publishState(true);

  Serial.println();
  Serial.println("=== Runtime ready ===");
  Serial.println("Device ID: " + deviceId);
  Serial.println("HTTP API: " + getHttpBase());
  Serial.println("Stream: " + getStreamUrl());
  Serial.println("MQTT state: " + mqttTopic("state"));
  Serial.println("MQTT drive: " + mqttTopic("cmd/drive"));
  Serial.println("MQTT servo: " + mqttTopic("cmd/servo"));
  Serial.println("MQTT stop: " + mqttTopic("cmd/stop"));
  Serial.println();
  Serial.println("Serial fallback:");
  Serial.println("f/b/l/r/s = motor, e = estop, 0/1/2 = servo, ? = status, x = reset config");
}

void loop() {
  httpServer.handleClient();

  cameraWs.loop();
  streamCameraFrame();

  maintainWifiRecoveryPortal();
  maintainMqtt();

  if (Serial.available()) {
    char c = Serial.read();
    handleSerialCommand(c);
  }

  // Motor command TTL / watchdog
  if (motorActive && millis() - lastMotorCommandMs > currentCommandTtlMs) {
    motorStop();
    publishSecurityEvent("motor_watchdog_stop", "command TTL expired", "warn");
    publishTelemetry();
  }

  // Telemetry
  if (millis() - lastTelemetryMs > cfg.telemetryIntervalMs) {
    lastTelemetryMs = millis();
    publishTelemetry();
  }

  // Retained dynamic state / IP announce
  if (millis() - lastStateMs > STATE_INTERVAL_MS) {
    lastStateMs = millis();
    publishState(true);
  }
}
