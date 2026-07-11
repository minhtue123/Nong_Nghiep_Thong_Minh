/*
  Agri ESP32-CAM Push Only
  ------------------------
  ESP32-CAM chi lam nhiem vu stream JPEG ve backend FastAPI.

  Backend endpoint:
    ws://<BACKEND_HOST>:8000/ws/camera/agri-camera-01

  Thu vien can co:
    - WebSockets by Markus Sattler
    - esp32-camera / ESP32 board package
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WebSocketsClient.h>
#include <esp_camera.h>
#include <esp_heap_caps.h>

// ===== SUA 3 DONG NAY TRUOC KHI NAP =====
const char *WIFI_SSID = "Ladaiday";
const char *WIFI_PASSWORD = "ladaiday";
const char *BACKEND_HOST = "172.20.10.5";  // IP laptop chay backend, khong dung 127.0.0.1

// ===== CAU HINH BACKEND =====
const uint16_t BACKEND_PORT = 8000;
const char *DEVICE_ID = "AGRICAM-FE8CE0";
const char *WS_BASE_PATH = "/ws/camera";
const bool USE_TLS = false;

// ===== CAMERA MODEL: AI THINKER ESP32-CAM =====
#define PWDN_GPIO_NUM     32
#define RESET_GPIO_NUM    -1
#define XCLK_GPIO_NUM      0
#define SIOD_GPIO_NUM     26
#define SIOC_GPIO_NUM     27
#define Y9_GPIO_NUM       35
#define Y8_GPIO_NUM       34
#define Y7_GPIO_NUM       39
#define Y6_GPIO_NUM       36
#define Y5_GPIO_NUM       21
#define Y4_GPIO_NUM       19
#define Y3_GPIO_NUM       18
#define Y2_GPIO_NUM        5
#define VSYNC_GPIO_NUM    25
#define HREF_GPIO_NUM     23
#define PCLK_GPIO_NUM     22

WebSocketsClient cameraWs;

bool wsConnected = false;
bool wsStarted = false;
bool cameraReady = false;
unsigned long lastFrameMs = 0;
unsigned long lastWifiAttemptMs = 0;
unsigned long frameCount = 0;

const unsigned long FRAME_INTERVAL_MS = 80; // ~12 FPS that hon: QVGA muot hon tren Wi-Fi ESP32-CAM

camera_config_t makeCameraConfig(framesize_t frameSize, int quality, int fbCount, camera_fb_location_t fbLocation) {
  camera_config_t config = {};
  config.ledc_channel = LEDC_CHANNEL_0;
  config.ledc_timer = LEDC_TIMER_0;
  config.pin_d0 = Y2_GPIO_NUM;
  config.pin_d1 = Y3_GPIO_NUM;
  config.pin_d2 = Y4_GPIO_NUM;
  config.pin_d3 = Y5_GPIO_NUM;
  config.pin_d4 = Y6_GPIO_NUM;
  config.pin_d5 = Y7_GPIO_NUM;
  config.pin_d6 = Y8_GPIO_NUM;
  config.pin_d7 = Y9_GPIO_NUM;
  config.pin_xclk = XCLK_GPIO_NUM;
  config.pin_pclk = PCLK_GPIO_NUM;
  config.pin_vsync = VSYNC_GPIO_NUM;
  config.pin_href = HREF_GPIO_NUM;
  config.pin_sscb_sda = SIOD_GPIO_NUM;
  config.pin_sscb_scl = SIOC_GPIO_NUM;
  config.pin_pwdn = PWDN_GPIO_NUM;
  config.pin_reset = RESET_GPIO_NUM;
  config.xclk_freq_hz = 20000000;
  config.pixel_format = PIXFORMAT_JPEG;
  config.frame_size = frameSize;
  config.jpeg_quality = quality;
  config.fb_count = fbCount;
  config.grab_mode = fbCount > 1 ? CAMERA_GRAB_LATEST : CAMERA_GRAB_WHEN_EMPTY;
  config.fb_location = fbLocation;
  return config;
}

bool tryCameraProfile(const char *name, framesize_t frameSize, int quality, int fbCount, camera_fb_location_t fbLocation) {
  Serial.printf("Trying camera profile: %s, quality=%d, fb_count=%d, fb_location=%s\n",
                name,
                quality,
                fbCount,
                fbLocation == CAMERA_FB_IN_PSRAM ? "PSRAM" : "DRAM");
  camera_config_t config = makeCameraConfig(frameSize, quality, fbCount, fbLocation);

  esp_err_t err = esp_camera_init(&config);
  if (err != ESP_OK) {
    Serial.printf("Camera profile failed: %s err=0x%x\n", name, err);
    esp_camera_deinit();
    return false;
  }

  sensor_t *s = esp_camera_sensor_get();
  if (s) {
    s->set_framesize(s, frameSize);
    s->set_quality(s, quality);
    s->set_brightness(s, -2);
    s->set_contrast(s, -2);
    s->set_saturation(s, -1);
    s->set_whitebal(s, 1);
    s->set_awb_gain(s, 1);
    s->set_exposure_ctrl(s, 0);
    s->set_aec2(s, 0);
    s->set_aec_value(s, 55);
    s->set_gain_ctrl(s, 0);
    s->set_agc_gain(s, 0);
    s->set_gainceiling(s, (gainceiling_t)0);
    s->set_bpc(s, 1);
    s->set_wpc(s, 1);
    s->set_lenc(s, 1);
    s->set_hmirror(s, 1);
    s->set_vflip(s, 1);
  }

  Serial.printf("Camera init OK: %s\n", name);
  return true;
}

bool initCamera() {
  Serial.printf("Free heap: %u bytes\n", ESP.getFreeHeap());
  Serial.printf("Free PSRAM: %u bytes\n", ESP.getFreePsram());
  Serial.printf("psramFound: %s\n", psramFound() ? "yes" : "no");

  if (psramFound()) {
    if (tryCameraProfile("QVGA PSRAM smooth-low-glare", FRAMESIZE_QVGA, 34, 2, CAMERA_FB_IN_PSRAM)) return true;
    if (tryCameraProfile("QVGA PSRAM stable-low-glare", FRAMESIZE_QVGA, 38, 1, CAMERA_FB_IN_PSRAM)) return true;
    if (tryCameraProfile("QQVGA PSRAM fallback-smooth", FRAMESIZE_QQVGA, 36, 1, CAMERA_FB_IN_PSRAM)) return true;
  }

  if (tryCameraProfile("QVGA DRAM no-psram", FRAMESIZE_QVGA, 38, 1, CAMERA_FB_IN_DRAM)) return true;
  if (tryCameraProfile("QQVGA PSRAM forced-smooth", FRAMESIZE_QQVGA, 40, 1, CAMERA_FB_IN_PSRAM)) return true;

  Serial.println("Camera init failed on every profile.");
  Serial.println("Check Tools -> PSRAM: Enabled, use stable 5V power, and reseat the OV2640 ribbon cable.");
  return false;
}

void connectWifi() {
  if (WiFi.status() == WL_CONNECTED) return;
  unsigned long now = millis();
  if (now - lastWifiAttemptMs < 5000) return;
  lastWifiAttemptMs = now;

  Serial.printf("Connecting Wi-Fi SSID=%s\n", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
}

void cameraWsEvent(WStype_t type, uint8_t *payload, size_t length) {
  switch (type) {
    case WStype_CONNECTED:
      wsConnected = true;
      Serial.println("Camera WebSocket connected to backend");
      break;
    case WStype_DISCONNECTED:
      wsConnected = false;
      Serial.println("Camera WebSocket disconnected");
      break;
    case WStype_ERROR:
      wsConnected = false;
      Serial.println("Camera WebSocket error");
      break;
    default:
      break;
  }
}

void setupCameraWs() {
  if (wsStarted) return;
  wsStarted = true;
  String path = String(WS_BASE_PATH) + "/" + DEVICE_ID;
  Serial.print("Camera push URL: ");
  Serial.print(USE_TLS ? "wss://" : "ws://");
  Serial.print(BACKEND_HOST);
  Serial.print(":");
  Serial.print(BACKEND_PORT);
  Serial.println(path);

  cameraWs.onEvent(cameraWsEvent);
  cameraWs.setReconnectInterval(3000);
  cameraWs.enableHeartbeat(15000, 3000, 2);
  if (USE_TLS) {
    cameraWs.beginSSL(BACKEND_HOST, BACKEND_PORT, path.c_str());
  } else {
    cameraWs.begin(BACKEND_HOST, BACKEND_PORT, path.c_str());
  }
}

void pushFrame() {
  if (!cameraReady || !wsConnected) return;
  unsigned long now = millis();
  if (now - lastFrameMs < FRAME_INTERVAL_MS) return;
  lastFrameMs = now;

  camera_fb_t *fb = esp_camera_fb_get();
  if (!fb) {
    Serial.println("Camera capture failed");
    return;
  }

  if (fb->format == PIXFORMAT_JPEG && fb->len > 0) {
    cameraWs.sendBIN(fb->buf, fb->len);
    frameCount++;
    if (frameCount % 50 == 0) {
      Serial.printf("Pushed frames=%lu size=%u bytes\n", frameCount, (unsigned)fb->len);
    }
  }
  esp_camera_fb_return(fb);
}

void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println();
  Serial.println("Agri ESP32-CAM Push Only");
  Serial.printf("Device ID: %s\n", DEVICE_ID);

  cameraReady = initCamera();
  connectWifi();

  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < 20000) {
    delay(300);
    Serial.print(".");
  }
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    Serial.print("Wi-Fi connected. ESP32-CAM IP: ");
    Serial.println(WiFi.localIP());
    setupCameraWs();
  } else {
    Serial.println("Wi-Fi not connected yet. Will retry in loop.");
  }
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    wsConnected = false;
    connectWifi();
    delay(50);
    return;
  }

  if (!wsStarted) {
    setupCameraWs();
  }

  cameraWs.loop();
  pushFrame();
}
