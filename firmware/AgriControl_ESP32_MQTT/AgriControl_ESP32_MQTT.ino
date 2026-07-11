#include <WiFi.h>
#include <WebServer.h>
#include <Preferences.h>
#include <PubSubClient.h>
#include <DHT.h>

// =====================================================
// 1. CAU HINH MAC DINH
// =====================================================

const char* DEFAULT_WIFI_SSID = "Ladaiday";
const char* DEFAULT_WIFI_PASSWORD = "ladaiday";

const char* DEFAULT_MQTT_HOST = "172.20.10.5";
const uint16_t DEFAULT_MQTT_PORT = 1883;

// Wi-Fi ESP32 tu phat khi khong ket noi duoc
const char* CONFIG_AP_SSID = "AgriControl-Setup";
const char* CONFIG_AP_PASSWORD = "12345678";

// =====================================================
// 2. KHAI BAO CHAN
// =====================================================

#define RELAY_PIN         26
#define DHT_PIN           27
#define WATER_SENSOR_PIN  34
#define SOIL_SENSOR_PIN   35

#define DHT_TYPE DHT22

// Relay cua bo hien tai: ON = HIGH, OFF = LOW.
// Neu relay cua ban hoat dong nguoc, doi lai thanh:
// #define RELAY_ON  LOW
// #define RELAY_OFF HIGH
#define RELAY_ON  HIGH
#define RELAY_OFF LOW

// =====================================================
// 3. MQTT TOPIC
// =====================================================

const char* DEVICE_ID =
  "agri-control-01";

const char* NODE_TYPE =
  "agri_control";

const char* TOPIC_SENSOR =
  "smartfarm/esp32/sensors";

const char* TOPIC_PUMP_COMMAND =
  "smartfarm/esp32/pump/set";

const char* TOPIC_PUMP_STATE =
  "smartfarm/esp32/pump/state";

const char* TOPIC_STATUS =
  "smartfarm/esp32/status";

const char* TOPIC_ONLINE =
  "smartfarm/esp32/online";

const char* TOPIC_AGRI_PUMP_COMMAND =
  "agri/agri-control-01/cmd/pump";

const char* TOPIC_AGRI_PUMP_ACK =
  "agri/agri-control-01/cmd_ack";

const char* TOPIC_AGRI_STATE =
  "agri/agri-control-01/state";

const char* TOPIC_AGRI_TELEMETRY =
  "agri/agri-control-01/telemetry";

// =====================================================
// 4. HIEU CHINH CAM BIEN
// =====================================================

// Gia tri RAW khi dat kho
int soilDryValue = 3200;

// Gia tri RAW khi dat uot
int soilWetValue = 1200;

// Do am <= 30% thi bat bom
int soilStartPump = 30;

// Do am >= 45% thi tat bom
int soilStopPump = 45;

// Water RAW >= gia tri nay thi coi la co nuoc
int waterMinimumValue = 300;

// Bao ve bom khi het nuoc
const bool USE_WATER_SAFETY = true;

// Trong luc test, neu cam bien nuoc chua dung,
// co the doi thanh:
// const bool USE_WATER_SAFETY = false;

// =====================================================
// 5. THOI GIAN
// =====================================================

const unsigned long SENSOR_INTERVAL = 2000;

const unsigned long WIFI_RECONNECT_INTERVAL = 10000;

const unsigned long MQTT_RECONNECT_INTERVAL = 5000;

// Sau 60 giay khong co Wi-Fi hoac MQTT,
// ESP32 tu phat Wi-Fi cau hinh
const unsigned long CONFIG_PORTAL_DELAY = 60000;

// Bom chay toi da 120 giay moi lan
const unsigned long MAX_PUMP_RUNTIME = 120000;

// Sau khi tat, bom nghi 10 giay
const unsigned long PUMP_COOLDOWN = 10000;

// =====================================================
// 6. DOI TUONG
// =====================================================

DHT dht(DHT_PIN, DHT_TYPE);

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);

WebServer configServer(80);
Preferences preferences;

// =====================================================
// 7. CAU HINH DUOC LUU TRONG ESP32
// =====================================================

String wifiSsid;
String wifiPassword;
String mqttHost;
uint16_t mqttPort;

// =====================================================
// 8. DU LIEU CAM BIEN
// =====================================================

float temperature = NAN;
float airHumidity = NAN;

int soilRaw = 0;
int soilPercent = 0;

int waterRaw = 0;
bool waterAvailable = false;

// =====================================================
// 9. TRANG THAI BOM
// =====================================================

bool pumpState = false;
bool autoMode = true;

unsigned long manualPumpOffAt = 0;
unsigned long lastCommandSeq = 0;
String lastCommandAckStatus = "none";
String lastCommandAckDetail = "";

// =====================================================
// 10. BIEN THOI GIAN
// =====================================================

unsigned long lastSensorTime = 0;
unsigned long lastWiFiReconnectTime = 0;
unsigned long lastMQTTReconnectTime = 0;

unsigned long pumpStartedTime = 0;
unsigned long pumpStoppedTime = 0;

unsigned long networkOfflineSince = 0;

bool configPortalActive = false;

void publishAgriState();
void publishAgriTelemetry();
void publishAgriCommandAck(
  unsigned long seq,
  const String& status,
  const String& detail
);

// =====================================================
// 11. DOC CAU HINH TU NVS
// =====================================================

void loadConfiguration() {
  preferences.begin("agricontrol", true);

  wifiSsid = preferences.getString(
    "ssid",
    DEFAULT_WIFI_SSID
  );

  wifiPassword = preferences.getString(
    "password",
    DEFAULT_WIFI_PASSWORD
  );

  mqttHost = preferences.getString(
    "mqtt_host",
    DEFAULT_MQTT_HOST
  );

  mqttPort = preferences.getUShort(
    "mqtt_port",
    DEFAULT_MQTT_PORT
  );

  preferences.end();

  Serial.println("Da doc cau hinh:");
  Serial.print("WiFi SSID: ");
  Serial.println(wifiSsid);

  Serial.print("MQTT Host: ");
  Serial.println(mqttHost);

  Serial.print("MQTT Port: ");
  Serial.println(mqttPort);
}

// =====================================================
// 12. LUU CAU HINH
// =====================================================

void saveConfiguration() {
  preferences.begin("agricontrol", false);

  preferences.putString("ssid", wifiSsid);
  preferences.putString("password", wifiPassword);
  preferences.putString("mqtt_host", mqttHost);
  preferences.putUShort("mqtt_port", mqttPort);

  preferences.end();

  Serial.println("Da luu cau hinh vao ESP32.");
}

// =====================================================
// 13. DOC ADC TRUNG BINH
// =====================================================

int readAnalogAverage(
  int pin,
  int numberOfSamples = 10
) {
  long total = 0;

  for (int i = 0; i < numberOfSamples; i++) {
    total += analogRead(pin);
    delay(2);
  }

  return total / numberOfSamples;
}

// =====================================================
// 14. DOC CAM BIEN
// =====================================================

void readSensors() {
  float newTemperature = dht.readTemperature();
  float newHumidity = dht.readHumidity();

  if (!isnan(newTemperature)) {
    temperature = newTemperature;
  }

  if (!isnan(newHumidity)) {
    airHumidity = newHumidity;
  }

  waterRaw = readAnalogAverage(WATER_SENSOR_PIN);

  waterAvailable =
    waterRaw >= waterMinimumValue;

  soilRaw = readAnalogAverage(SOIL_SENSOR_PIN);

  soilPercent = map(
    soilRaw,
    soilDryValue,
    soilWetValue,
    0,
    100
  );

  soilPercent = constrain(
    soilPercent,
    0,
    100
  );
}

// =====================================================
// 15. KIEM TRA THOI GIAN NGHI BOM
// =====================================================

bool pumpCooldownFinished() {
  if (pumpStoppedTime == 0) {
    return true;
  }

  return (
    millis() - pumpStoppedTime >=
    PUMP_COOLDOWN
  );
}

// =====================================================
// 16. GUI TRANG THAI BOM
// =====================================================

void publishPumpState() {
  if (!mqttClient.connected()) {
    return;
  }

  String payload = "{";

  payload += "\"pump\":";
  payload += pumpState ? "true" : "false";

  payload += ",\"mode\":\"";
  payload += autoMode ? "auto" : "manual";
  payload += "\"";

  payload += ",\"soil_start\":";
  payload += String(soilStartPump);

  payload += ",\"soil_stop\":";
  payload += String(soilStopPump);

  payload += "}";

  mqttClient.publish(
    TOPIC_PUMP_STATE,
    payload.c_str(),
    true
  );

  publishAgriState();
}

// =====================================================
// 17. BAT/TAT BOM
// =====================================================

void writeRelayOff() {
  digitalWrite(
    RELAY_PIN,
    RELAY_OFF
  );

  delay(20);

  digitalWrite(
    RELAY_PIN,
    RELAY_OFF
  );
}

void writeRelayOn() {
  digitalWrite(
    RELAY_PIN,
    RELAY_ON
  );

  delay(20);

  digitalWrite(
    RELAY_PIN,
    RELAY_ON
  );
}

bool setPump(bool newState) {
  // -------------------------
  // YEU CAU BAT BOM
  // -------------------------

  if (newState) {
    if (pumpState) {
      return true;
    }

    if (
      USE_WATER_SAFETY &&
      !waterAvailable
    ) {
      Serial.println(
        "KHONG BAT BOM: CAM BIEN BAO HET NUOC"
      );

      return false;
    }

    if (!pumpCooldownFinished()) {
      Serial.println(
        "KHONG BAT BOM: BOM DANG NGHI"
      );

      return false;
    }

    writeRelayOn();

    pumpState = true;
    pumpStartedTime = millis();

    Serial.println(">>> MAY BOM DA BAT");

    publishPumpState();

    return true;
  }

  // -------------------------
  // YEU CAU TAT BOM
  // -------------------------

  writeRelayOff();
  manualPumpOffAt = 0;

  if (pumpState) {
    pumpState = false;
    pumpStoppedTime = millis();

    Serial.println(">>> MAY BOM DA TAT");

    publishPumpState();
  } else {
    Serial.println(">>> LENH TAT BOM DA GUI LAI TOI RELAY");
    publishPumpState();
  }

  return true;
}

// =====================================================
// 18. DIEU KHIEN TU DONG
// =====================================================

void controlPumpAutomatically() {
  if (!autoMode) {
    return;
  }

  // Het nuoc thi tat bom
  if (
    USE_WATER_SAFETY &&
    !waterAvailable
  ) {
    if (pumpState) {
      Serial.println(
        "HET NUOC -> TU DONG TAT BOM"
      );

      setPump(false);
    }

    return;
  }

  // Dat kho
  if (
    !pumpState &&
    soilPercent <= soilStartPump
  ) {
    Serial.println(
      "DAT KHO -> TU DONG BAT BOM"
    );

    setPump(true);
    return;
  }

  // Dat du am
  if (
    pumpState &&
    soilPercent >= soilStopPump
  ) {
    Serial.println(
      "DAT DU AM -> TU DONG TAT BOM"
    );

    setPump(false);
  }
}

// =====================================================
// 19. BAO VE MAY BOM
// =====================================================

void protectPump() {
  if (!pumpState) {
    return;
  }

  if (
    USE_WATER_SAFETY &&
    !waterAvailable
  ) {
    Serial.println(
      "CANH BAO: HET NUOC -> TAT BOM"
    );

    setPump(false);
    return;
  }

  if (
    manualPumpOffAt > 0 &&
    (long)(millis() - manualPumpOffAt) >= 0
  ) {
    Serial.println(
      "HET THOI GIAN TUOI THU CONG -> TAT BOM"
    );

    setPump(false);
    return;
  }

  if (
    millis() - pumpStartedTime >=
    MAX_PUMP_RUNTIME
  ) {
    Serial.println(
      "CANH BAO: BOM CHAY DU 120 GIAY"
    );

    setPump(false);
  }
}

// =====================================================
// 20. TAO JSON CAM BIEN
// =====================================================

String createSensorJson() {
  String payload = "{";

  payload += "\"temperature\":";

  if (isnan(temperature)) {
    payload += "null";
  } else {
    payload += String(temperature, 1);
  }

  payload += ",\"humidity\":";

  if (isnan(airHumidity)) {
    payload += "null";
  } else {
    payload += String(airHumidity, 1);
  }

  payload += ",\"soil_raw\":";
  payload += String(soilRaw);

  payload += ",\"soil_percent\":";
  payload += String(soilPercent);

  payload += ",\"water_raw\":";
  payload += String(waterRaw);

  payload += ",\"water_available\":";
  payload += waterAvailable ? "true" : "false";

  payload += ",\"pump\":";
  payload += pumpState ? "true" : "false";

  payload += ",\"mode\":\"";
  payload += autoMode ? "auto" : "manual";
  payload += "\"";

  payload += ",\"wifi_rssi\":";
  payload += String(WiFi.RSSI());

  payload += ",\"esp32_ip\":\"";
  payload += WiFi.localIP().toString();
  payload += "\"";

  payload += ",\"mqtt_host\":\"";
  payload += mqttHost;
  payload += "\"";

  payload += ",\"uptime_seconds\":";
  payload += String(millis() / 1000);

  payload += "}";

  return payload;
}

int waterLevelPercent() {
  int percent = map(
    waterRaw,
    0,
    1800,
    0,
    100
  );

  return constrain(
    percent,
    0,
    100
  );
}

String createAgriStateJson() {
  String payload = "{";

  payload += "\"schema\":\"agri.state.v1\"";
  payload += ",\"kind\":\"state\"";
  payload += ",\"device_id\":\"";
  payload += DEVICE_ID;
  payload += "\"";
  payload += ",\"node_type\":\"";
  payload += NODE_TYPE;
  payload += "\"";
  payload += ",\"online\":true";
  payload += ",\"transport\":\"mqtt\"";
  payload += ",\"firmware\":\"AgriControl ESP32 MQTT\"";
  payload += ",\"ip\":\"";
  payload += WiFi.localIP().toString();
  payload += "\"";
  payload += ",\"mqtt_connected\":";
  payload += mqttClient.connected() ? "true" : "false";
  payload += ",\"ws_connected\":false";
  payload += ",\"pump\":\"";
  payload += pumpState ? "on" : "off";
  payload += "\"";
  payload += ",\"auto_mode\":";
  payload += autoMode ? "true" : "false";
  payload += ",\"relay_pin\":";
  payload += String(RELAY_PIN);
  payload += ",\"ts_ms\":";
  payload += String(millis());
  payload += "}";

  return payload;
}

String createAgriTelemetryJson() {
  String payload = "{";

  payload += "\"schema\":\"agri.telemetry.v1\"";
  payload += ",\"kind\":\"telemetry\"";
  payload += ",\"device_id\":\"";
  payload += DEVICE_ID;
  payload += "\"";
  payload += ",\"node_type\":\"";
  payload += NODE_TYPE;
  payload += "\"";
  payload += ",\"soil_moisture_percent\":";
  payload += String(soilPercent);
  payload += ",\"soil_raw\":";
  payload += String(soilRaw);
  payload += ",\"water_level_percent\":";
  payload += String(waterLevelPercent());
  payload += ",\"water_raw\":";
  payload += String(waterRaw);
  payload += ",\"water_available\":";
  payload += waterAvailable ? "true" : "false";
  payload += ",\"air_temperature_c\":";

  if (isnan(temperature)) {
    payload += "null";
  } else {
    payload += String(temperature, 1);
  }

  payload += ",\"air_humidity_percent\":";

  if (isnan(airHumidity)) {
    payload += "null";
  } else {
    payload += String(airHumidity, 1);
  }

  payload += ",\"pump\":\"";
  payload += pumpState ? "on" : "off";
  payload += "\"";
  payload += ",\"auto_mode\":";
  payload += autoMode ? "true" : "false";
  payload += ",\"last_cmd_seq\":";
  payload += String(lastCommandSeq);
  payload += ",\"last_cmd_ack_status\":\"";
  payload += lastCommandAckStatus;
  payload += "\"";
  payload += ",\"last_cmd_ack_detail\":\"";
  payload += lastCommandAckDetail;
  payload += "\"";
  payload += ",\"uptime_ms\":";
  payload += String(millis());
  payload += ",\"transport\":\"mqtt\"";
  payload += ",\"ts_ms\":";
  payload += String(millis());
  payload += "}";

  return payload;
}

void publishAgriState() {
  if (!mqttClient.connected()) {
    return;
  }

  String payload = createAgriStateJson();

  mqttClient.publish(
    TOPIC_AGRI_STATE,
    payload.c_str(),
    true
  );
}

void publishAgriTelemetry() {
  if (!mqttClient.connected()) {
    return;
  }

  String payload = createAgriTelemetryJson();

  mqttClient.publish(
    TOPIC_AGRI_TELEMETRY,
    payload.c_str(),
    false
  );
}

void publishAgriCommandAck(
  unsigned long seq,
  const String& status,
  const String& detail
) {
  lastCommandSeq = seq;
  lastCommandAckStatus = status;
  lastCommandAckDetail = detail;

  if (!mqttClient.connected()) {
    return;
  }

  String payload = "{";

  payload += "\"schema\":\"agri.cmd_ack.v1\"";
  payload += ",\"device_id\":\"";
  payload += DEVICE_ID;
  payload += "\"";
  payload += ",\"node_type\":\"";
  payload += NODE_TYPE;
  payload += "\"";
  payload += ",\"command\":\"pump\"";
  payload += ",\"seq\":";
  payload += String(seq);
  payload += ",\"status\":\"";
  payload += status;
  payload += "\"";
  payload += ",\"detail\":\"";
  payload += detail;
  payload += "\"";
  payload += ",\"pump\":\"";
  payload += pumpState ? "on" : "off";
  payload += "\"";
  payload += ",\"auto_mode\":";
  payload += autoMode ? "true" : "false";
  payload += ",\"ts_ms\":";
  payload += String(millis());
  payload += "}";

  mqttClient.publish(
    TOPIC_AGRI_PUMP_ACK,
    payload.c_str(),
    false
  );
}

// =====================================================
// 21. GUI CAM BIEN LEN MQTT
// =====================================================

void publishSensorData() {
  if (!mqttClient.connected()) {
    return;
  }

  String payload = createSensorJson();

  bool success = mqttClient.publish(
    TOPIC_SENSOR,
    payload.c_str(),
    false
  );

  if (success) {
    Serial.println(
      "Da gui du lieu len MQTT:"
    );

    Serial.println(payload);
  } else {
    Serial.println(
      "Loi khi gui du lieu MQTT."
    );
  }

  publishAgriTelemetry();
}

// =====================================================
// 22. IN DU LIEU SERIAL MONITOR
// =====================================================

void printSensorData() {
  Serial.println();
  Serial.println(
    "========================================"
  );

  Serial.print("WiFi                 : ");
  Serial.println(
    WiFi.status() == WL_CONNECTED
      ? "DA KET NOI"
      : "MAT KET NOI"
  );

  Serial.print("WiFi SSID            : ");
  Serial.println(wifiSsid);

  Serial.print("IP ESP32             : ");

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("CHUA CO IP");
  }

  Serial.print("IP laptop/MQTT       : ");
  Serial.println(mqttHost);

  Serial.print("MQTT port            : ");
  Serial.println(mqttPort);

  Serial.print("MQTT                 : ");
  Serial.println(
    mqttClient.connected()
      ? "DA KET NOI"
      : "CHUA KET NOI"
  );

  Serial.print("Nhiet do             : ");

  if (isnan(temperature)) {
    Serial.println("LOI DHT22");
  } else {
    Serial.print(temperature, 1);
    Serial.println(" C");
  }

  Serial.print("Do am khong khi      : ");

  if (isnan(airHumidity)) {
    Serial.println("LOI DHT22");
  } else {
    Serial.print(airHumidity, 1);
    Serial.println(" %");
  }

  Serial.print("Soil RAW             : ");
  Serial.println(soilRaw);

  Serial.print("Do am dat            : ");
  Serial.print(soilPercent);
  Serial.println(" %");

  Serial.print("Water RAW            : ");
  Serial.println(waterRaw);

  Serial.print("Trang thai nuoc      : ");
  Serial.println(
    waterAvailable
      ? "CO NUOC"
      : "HET NUOC"
  );

  Serial.print("Che do               : ");
  Serial.println(
    autoMode
      ? "TU DONG"
      : "THU CONG"
  );

  Serial.print("May bom              : ");
  Serial.println(
    pumpState
      ? "DANG BAT"
      : "DANG TAT"
  );

  Serial.println(
    "========================================"
  );
}

// =====================================================
// 23. NHAN LENH MQTT
// =====================================================

String extractJsonText(
  const String& text,
  const String& key
) {
  String pattern = "\"";
  pattern += key;
  pattern += "\"";

  int keyIndex = text.indexOf(pattern);

  if (keyIndex < 0) {
    return "";
  }

  int colonIndex = text.indexOf(
    ':',
    keyIndex + pattern.length()
  );

  if (colonIndex < 0) {
    return "";
  }

  int valueStart = colonIndex + 1;

  while (
    valueStart < text.length() &&
    text.charAt(valueStart) == ' '
  ) {
    valueStart++;
  }

  if (
    valueStart < text.length() &&
    text.charAt(valueStart) == '"'
  ) {
    int valueEnd = text.indexOf(
      '"',
      valueStart + 1
    );

    if (valueEnd < 0) {
      return "";
    }

    return text.substring(
      valueStart + 1,
      valueEnd
    );
  }

  int valueEnd = valueStart;

  while (
    valueEnd < text.length() &&
    text.charAt(valueEnd) != ',' &&
    text.charAt(valueEnd) != '}'
  ) {
    valueEnd++;
  }

  String value = text.substring(
    valueStart,
    valueEnd
  );

  value.trim();
  return value;
}

unsigned long extractJsonNumber(
  const String& text,
  const String& key
) {
  String value = extractJsonText(
    text,
    key
  );

  if (value.length() == 0) {
    return 0;
  }

  return value.toInt();
}

String detectPumpCommand(
  const String& message
) {
  String command = extractJsonText(
    message,
    "CMD"
  );

  if (command.length() == 0) {
    command = extractJsonText(
      message,
      "COMMAND"
    );
  }

  command.trim();
  command.toUpperCase();

  if (
    command == "AUTO_OFF" ||
    command == "TAT_AUTO"
  ) {
    return "MANUAL";
  }

  if (command.length() > 0) {
    return command;
  }

  if (
    message == "AUTO_OFF" ||
    message == "TAT_AUTO"
  ) {
    return "MANUAL";
  }

  if (message == "AUTO") {
    return "AUTO";
  }

  if (message == "MANUAL") {
    return "MANUAL";
  }

  if (message == "OFF") {
    return "OFF";
  }

  if (message == "ON") {
    return "ON";
  }

  return "";
}

void handlePumpCommand(
  const String& topic,
  const String& message
) {
  String command = detectPumpCommand(
    message
  );

  unsigned long seq = extractJsonNumber(
    message,
    "SEQ"
  );

  unsigned long durationMs =
    extractJsonNumber(
      message,
      "DURATION_MS"
    );

  if (durationMs > MAX_PUMP_RUNTIME) {
    durationMs = MAX_PUMP_RUNTIME;
  }

  String ackStatus = "executed";
  String ackDetail = "";

  if (command == "AUTO") {
    autoMode = true;
    manualPumpOffAt = 0;

    Serial.println(
      "Da chuyen sang TU DONG"
    );

    readSensors();
    controlPumpAutomatically();

    ackDetail = "auto_mode_enabled";
  }

  else if (command == "MANUAL") {
    autoMode = false;
    manualPumpOffAt = 0;

    Serial.println(
      "Da chuyen sang THU CONG"
    );

    ackDetail = "manual_mode_enabled";
  }

  else if (command == "OFF") {
    autoMode = false;
    manualPumpOffAt = 0;

    setPump(false);
    ackDetail = "pump_off";
  }

  else if (command == "ON") {
    autoMode = false;
    manualPumpOffAt = 0;

    readSensors();

    if (setPump(true)) {
      if (durationMs > 0) {
        manualPumpOffAt = millis() + durationMs;
        ackDetail = "pump_on_timed";
      } else {
        ackDetail = "pump_on";
      }
    } else {
      ackStatus = "rejected";

      if (
        USE_WATER_SAFETY &&
        !waterAvailable
      ) {
        ackDetail = "water_level_low";
      } else if (!pumpCooldownFinished()) {
        ackDetail = "pump_cooldown";
      } else {
        ackDetail = "pump_start_failed";
      }
    }
  }

  else {
    ackStatus = "rejected";
    ackDetail = "invalid_cmd";
  }

  publishPumpState();
  publishAgriTelemetry();

  if (
    topic == TOPIC_AGRI_PUMP_COMMAND ||
    seq > 0
  ) {
    publishAgriCommandAck(
      seq,
      ackStatus,
      ackDetail
    );
  }
}

void mqttCallback(
  char* topic,
  byte* payload,
  unsigned int length
) {
  String message = "";

  for (unsigned int i = 0; i < length; i++) {
    message += (char)payload[i];
  }

  message.trim();
  message.toUpperCase();

  String topicText = String(topic);

  Serial.println();
  Serial.print("Nhan topic: ");
  Serial.println(topic);

  Serial.print("Nhan payload: ");
  Serial.println(message);

  if (
    topicText != TOPIC_PUMP_COMMAND &&
    topicText != TOPIC_AGRI_PUMP_COMMAND
  ) {
    return;
  }

  handlePumpCommand(
    topicText,
    message
  );
}

void handleSerialPumpCommand(
  String command
) {
  command.trim();
  command.toUpperCase();

  if (command.length() == 0) {
    return;
  }

  if (
    command == "HELP" ||
    command == "?"
  ) {
    Serial.println();
    Serial.println("Lenh Serial:");
    Serial.println("  ON        : Bat bom thu cong");
    Serial.println("  OFF       : Tat bom");
    Serial.println("  AUTO      : Bat che do tu dong");
    Serial.println("  MANUAL    : Tat che do auto");
    Serial.println("  STATUS    : In trang thai cam bien");
    return;
  }

  if (command == "STATUS") {
    readSensors();
    printSensorData();
    return;
  }

  if (
    command == "BAT" ||
    command == "BOM_ON"
  ) {
    command = "ON";
  }

  if (
    command == "TAT" ||
    command == "BOM_OFF"
  ) {
    command = "OFF";
  }

  if (
    command == "AUTO_OFF" ||
    command == "TAT_AUTO"
  ) {
    command = "MANUAL";
  }

  if (
    command != "ON" &&
    command != "OFF" &&
    command != "AUTO" &&
    command != "MANUAL"
  ) {
    Serial.println(
      "Lenh Serial khong hop le. Go HELP de xem lenh."
    );
    return;
  }

  Serial.print("Lenh Serial dieu khien bom: ");
  Serial.println(command);

  handlePumpCommand(
    String(TOPIC_PUMP_COMMAND),
    command
  );
}

void maintainSerialCommands() {
  if (!Serial.available()) {
    return;
  }

  String command = Serial.readStringUntil('\n');
  handleSerialPumpCommand(command);
}

// =====================================================
// 24. KET NOI WIFI
// =====================================================

void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.setSleep(false);

  Serial.println();
  Serial.print("Dang ket noi WiFi: ");
  Serial.println(wifiSsid);

  WiFi.begin(
    wifiSsid.c_str(),
    wifiPassword.c_str()
  );

  unsigned long startTime = millis();

  while (
    WiFi.status() != WL_CONNECTED &&
    millis() - startTime < 20000
  ) {
    writeRelayOff();

    pumpState = false;

    delay(500);
    Serial.print(".");
  }

  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println(
      "WiFi da ket noi thanh cong."
    );

    Serial.print("IP ESP32: ");
    Serial.println(WiFi.localIP());

    Serial.print("IP laptop/MQTT: ");
    Serial.println(mqttHost);
  } else {
    Serial.println(
      "Chua ket noi duoc WiFi."
    );

    Serial.println(
      "ESP32 van chay tu dong offline."
    );
  }
}

// =====================================================
// 25. DUY TRI WIFI
// =====================================================

void maintainWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    return;
  }

  if (
    millis() - lastWiFiReconnectTime <
    WIFI_RECONNECT_INTERVAL
  ) {
    return;
  }

  lastWiFiReconnectTime = millis();

  Serial.println(
    "Dang ket noi lai WiFi..."
  );

  WiFi.disconnect(false, false);

  WiFi.begin(
    wifiSsid.c_str(),
    wifiPassword.c_str()
  );
}

// =====================================================
// 26. KET NOI MQTT
// =====================================================

void connectMQTT() {
  if (
    WiFi.status() != WL_CONNECTED ||
    mqttClient.connected()
  ) {
    return;
  }

  uint64_t chipId = ESP.getEfuseMac();

  char mqttClientId[40];

  snprintf(
    mqttClientId,
    sizeof(mqttClientId),
    "AgriControl-%04X%08X",
    (uint16_t)(chipId >> 32),
    (uint32_t)chipId
  );

  Serial.print(
    "Dang ket noi MQTT toi "
  );

  Serial.print(mqttHost);
  Serial.print(":");
  Serial.println(mqttPort);

  bool connected = mqttClient.connect(
    mqttClientId,
    nullptr,
    nullptr,
    TOPIC_ONLINE,
    1,
    true,
    "offline"
  );

  if (connected) {
    Serial.println(
      "MQTT DA KET NOI THANH CONG"
    );

    mqttClient.subscribe(
      TOPIC_PUMP_COMMAND
    );

    mqttClient.subscribe(
      TOPIC_AGRI_PUMP_COMMAND
    );

    mqttClient.publish(
      TOPIC_ONLINE,
      "online",
      true
    );

    String statusPayload = "{";

    statusPayload += "\"status\":\"online\"";

    statusPayload += ",\"ip\":\"";
    statusPayload += WiFi.localIP().toString();
    statusPayload += "\"";

    statusPayload += ",\"mqtt_host\":\"";
    statusPayload += mqttHost;
    statusPayload += "\"";

    statusPayload += "}";

    mqttClient.publish(
      TOPIC_STATUS,
      statusPayload.c_str(),
      true
    );

    publishPumpState();
    publishSensorData();
    publishAgriState();
  } else {
    Serial.print(
      "Khong ket noi duoc MQTT. Ma loi: "
    );

    Serial.println(mqttClient.state());

    Serial.println(
      "Kiem tra Mosquitto, Firewall va cong 1883."
    );
  }
}

// =====================================================
// 27. DUY TRI MQTT
// =====================================================

void maintainMQTT() {
  if (mqttClient.connected()) {
    mqttClient.loop();
    return;
  }

  if (
    millis() - lastMQTTReconnectTime <
    MQTT_RECONNECT_INTERVAL
  ) {
    return;
  }

  lastMQTTReconnectTime = millis();

  connectMQTT();
}

// =====================================================
// 28. TRANG CAU HINH
// =====================================================

String createConfigurationPage() {
  String html = R"rawliteral(
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport"
        content="width=device-width,initial-scale=1">

  <title>AgriControl Setup</title>

  <style>
    body {
      font-family: Arial, sans-serif;
      max-width: 520px;
      margin: auto;
      padding: 20px;
      background: #f3f5f7;
    }

    .card {
      background: white;
      padding: 22px;
      border-radius: 12px;
      box-shadow: 0 2px 10px #bbb;
    }

    input {
      width: 100%;
      padding: 11px;
      margin: 7px 0 15px 0;
      box-sizing: border-box;
    }

    button {
      width: 100%;
      padding: 13px;
      border: none;
      border-radius: 8px;
      background: #198754;
      color: white;
      font-size: 17px;
    }
  </style>
</head>

<body>
  <div class="card">
    <h2>Cau hinh AgriControl</h2>

    <form method="POST" action="/save">
      <label>Ten Wi-Fi</label>
      <input name="ssid" value=")rawliteral";

  html += wifiSsid;

  html += R"rawliteral(" required>

      <label>Mat khau Wi-Fi</label>
      <input name="password"
             type="password"
             value=")rawliteral";

  html += wifiPassword;

  html += R"rawliteral(" required>

      <label>IP laptop / MQTT</label>
      <input name="mqtt_host" value=")rawliteral";

  html += mqttHost;

  html += R"rawliteral(" required>

      <label>MQTT Port</label>
      <input name="mqtt_port"
             type="number"
             value=")rawliteral";

  html += String(mqttPort);

  html += R"rawliteral(" required>

      <button type="submit">
        LUU VA KHOI DONG LAI
      </button>
    </form>
  </div>
</body>
</html>
)rawliteral";

  return html;
}

// =====================================================
// 29. CAU HINH WEB SERVER
// =====================================================

void configureWebServerRoutes() {
  configServer.on(
    "/",
    HTTP_GET,
    []() {
      configServer.send(
        200,
        "text/html; charset=utf-8",
        createConfigurationPage()
      );
    }
  );

  configServer.on(
    "/save",
    HTTP_POST,
    []() {
      if (
        configServer.hasArg("ssid") &&
        configServer.hasArg("password") &&
        configServer.hasArg("mqtt_host") &&
        configServer.hasArg("mqtt_port")
      ) {
        wifiSsid =
          configServer.arg("ssid");

        wifiPassword =
          configServer.arg("password");

        mqttHost =
          configServer.arg("mqtt_host");

        int newPort =
          configServer.arg("mqtt_port").toInt();

        if (
          newPort > 0 &&
          newPort <= 65535
        ) {
          mqttPort = newPort;
        }

        saveConfiguration();

        configServer.send(
          200,
          "text/html; charset=utf-8",
          "<h2>Da luu cau hinh.</h2>"
          "<p>ESP32 dang khoi dong lai...</p>"
        );

        delay(1500);
        ESP.restart();
      } else {
        configServer.send(
          400,
          "text/plain",
          "Thieu thong tin cau hinh."
        );
      }
    }
  );

  configServer.on(
    "/status",
    HTTP_GET,
    []() {
      String json = "{";

      json += "\"wifi\":";
      json += (
        WiFi.status() == WL_CONNECTED
          ? "true"
          : "false"
      );

      json += ",\"mqtt\":";
      json += (
        mqttClient.connected()
          ? "true"
          : "false"
      );

      json += ",\"esp32_ip\":\"";
      json += WiFi.localIP().toString();
      json += "\"";

      json += "}";

      configServer.send(
        200,
        "application/json",
        json
      );
    }
  );
}

// =====================================================
// 30. BAT WIFI CAU HINH
// =====================================================

void startConfigurationPortal() {
  if (configPortalActive) {
    return;
  }

  Serial.println();
  Serial.println(
    "Khoi dong Wi-Fi cau hinh..."
  );

  WiFi.mode(WIFI_AP_STA);

  IPAddress apIp(192, 168, 4, 1);
  IPAddress gateway(192, 168, 4, 1);
  IPAddress subnet(255, 255, 255, 0);

  WiFi.softAPConfig(
    apIp,
    gateway,
    subnet
  );

  bool success = WiFi.softAP(
    CONFIG_AP_SSID,
    CONFIG_AP_PASSWORD
  );

  if (!success) {
    Serial.println(
      "Khong the khoi dong Wi-Fi cau hinh."
    );

    return;
  }

  configServer.begin();
  configPortalActive = true;

  Serial.println(
    "========================================"
  );

  Serial.println(
    "WIFI CAU HINH DA BAT"
  );

  Serial.print("Ten Wi-Fi: ");
  Serial.println(CONFIG_AP_SSID);

  Serial.print("Mat khau: ");
  Serial.println(CONFIG_AP_PASSWORD);

  Serial.println(
    "Mo trinh duyet: http://192.168.4.1"
  );

  Serial.println(
    "========================================"
  );
}

// =====================================================
// 31. TAT WIFI CAU HINH
// =====================================================

void stopConfigurationPortal() {
  if (!configPortalActive) {
    return;
  }

  configServer.stop();
  WiFi.softAPdisconnect(false);
  WiFi.mode(WIFI_STA);

  configPortalActive = false;

  Serial.println(
    "Da tat Wi-Fi cau hinh."
  );
}

// =====================================================
// 32. KIEM TRA MANG VA BAT PORTAL
// =====================================================

void manageConfigurationPortal() {
  bool networkReady =
    WiFi.status() == WL_CONNECTED &&
    mqttClient.connected();

  if (networkReady) {
    networkOfflineSince = 0;

    if (configPortalActive) {
      stopConfigurationPortal();
    }

    return;
  }

  if (networkOfflineSince == 0) {
    networkOfflineSince = millis();
  }

  if (
    !configPortalActive &&
    millis() - networkOfflineSince >=
    CONFIG_PORTAL_DELAY
  ) {
    startConfigurationPortal();
  }

  if (configPortalActive) {
    configServer.handleClient();
  }
}

// =====================================================
// 33. SETUP
// =====================================================

void setup() {
  Serial.begin(115200);
  Serial.setTimeout(100);

  pinMode(RELAY_PIN, OUTPUT);
  pinMode(WATER_SENSOR_PIN, INPUT);
  pinMode(SOIL_SENSOR_PIN, INPUT);

  // Dam bao bom tat luc khoi dong
  writeRelayOff();

  pumpState = false;

  analogReadResolution(12);

  dht.begin();

  configureWebServerRoutes();
  loadConfiguration();

  mqttClient.setServer(
    mqttHost.c_str(),
    mqttPort
  );

  mqttClient.setCallback(
    mqttCallback
  );

  mqttClient.setBufferSize(768);
  mqttClient.setKeepAlive(30);
  mqttClient.setSocketTimeout(2);

  Serial.println();
  Serial.println(
    "========================================"
  );

  Serial.println(
    "HE THONG TUOI CAY THONG MINH"
  );

  Serial.println(
    "ESP32 + SENSOR + BOM + WIFI + MQTT"
  );

  Serial.println(
    "SERIAL: ON, OFF, AUTO, MANUAL, STATUS, HELP"
  );

  Serial.println(
    "========================================"
  );

  delay(2000);

  readSensors();
  printSensorData();

  connectWiFi();
  connectMQTT();

  lastSensorTime = millis();

  if (
    WiFi.status() != WL_CONNECTED ||
    !mqttClient.connected()
  ) {
    networkOfflineSince = millis();
  }
}

// =====================================================
// 34. LOOP
// =====================================================

void loop() {
  maintainWiFi();
  maintainMQTT();
  maintainSerialCommands();

  manageConfigurationPortal();

  // Bao ve bom lien tuc
  protectPump();

  if (
    millis() - lastSensorTime >=
    SENSOR_INTERVAL
  ) {
    lastSensorTime = millis();

    readSensors();
    printSensorData();

    // Van tu dong tuoi du MQTT bi mat
    controlPumpAutomatically();

    publishSensorData();
  }

  delay(2);
}
