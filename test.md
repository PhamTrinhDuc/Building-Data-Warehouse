# BÁO CÁO DỰ ÁN: HỆ THỐNG IoT THÔNG MINH TRÊN NỀN TẢNG PHÂN TÁN

**Môn học:** Các Hệ Thống Phân Tán  
**Chủ đề:** AI + IoT — Smart Factory Monitoring System  
**Công nghệ chính:** Apache Kafka · Python · YOLOv8 · Scikit-learn · GCP · Docker  

---

## MỤC LỤC

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Bài toán & Kịch bản thực tế](#2-bài-toán--kịch-bản-thực-tế)
3. [Kiến trúc hệ thống](#3-kiến-trúc-hệ-thống)
4. [Các thành phần chi tiết](#4-các-thành-phần-chi-tiết)
   - 4.1 [IoT Simulator — Producer](#41-iot-simulator--producer)
   - 4.2 [Apache Kafka — Message Broker](#42-apache-kafka--message-broker)
   - 4.3 [Service A — Stream Analytics (Task 1)](#43-service-a--stream-analytics-task-1)
   - 4.4 [Service B — ML Predict Failure (Task 2)](#44-service-b--ml-predict-failure-task-2)
   - 4.5 [Service C — YOLO Camera Detection (Task 3)](#45-service-c--yolo-camera-detection-task-3)
   - 4.6 [Service D — Correlation Engine](#46-service-d--correlation-engine)
5. [Luồng dữ liệu end-to-end](#5-luồng-dữ-liệu-end-to-end)
6. [Các tính chất Hệ Thống Phân Tán được thể hiện](#6-các-tính-chất-hệ-thống-phân-tán-được-thể-hiện)
7. [Công nghệ & Stack](#7-công-nghệ--stack)
8. [Kế hoạch triển khai](#8-kế-hoạch-triển-khai)

---

## 1. Tổng quan dự án

### 1.1 Ba nhiệm vụ đặt ra

| # | Nhiệm vụ | Mô tả |
|---|---|---|
| Task 1 | **Hệ thống giám sát & phân tích dữ liệu thời gian thực** | Thu thập dữ liệu cảm biến IoT liên tục, xử lý stream, lưu trữ và hiển thị trên dashboard |
| Task 2 | **ML dự đoán sự cố thiết bị** | Dùng Isolation Forest phát hiện pattern bất thường từ dữ liệu cảm biến, cảnh báo trước khi thiết bị hỏng |
| Task 3 | **Giám sát an ninh bằng nhận diện hình ảnh** | YOLOv8 xử lý camera feed, phát hiện người/vật thể bất thường trong khu vực nhà máy |

### 1.2 Điểm then chốt của thiết kế

Ba nhiệm vụ **không tồn tại độc lập** mà được tích hợp vào **một hệ thống phân tán thống nhất**, trong đó:

- **Dữ liệu cảm biến** và **dữ liệu hình ảnh** là hai nguồn độc lập, được xử lý song song bởi các service riêng biệt
- Một **Correlation Engine** kết hợp kết quả từ cả hai nguồn để đưa ra alert có độ tin cậy cao hơn
- Toàn bộ giao tiếp giữa các service qua **Apache Kafka** — không service nào gọi trực tiếp service khác

---

## 2. Bài toán & Kịch bản thực tế

### 2.1 Kịch bản: Smart Factory

Một nhà máy có **50 máy móc** được trang bị:
- **Cảm biến vật lý**: đo nhiệt độ, độ rung, điện áp, áp suất mỗi giây
- **Camera IP**: giám sát theo khu vực (zone A, B, C, D)

Yêu cầu hệ thống phải:
1. **Theo dõi liên tục** toàn bộ trạng thái máy móc theo thời gian thực
2. **Phát hiện sớm** nguy cơ sự cố trước khi xảy ra (predictive maintenance)
3. **Giám sát an toàn** — cảnh báo khi có người tiếp cận máy nguy hiểm

### 2.2 Câu hỏi cốt lõi: Làm sao sensor biết máy sắp hỏng?

Máy móc công nghiệp khi **sắp hỏng** luôn có dấu hiệu vật lý trước:

| Triệu chứng vật lý | Cảm biến đo được | Dấu hiệu |
|---|---|---|
| Vòng bi (bearing) mòn | Vibration sensor | Độ rung tăng dần đều theo thời gian |
| Motor quá tải | Temp + Power sensor | Nhiệt độ tăng + Điện tiêu thụ tăng đột biến |
| Bơm tắc nghẽn | Pressure sensor | Áp suất tăng bất thường |
| Rò rỉ khí nén | Pressure sensor | Áp suất giảm liên tục |

Sensor không trực tiếp biết máy hỏng — nó đo **triệu chứng vật lý**. ML model học được **profile bình thường** của từng máy, sau đó phát hiện khi pattern **lệch khỏi bình thường**.

```
Máy A — baseline bình thường:
  Nhiệt độ:  65–75°C
  Độ rung:   0.01–0.03 mm/s
  Điện áp:   215–225W

Hôm nay đo được:
  Nhiệt độ:  91°C  ← lệch khỏi baseline
  Độ rung:   0.09 mm/s ← lệch khỏi baseline
  Điện áp:   248W  ← lệch khỏi baseline

→ Model: ANOMALY — máy có nguy cơ sự cố
```

**Quan trọng:** Không check từng điểm đơn lẻ (nhiễu cao) mà check **xu hướng theo window thời gian**:

```
Window 7 điểm gần nhất: [69, 71, 74, 78, 83, 88, 91]
  → Rate of change:  +22°C trong 7 phút  ← tăng nhanh bất thường
  → Slope dương:     xu hướng leo thang
  → Std deviation:   7.8 ← biến động lớn hơn bình thường
→ Đây là feature đưa vào ML model, không phải giá trị thô
```

### 2.3 Tại sao cần kết hợp sensor + camera?

Xét ví dụ cụ thể:

```
Chỉ sensor:   "machine_01 nhiệt độ cao"
              → Alert MEDIUM (có thể là false positive, thời tiết nóng)

Chỉ camera:   "zone_A có người lúc 2 giờ sáng"
              → Alert LOW (có thể là bảo vệ đi tuần)

Kết hợp cả 2: "Máy bất thường + có người đứng gần lúc 2 giờ sáng"
              → Alert CRITICAL — cần xử lý ngay
```

**Kết hợp hai nguồn = tăng độ tin cậy, giảm false positive.**

---

## 3. Kiến trúc hệ thống

### 3.1 Sơ đồ tổng thể

```
┌────────────────────────────────────────────────────────────────────┐
│                       IoT SIMULATOR (Producer)                     │
│                                                                    │
│   [Sensor Simulator]                    [Camera Simulator]         │
│   50 thiết bị × 1 msg/giây             Đọc video file → frames    │
│   {device_id, zone, temp,              {zone, timestamp,           │
│    vibration, power, pressure}          frame_base64}              │
└──────────────┬──────────────────────────────┬──────────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────────────────────────────────────────────────  ┐
│                      APACHE KAFKA (Message Broker)                 │
│                                                                    │
│   Topic: raw-sensor        Topic: raw-camera                       │
│   (3 partitions)           (3 partitions)                          │
│                                                                    │
│   Topic: alerts            Topic: camera-events                    │
│   (1 partition)            (1 partition)                           │
└──────┬──────────────────────────┬───────────────────┬─────────────┘
       │                          │                   │
       ▼                          ▼                   ▼
┌────────────┐           ┌─────────────────┐   ┌───────────────────┐
│ SERVICE A  │           │   SERVICE B     │   │    SERVICE C      │
│            │           │                 │   │                   │
│  Stream    │           │  ML Predict     │   │  YOLO Detection   │
│ Analytics  │           │                 │   │                   │
│ (Task 1)   │           │  Isolation      │   │  YOLOv8n          │
│            │           │  Forest         │   │                   │
│ Consume:   │           │  Consume:       │   │  Consume:         │
│ raw-sensor │           │  raw-sensor     │   │  raw-camera       │
│            │           │                 │   │                   │
│ Output:    │           │  Produce:       │   │  Produce:         │
│ BigQuery   │◄──────────│  alerts topic   │   │  camera-events    │
│ (realtime) │  (metrics)│  (nếu anomaly)  │   │  topic            │
└─────┬──────┘           └────────┬────────┘   └────────┬──────────┘
      │                           │                     │
      │ BigQuery                  └──────────┬───────────┘
      │                                      │ alerts + camera-events
      │                                      ▼
      │                          ┌───────────────────────┐
      │                          │      SERVICE D        │
      │                          │                       │
      │                          │  Correlation Engine   │
      │                          │                       │
      │                          │  Join theo zone +     │
      │                          │  time window 30s      │
      │                          │                       │
      │                          │  REST API:            │
      │                          │  GET /alerts          │
      │                          │  GET /health          │
      │                          └───────────┬───────────┘
      │                                      │
      └──────────────────┬───────────────────┘
                         ▼
              ┌─────────────────────┐
              │   GRAFANA DASHBOARD │
              │   localhost:3000    │
              │                    │
              │  Datasource 1:     │
              │  BigQuery plugin   │← sensor charts
              │                    │
              │  Datasource 2:     │
              │  JSON API plugin   │← alerts table
              │  (poll /alerts)    │
              └────────────────────┘
```

### 3.2 Nguyên tắc thiết kế

**Event-Driven Architecture:** Không service nào gọi trực tiếp service khác. Tất cả giao tiếp qua Kafka topics.

**Separation of Concerns:**
- Service A — chỉ lo analytics và lưu trữ
- Service B — chỉ lo ML inference
- Service C — chỉ lo computer vision
- Service D — chỉ lo correlation và API

**Tại sao Service A ghi thẳng BigQuery, còn B và C publish vào Kafka?**

| | Service A | Service B & C |
|---|---|---|
| Output là gì? | Time-series data thô (mọi message) | Event thưa — chỉ khi có bất thường |
| Volume | 50 msg/giây = ~4 triệu/ngày | Vài chục alert/ngày |
| Ai cần đọc tiếp? | Grafana — chỉ cần visualize | Service D cần correlate realtime |
| Cần correlation? | Không | Có |

---

## 4. Các thành phần chi tiết

### 4.1 IoT Simulator — Producer

**Mục đích:** Mô phỏng 50 thiết bị IoT gửi dữ liệu liên tục lên Kafka, thay thế phần cứng thực tế.

#### Sensor Simulator

Mỗi thiết bị được gán một `baseline` riêng — nhiệt độ hoạt động bình thường của từng máy khác nhau. 5% thời gian sẽ **inject anomaly** để tạo dữ liệu huấn luyện và test model.

```python
# sensor_simulator.py
import asyncio, json, random, time
from kafka import KafkaProducer

DEVICES = [
    {"id": f"machine_{i:02d}", "zone": f"zone_{chr(65 + i//13)}",
     "baseline": {"temp": random.uniform(60, 80), "vibration": 0.02, "power": 220}}
    for i in range(50)
]

async def simulate_device(producer, device):
    while True:
        is_anomaly = random.random() < 0.05   # 5% inject lỗi

        data = {
            "device_id":   device["id"],
            "zone":        device["zone"],
            "timestamp":   time.time(),
            "temperature": device["baseline"]["temp"]
                           + random.gauss(0, 2)
                           + (random.uniform(15, 30) if is_anomaly else 0),
            "vibration":   device["baseline"]["vibration"]
                           + random.gauss(0, 0.003)
                           + (random.uniform(0.05, 0.1) if is_anomaly else 0),
            "power":       device["baseline"]["power"]
                           + random.gauss(0, 5)
                           + (random.uniform(20, 40) if is_anomaly else 0),
            "is_injected_fault": is_anomaly   # label để đánh giá model sau
        }
        producer.send("raw-sensor", json.dumps(data).encode())
        await asyncio.sleep(1)
```

#### Camera Simulator

Đọc một video file (factory footage) frame-by-frame và gửi lên topic `raw-camera`. Mỗi frame được encode base64 hoặc upload lên GCS và chỉ gửi URL.

```python
# camera_simulator.py
import cv2, base64, json, time
from kafka import KafkaProducer

CAMERAS = [
    {"id": "cam_A", "zone": "zone_A", "video": "data/zone_a.mp4"},
    {"id": "cam_B", "zone": "zone_B", "video": "data/zone_b.mp4"},
]

def simulate_camera(producer, camera):
    cap = cv2.VideoCapture(camera["video"])
    while True:
        ret, frame = cap.read()
        if not ret:
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)   # loop video
            continue

        # Resize để giảm kích thước gửi qua Kafka
        frame_small = cv2.resize(frame, (640, 480))
        _, buffer = cv2.imencode(".jpg", frame_small, [cv2.IMWRITE_JPEG_QUALITY, 70])
        frame_b64 = base64.b64encode(buffer).decode()

        data = {
            "camera_id": camera["id"],
            "zone":      camera["zone"],
            "timestamp": time.time(),
            "frame":     frame_b64
        }
        producer.send("raw-camera", json.dumps(data).encode())
        time.sleep(1/10)   # 10 FPS
```

---

### 4.2 Apache Kafka — Message Broker

**Vai trò:** Trung tâm giao tiếp bất đồng bộ giữa toàn bộ các service.

#### Cấu hình Kafka topics

| Topic | Partitions | Retention | Mô tả |
|---|---|---|---|
| `raw-sensor` | 3 | 24h | Dữ liệu cảm biến thô từ 50 thiết bị |
| `raw-camera` | 3 | 2h | Camera frames (dung lượng lớn, retention ngắn) |
| `alerts` | 1 | 7 ngày | Sensor anomaly alerts từ Service B |
| `camera-events` | 1 | 7 ngày | Detection events từ Service C |

#### Tại sao Kafka thay vì GCP Pub/Sub?

Kafka được chọn vì thể hiện rõ các khái niệm hệ thống phân tán:

- **Consumer Group:** Nhiều instance cùng group tự động chia nhau partitions
- **Offset Management:** Service down rồi restart sẽ đọc lại từ đúng vị trí, không mất message
- **Partition:** Đơn vị song song hóa, tường minh hơn Pub/Sub

---

### 4.3 Service A — Stream Analytics (Task 1)

**Subscribe:** `raw-sensor`  
**Output:** BigQuery (ghi trực tiếp)  
**Consumer group:** `analytics-group`

#### Chức năng

- Nhận toàn bộ sensor data từ 50 thiết bị
- Tính **rolling average** 60 điểm gần nhất theo từng thiết bị
- Phát hiện spike đơn giản (> 3 sigma)
- Ghi data thô + metrics vào **BigQuery** để Grafana query

```python
# service-a-analytics/main.py
from kafka import KafkaConsumer
from google.cloud import bigquery
import json, statistics, os

consumer = KafkaConsumer(
    "raw-sensor",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    group_id="analytics-group",
    value_deserializer=lambda x: json.loads(x)
)

bq = bigquery.Client()
TABLE = f"{os.getenv('GCP_PROJECT')}.iot_analytics.sensor_data"

windows = {}   # rolling window per device: {device_id: [values]}

for msg in consumer:
    data = msg.value
    device = data["device_id"]

    # Cập nhật rolling window
    windows.setdefault(device, []).append(data["temperature"])
    if len(windows[device]) > 60:
        windows[device].pop(0)

    w = windows[device]
    avg  = statistics.mean(w)
    std  = statistics.stdev(w) if len(w) > 1 else 0
    roc  = w[-1] - w[0]        # rate of change
    is_spike = abs(data["temperature"] - avg) > 3 * std

    row = {
        **data,
        "rolling_avg_temp": round(avg, 2),
        "temp_std":         round(std, 2),
        "rate_of_change":   round(roc, 2),
        "is_spike":         is_spike,
        "insert_time":      "AUTO"
    }
    bq.insert_rows_json(TABLE, [row])
```

#### Scale demo

```bash
# Chạy 3 instance song song — Kafka chia đều 3 partitions
docker-compose up -d --scale service-a-analytics=3

# Kết quả:
# Instance 1 → Partition 0 (device_00 → device_16)
# Instance 2 → Partition 1 (device_17 → device_33)
# Instance 3 → Partition 2 (device_34 → device_49)
```

---

### 4.4 Service B — ML Predict Failure (Task 2)

**Subscribe:** `raw-sensor`  
**Publish:** `alerts` (khi phát hiện anomaly)  
**Consumer group:** `ml-group`

#### Tại sao dùng Isolation Forest?

Isolation Forest là thuật toán **unsupervised anomaly detection** — không cần dữ liệu lỗi được gán nhãn trước. Phù hợp với bài toán này vì trong thực tế dữ liệu sự cố thiết bị rất hiếm và khó label.

Nguyên lý: Điểm dữ liệu "bất thường" (outlier) cần **ít bước hơn** để isolate trong một random tree, do đó có anomaly score thấp hơn.

#### Feature Engineering — Quan trọng hơn model

Thay vì đưa giá trị thô vào model, sử dụng **derived features** từ time window để nắm bắt xu hướng:

```python
def extract_features(window: list[dict]) -> list[float]:
    """
    window: 60 data points gần nhất của 1 thiết bị
    """
    temps  = [d["temperature"] for d in window]
    vibs   = [d["vibration"]   for d in window]
    powers = [d["power"]       for d in window]

    return [
        statistics.mean(temps),              # F1: nhiệt độ trung bình
        statistics.stdev(temps),             # F2: độ biến động nhiệt độ
        temps[-1] - temps[0],                # F3: rate of change (xu hướng tăng/giảm)
        max(vibs),                           # F4: rung cực đại trong window
        statistics.mean(vibs),               # F5: rung trung bình
        statistics.mean(powers),             # F6: điện tiêu thụ trung bình
        powers[-1] - powers[0],              # F7: rate of change điện
    ]
    # 7 features này nắm bắt được: mức độ, biến động, xu hướng
```

#### Train model (offline)

```python
# service-b-ml/train_model.py
from sklearn.ensemble import IsolationForest
from google.cloud import bigquery
import pandas as pd, pickle

# Lấy data bình thường từ BigQuery (lọc bỏ injected faults)
query = """
    SELECT temperature, vibration, power, rate_of_change, temp_std
    FROM `iot_analytics.sensor_data`
    WHERE is_injected_fault = FALSE
    LIMIT 50000
"""
df = bigquery.Client().query(query).to_dataframe()

model = IsolationForest(
    n_estimators=100,
    contamination=0.05,   # giả định 5% là anomaly
    random_state=42
)
model.fit(df)

pickle.dump(model, open("models/isolation_forest.pkl", "wb"))
print(f"✅ Trained on {len(df)} samples")
```

#### Inference service (realtime)

```python
# service-b-ml/predict.py
import pickle
from kafka import KafkaConsumer, KafkaProducer

model = pickle.load(open("models/isolation_forest.pkl", "rb"))
windows = {}

for msg in consumer:
    data = msg.value
    device = data["device_id"]

    # Cập nhật window
    windows.setdefault(device, []).append(data)
    if len(windows[device]) > 60:
        windows[device].pop(0)

    if len(windows[device]) < 10:
        continue   # chưa đủ data để predict

    features = [extract_features(windows[device])]
    score      = model.decision_function(features)[0]   # âm = bất thường
    prediction = model.predict(features)[0]             # -1 = anomaly

    if prediction == -1:
        alert = {
            "device_id": device,
            "zone":      data["zone"],
            "timestamp": data["timestamp"],
            "type":      classify_anomaly(windows[device]),  # "overheating" / "vibration" / "power_surge"
            "severity":  "HIGH" if score < -0.2 else "MEDIUM",
            "score":     round(score, 4)
        }
        producer.send("alerts", json.dumps(alert).encode())

def classify_anomaly(window):
    """Đoán loại sự cố dựa trên feature nào bất thường nhất"""
    temps  = [d["temperature"] for d in window]
    vibs   = [d["vibration"]   for d in window]
    powers = [d["power"]       for d in window]

    temp_z  = (temps[-1]  - statistics.mean(temps[:-1]))  / (statistics.stdev(temps[:-1])  + 1e-9)
    vib_z   = (vibs[-1]   - statistics.mean(vibs[:-1]))   / (statistics.stdev(vibs[:-1])   + 1e-9)
    power_z = (powers[-1] - statistics.mean(powers[:-1])) / (statistics.stdev(powers[:-1]) + 1e-9)

    dominant = max([("overheating", temp_z), ("vibration", vib_z), ("power_surge", power_z)],
                   key=lambda x: x[1])
    return dominant[0]
```

---

### 4.5 Service C — YOLO Camera Detection (Task 3)

**Subscribe:** `raw-camera`  
**Publish:** `camera-events` (khi phát hiện đối tượng đáng chú ý)  
**Consumer group:** `yolo-group`

#### Tại sao YOLOv8n?

YOLOv8 nano là model nhỏ nhất trong họ YOLOv8, inference ~30ms/frame trên CPU bình thường — đủ nhanh để xử lý 10 FPS camera feed trên một container thông thường mà không cần GPU.

```python
# service-c-yolo/main.py
from ultralytics import YOLO
from kafka import KafkaConsumer, KafkaProducer
import cv2, numpy as np, base64, json

model = YOLO("yolov8n.pt")   # ~6MB, tự download lần đầu

# Classes cần giám sát (từ COCO dataset)
WATCH = {
    "person":       "worker_detected",
    "fire hydrant": "fire_equipment",
    # Có thể fine-tune thêm class "fire", "smoke" với custom dataset
}

for msg in consumer:
    data = msg.value

    # Decode frame
    img_bytes = base64.b64decode(data["frame"])
    frame = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)

    # Inference
    results = model(frame, conf=0.5, verbose=False)
    detected_classes = set()
    detections_detail = []

    for box in results[0].boxes:
        cls_name   = model.names[int(box.cls)]
        confidence = float(box.conf)
        if cls_name in WATCH:
            detected_classes.add(cls_name)
            detections_detail.append({
                "class": cls_name,
                "confidence": round(confidence, 3),
                "bbox": box.xyxy[0].tolist()
            })

    if detected_classes:
        event = {
            "camera_id":  data["camera_id"],
            "zone":       data["zone"],
            "timestamp":  data["timestamp"],
            "detected":   list(detected_classes),
            "event":      "person_detected" if "person" in detected_classes else "object_detected",
            "detail":     detections_detail,
            "person_count": sum(1 for d in detections_detail if d["class"] == "person")
        }
        producer.send("camera-events", json.dumps(event).encode())
        print(f"📷 [{data['zone']}] Detected: {detected_classes}")
```

---

### 4.6 Service D — Correlation Engine

**Subscribe:** `alerts` + `camera-events`  
**Output:** REST API (FastAPI)  
**Consumer group:** `correlation-group`

#### Vấn đề cần giải quyết: 2 luồng không đến cùng lúc

```
t=10.0s: Service B → alert "machine_01, zone_A, overheating"
t=10.3s: Service C → event "zone_A, person_detected"
                             ↑
         Cách nhau 0.3s — có phải cùng 1 incident không?
```

Giải pháp: **Time-window buffer** — giữ event trong 30 giây, sau đó match theo zone.

#### Logic Correlation chi tiết

```python
# service-d-correlation/main.py
from fastapi import FastAPI
from collections import defaultdict
import threading, time, json

app = FastAPI()

# Buffer lưu event tạm thời theo zone
sensor_buffer = defaultdict(list)    # zone → [alerts]
camera_buffer = defaultdict(list)    # zone → [events]
final_alerts  = []
WINDOW_SEC    = 30

# ---- Consumer thread ----
def consume_alerts():
    for msg in alert_consumer:
        alert = json.loads(msg.value)
        zone  = alert["zone"]
        sensor_buffer[zone].append(alert)

def consume_camera():
    for msg in camera_consumer:
        event = json.loads(msg.value)
        zone  = event["zone"]
        camera_buffer[zone].append(event)

# ---- Correlation thread ----
def correlate():
    while True:
        now = time.time()
        all_zones = set(sensor_buffer.keys()) | set(camera_buffer.keys())

        for zone in all_zones:
            # Lọc event còn trong window 30 giây
            s_events = [e for e in sensor_buffer[zone]  if now - e["timestamp"] < WINDOW_SEC]
            c_events = [e for e in camera_buffer[zone]  if now - e["timestamp"] < WINDOW_SEC]
            sensor_buffer[zone] = s_events
            camera_buffer[zone] = c_events

            # Không có gì để correlate
            if not s_events and not c_events:
                continue

            alert = apply_rules(zone, s_events, c_events, now)
            if alert:
                final_alerts.append(alert)
                print(f"🚨 [{alert['severity']}] {alert['message']}")

        time.sleep(5)   # chạy correlation mỗi 5 giây

def apply_rules(zone, s_events, c_events, now):
    has_sensor  = len(s_events) > 0
    has_camera  = len(c_events) > 0
    has_person  = any("person" in e.get("detected", []) for e in c_events)
    has_high    = any(e.get("severity") == "HIGH"       for e in s_events)
    anomaly_type = s_events[-1]["type"] if s_events else None

    # Rule 1: Máy nguy hiểm + có người → nguy hiểm cho người
    if has_sensor and has_person and anomaly_type in ["overheating", "vibration"]:
        return {
            "severity":  "CRITICAL",
            "zone":      zone,
            "message":   f"Worker near failing machine in {zone}!",
            "action":    "Evacuate zone immediately",
            "timestamp": now,
            "sources":   ["sensor", "camera"]
        }

    # Rule 2: Nhiệt độ cao bất thường + phát hiện vật thể lạ → nguy cơ cháy
    if has_high and anomaly_type == "overheating" and has_camera and not has_person:
        return {
            "severity":  "HIGH",
            "zone":      zone,
            "message":   f"Possible fire hazard in {zone} — no personnel present",
            "action":    "Dispatch safety team",
            "timestamp": now,
            "sources":   ["sensor", "camera"]
        }

    # Rule 3: Chỉ sensor alert — không có người → ưu tiên thấp hơn
    if has_sensor and not has_camera:
        return {
            "severity":  s_events[-1]["severity"],
            "zone":      zone,
            "message":   f"Machine anomaly in {zone} ({anomaly_type}) — zone appears clear",
            "action":    "Schedule maintenance",
            "timestamp": now,
            "sources":   ["sensor"]
        }

    return None

# ---- REST API ----
@app.get("/alerts")
def get_alerts(limit: int = 20):
    return {"alerts": final_alerts[-limit:], "total": len(final_alerts)}

@app.get("/health")
def health():
    return {
        "status":        "ok",
        "alerts_total":  len(final_alerts),
        "buffer_sensor": {z: len(v) for z, v in sensor_buffer.items()},
        "buffer_camera": {z: len(v) for z, v in camera_buffer.items()}
    }

# Khởi động các thread
threading.Thread(target=consume_alerts,  daemon=True).start()
threading.Thread(target=consume_camera,  daemon=True).start()
threading.Thread(target=correlate,       daemon=True).start()
```

---

## 5. Luồng dữ liệu end-to-end

### 5.1 Luồng cảm biến bình thường

```
[Sensor Simulator]
  → produce {device_id: "machine_01", zone: "zone_A", temp: 72.3, ...}
  → Kafka topic: raw-sensor

[Service A] consume raw-sensor
  → tính rolling avg = 71.8, std = 1.9
  → không phải spike
  → ghi vào BigQuery: {temp: 72.3, rolling_avg: 71.8, is_spike: false}

[Service B] consume raw-sensor
  → extract 7 features từ window 60 điểm
  → Isolation Forest predict: NORMAL (score = +0.15)
  → không publish gì

[Grafana] query BigQuery mỗi 30s
  → hiển thị line chart nhiệt độ theo thời gian
```

### 5.2 Luồng phát hiện sự cố (Sensor + Camera)

```
[Sensor Simulator]
  → inject anomaly cho machine_01, zone_A
  → temp: 91°C, vibration: 0.09

[Service B] consume raw-sensor
  → features: [mean=83, std=7.8, roc=+22, vib_max=0.09, ...]
  → Isolation Forest: ANOMALY (score = -0.28)
  → classify: "overheating"
  → publish to alerts: {device: machine_01, zone: zone_A, type: overheating, severity: HIGH}

[Camera Simulator]
  → frame zone_A có người đứng gần machine_01

[Service C] consume raw-camera
  → YOLOv8 detect: person (confidence 0.87)
  → publish to camera-events: {zone: zone_A, detected: ["person"]}

[Service D] correlation thread (chạy mỗi 5s)
  → sensor_buffer[zone_A] = [alert overheating HIGH]
  → camera_buffer[zone_A] = [event person_detected]
  → Cả 2 trong window 30s → apply Rule 1
  → final_alert: {severity: CRITICAL, message: "Worker near failing machine!"}

[Grafana]
  → poll GET /alerts mỗi 10s
  → hiển thị CRITICAL alert trên dashboard
  → gửi notification
```

### 5.3 Luồng camera độc lập (không có sensor alert)

```
[Service C] detect person trong zone_B lúc 2:00 AM
  → publish camera-events: {zone: zone_B, detected: ["person"], timestamp: ...}

[Service D] correlation
  → camera_buffer[zone_B] có event
  → sensor_buffer[zone_B] trống (không có anomaly)
  → Rule 3 không match
  → Không tạo alert (chỉ 1 nguồn xác nhận, không đủ tin cậy)
  → Ghi log để review sau
```

> **Lưu ý thiết kế:** Service D có thể được cấu hình để alert kể cả chỉ 1 nguồn, nhưng severity thấp hơn. Việc yêu cầu 2 nguồn xác nhận giúp giảm false positive đáng kể.

---

## 6. Các tính chất Hệ Thống Phân Tán được thể hiện

### 6.1 Loose Coupling — Các service độc lập hoàn toàn

Không có bất kỳ service nào gọi trực tiếp HTTP/gRPC vào service khác. Tất cả giao tiếp qua Kafka. Service B không biết Service C tồn tại, và ngược lại.

**Demo:** Tắt Service C (YOLO) → Service A và B vẫn xử lý sensor data bình thường, không bị ảnh hưởng.

### 6.2 Fault Tolerance — Tự phục hồi sau sự cố

Kafka lưu message theo **offset**. Khi một service bị crash và restart, nó đọc lại từ last committed offset — không mất message nào.

```bash
# Demo fault tolerance
docker-compose stop service-c-yolo
# → Service A, B tiếp tục chạy
# → Kafka giữ camera frames trong topic (chưa có consumer đọc)

docker-compose start service-c-yolo
# → Service C restart, đọc lại từ offset cuối cùng đã commit
# → Xử lý các frame bị backlog trong lúc down
# → Hệ thống tự recover, không cần can thiệp thủ công
```

### 6.3 Horizontal Scalability — Tăng throughput bằng cách thêm instance

Kafka consumer group tự động chia partition cho các consumer trong cùng group:

```bash
# Scale Service A lên 3 instance
docker-compose up -d --scale service-a-analytics=3

# Kafka tự phân chia:
# Partition 0 → Instance 1
# Partition 1 → Instance 2
# Partition 2 → Instance 3
# Throughput tăng gấp 3 lần
```

### 6.4 Asynchronous Communication — Không bị block

Producer gửi message vào Kafka và tiếp tục, không cần đợi consumer xử lý xong. Khi Service B chậm (đang train model hoặc load), message tích lũy trong topic và được xử lý khi B sẵn sàng — không mất data.

### 6.5 Independent Deployment — Deploy từng service riêng lẻ

Mỗi service là một Docker container độc lập với Dockerfile riêng. Có thể update Service B (đổi ML model) mà không cần restart Service A, C, D.

### 6.6 Observability — Quan sát toàn hệ thống

| Công cụ | Quan sát gì |
|---|---|
| Grafana | Sensor metrics, alert history, throughput |
| Kafka UI | Topic lag, partition distribution, consumer group offset |
| Service D `/health` | Buffer size, total alerts, service status |
| Docker logs | Per-service debugging |

---

## 7. Công nghệ & Stack

### 7.1 Tổng hợp stack

| Layer | Công nghệ | Lý do chọn |
|---|---|---|
| **Message Broker** | Apache Kafka | Tường minh về distributed concepts, offset management, consumer group |
| **ML Model** | Isolation Forest (scikit-learn) | Unsupervised, không cần labeled data, nhẹ, dễ deploy |
| **Computer Vision** | YOLOv8n (ultralytics) | Nhanh (~30ms/frame CPU), dễ dùng, model nhỏ |
| **Stream Storage** | Google BigQuery | Serverless, tối ưu cho analytics query lớn |
| **API Framework** | FastAPI (Python) | Async, tự sinh docs, nhẹ |
| **Dashboard** | Grafana | Plugin BigQuery + JSON API có sẵn |
| **Container** | Docker + Docker Compose | Portable, dễ demo fault tolerance |
| **Cloud** | GCP (GCE + BigQuery) | Free tier đủ dùng cho demo |

### 7.2 Sơ đồ triển khai Docker

```
docker-compose.yml
  ├── kafka              (port 9092)
  ├── zookeeper          (port 2181)
  ├── kafka-ui           (port 8080)  ← monitor Kafka
  ├── simulator          (no port)    ← producer
  ├── service-a-analytics (no port)
  ├── service-b-ml        (no port)
  ├── service-c-yolo      (no port)
  ├── service-d-correlation (port 8000) ← REST API
  └── grafana            (port 3000)
```

---

## 8. Kế hoạch triển khai

### 8.1 Timeline 4 tuần

| Tuần | Mục tiêu | Checklist |
|---|---|---|
| **Tuần 1** | Hạ tầng + Simulator | ☐ Kafka + Docker Compose chạy OK &nbsp; ☐ Sensor data vào topic `raw-sensor` &nbsp; ☐ Camera frames vào topic `raw-camera` &nbsp; ☐ Verify bằng Kafka UI |
| **Tuần 2** | Task 1 — Analytics | ☐ Service A consume được `raw-sensor` &nbsp; ☐ Data vào BigQuery thành công &nbsp; ☐ Grafana kết nối BigQuery, hiển thị chart &nbsp; ☐ Rolling average tính đúng |
| **Tuần 3** | Task 2 + Task 3 | ☐ Train Isolation Forest, save model &nbsp; ☐ Service B publish alert khi anomaly &nbsp; ☐ YOLOv8 detect person trong video test &nbsp; ☐ Service C publish camera-events |
| **Tuần 4** | Service D + Demo | ☐ Correlation logic hoạt động đúng &nbsp; ☐ REST API `/alerts` trả dữ liệu &nbsp; ☐ Grafana hiển thị alert từ Service D &nbsp; ☐ Demo fault tolerance (kill/restart service) &nbsp; ☐ Báo cáo hoàn thiện |

### 8.2 Checklist demo cho thầy

**Tính năng cốt lõi:**
- [ ] Dashboard hiển thị dữ liệu sensor realtime
- [ ] ML model phát hiện anomaly và tạo alert
- [ ] YOLO detect người trong camera feed
- [ ] Correlation Engine tạo combined alert CRITICAL

**Tính chất phân tán:**
- [ ] Kill Service C → A, B vẫn chạy bình thường (loose coupling)
- [ ] Restart Service C → tự recover, không mất frame (fault tolerance)
- [ ] Scale Service A lên 3 instance → Kafka chia partition (scalability)
- [ ] Đo latency end-to-end: sensor event → alert < 5 giây

### 8.3 Cấu hình GCP tối thiểu

```bash
# 1 VM e2-standard-4 (4 vCPU, 16GB RAM) chạy toàn bộ Docker Compose
# Ước tính chi phí: ~$0.15/giờ → ~$3.6/ngày
# Chỉ bật khi demo để tiết kiệm

gcloud compute instances create smart-factory \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --zone=asia-southeast1-a   # Singapore — latency thấp từ VN
```

---

## Tổng kết

Dự án này xây dựng một **Smart Factory Monitoring System** thể hiện đầy đủ các tính chất của hệ thống phân tán:

- **Event-Driven Architecture** qua Apache Kafka — các service hoàn toàn độc lập, giao tiếp bất đồng bộ
- **Fault Tolerance** — hệ thống tự phục hồi khi một service gặp sự cố
- **Horizontal Scalability** — tăng throughput bằng cách thêm instance, không thay đổi code
- **AI/ML Integration** — Isolation Forest cho predictive maintenance, YOLOv8 cho computer vision
- **Cross-source Correlation** — kết hợp sensor + camera để tăng độ tin cậy của alert

Ba nhiệm vụ (Task 1, 2, 3) không tồn tại độc lập mà được tích hợp thành một pipeline thống nhất, trong đó mỗi thành phần có vai trò rõ ràng và có thể phát triển/deploy độc lập.

---

*Tài liệu này là bản phác thảo kiến trúc — chi tiết implementation có thể điều chỉnh trong quá trình phát triển.*