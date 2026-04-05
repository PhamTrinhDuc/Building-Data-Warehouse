# ETL Pipeline - Data Warehouse

**Transform-focused ETL pipeline** chuyển đổi dữ liệu từ hệ thống OLTP (PostgreSQL) sang Data Warehouse (ClickHouse) sử dụng **Star Schema**.

## 📋 Nội dung

- [Tổng quan](#tổng-quan)
- [Kiến trúc ETL](#kiến-trúc-etl)
- [Chi tiết Transform](#chi-tiết-transform)
- [Cài đặt & Chạy](#cài-đặt--chạy)

## Tổng quan

Pipeline **ETL** gồm 3 giai đoạn:

| Giai đoạn | Mô tả | Nguồn | Đầu ra |
|-----------|-------|--------|--------|
| **Extract** | Đọc dữ liệu từ OLTP | PostgreSQL (Schema: `idb`) | Source data dict |
| **Transform** | Chuyển sang Star Schema | Source data | Dimension tables + Fact tables |
| **Load** | Ghi vào Data Warehouse | DW data dict | ClickHouse tables |

**Tệp chính:**
- `extract.py` — Trích xuất dữ liệu từ PostgreSQL
- `transform.py` — **Chuyển đổi + tính toán dữ liệu (FOCUS)**
- `load.py` — Ghi dữ liệu vào ClickHouse
- `run_etl.py` — Chạy pipeline hoàn chỉnh

## Kiến trúc ETL

### Data Flow

```
PostgreSQL (OLTP)
    ↓
[EXTRACT] → Source Tables (dict)
    ↓
[TRANSFORM] → Star Schema (Dimensions + Facts)
    ↓
[LOAD] → ClickHouse (DW)
```

### Star Schema Design

**Dimension Tables (Bảng chiều):**
- `Dim_ThoiGian` — Thời gian
- `Dim_MatHang` — Mặt hàng/Sản phẩm
- `Dim_CuaHang` — Cửa hàng/Điểm bán
- `Dim_KhachHang` — Khách hàng

**Fact Tables (Bảng sự kiện):**
- `Fact_DonDatHang` — Đơn đặt hàng (Orders)
- `Fact_TonKho` — Tồn kho (Inventory)

---

## Chi tiết Transform

### 📍 File: `transform.py`

Hàm chính: `transform_data(source_data)` → trả về dict bảng DW

#### 1️⃣ **Dim_ThoiGian** (Time Dimension)

Tạo bảng chiều thời gian từ tất cả ngày trong khoảng dữ liệu:

```python
def _make_time_key(date_series):
    """Chuyển date sang integer key dạng YYYYMMDD"""
    return pd.to_datetime(date_series).dt.strftime('%Y%m%d').astype(int)
```

**Xử lý:**
- Tìm ngày min/max từ `DonDatHang.ngaydathang` + `MatHang.ngaycapnhat`
- Sinh date range liên tục `[min_date, max_date]`
- Tính toán: `thang`, `quy`, `nam` từ `ngay`

**Schema:**
```
sk_thoiGian (INT) — Time surrogate key (YYYYMMDD)
ngay (DATE)       — Ngày
thang (INT)       — Tháng (1-12)
quy (INT)         — Quý (1-4)
nam (INT)         — Năm
```

**Example:**
```
| sk_thoiGian | ngay       | thang | quy | nam  |
|-------------|------------|-------|-----|------|
| 20250115    | 2025-01-15 | 1     | 1   | 2025 |
| 20250116    | 2025-01-16 | 1     | 1   | 2025 |
```

---

#### 2️⃣ **Dim_MatHang** (Product Dimension)

Chuyển đổi bảng sản phẩm với surrogate key:

**Xử lý:**
- Chuẩn hóa tên cột: `mamh` → `maMH`, `tenmh` → `tenMH`, etc.
- Tạo `sk_matHang` = row index (1-based)
- Giữ thuộc tính: mã, tên, mô tả, kích cỡ, trọng lượng, giá

**Schema:**
```
sk_matHang (INT)    — Surrogate key
maMH (STR)          — Khóa tự nhiên (Product ID)
tenMH (STR)         — Tên sản phẩm
moTa (STR)          — Mô tả
kichCo (STR)        — Kích cỡ
trongLuong (FLOAT)  — Trọng lượng
gia (DECIMAL)       — Giá
ngayCapNhat (DATE)  — Ngày cập nhật
```

---

#### 3️⃣ **Dim_CuaHang** (Store Dimension)

Kết hợp dữ liệu: `CuaHang` + `VanPhongDaiDien` (geography info)

**Xử lý:**
- Join `CuaHang` với `VanPhongDaiDien` để lấy: `tenThanhPho`, `mien`, `diaChiVP`
- Chuẩn hóa cột: `mach` → `maCH`, `sodienthoai` → `soDienThoai`, etc.
- Tạo surrogate key `sk_cuaHang`

**Schema:**
```
sk_cuaHang (INT)      — Surrogate key
maCH (STR)            — Khóa tự nhiên (Store ID)
soDienThoai (STR)     — Số điện thoại
ngayThanhLapCH (DATE) — Ngày thành lập cửa hàng
tenThanhPho (STR)     — Tên thành phố
mien (STR)            — Bang/Tỉnh
diaChiVP (STR)        — Địa chỉ văn phòng đại diện
```

---

#### 4️⃣ **Dim_KhachHang** (Customer Dimension)

**Phức tạp nhất!** Join nhiều bảng con + derive customer type

**Xử lý:**
```
1. Left Join KhachHang với KhachHangDuLich
   → lấy huongDanVien, ngayDangKyDuLich

2. Left Join KhachHang với KhachHangBuuDien
   → lấy diaChiBuuDien, ngayDangKyBuuDien

3. DERIVE loaiKhachHang (Customer Type):
   - Nếu có cả DuLich + BuuDien → 'Ca hai' (Both)
   - Nếu chỉ có DuLich → 'Du lich' (Travel)
   - Nếu chỉ có BuuDien → 'Buu dien' (Mail)
   - Nếu không có → 'Khong phan loai' (Unclassified)
```

**Ứng dụng:** Phân cấp khách hàng cho phân tích hành vi

**Schema:**
```
sk_khachHang (INT)        — Surrogate key
maKH (STR)                — Khóa tự nhiên (Customer ID)
tenKH (STR)               — Tên khách hàng
ngayDatHangDauTien (DATE) — Ngày đặt hàng đầu tiên
huongDanVien (STR)        — Hướng dẫn viên du lịch
diaChiBuuDien (STR)       — Địa chỉ bưu điện
loaiKhachHang (STR)       — Loại khách (Ca hai|Du lich|Buu dien|Khong phan loai)
tenThanhPho (STR)         — Tên thành phố
mien (STR)                — Bang/Tỉnh
```

---

#### 5️⃣ **Fact_DonDatHang** (Order Fact Table)

Bảng sự kiện: **mỗi dòng = 1 loại sản phẩm trong 1 đơn hàng**

**Xử lý:**
```
1. Source: MatHangDuocDat (JOIN DonDatHang)
   ├─ Dữ liệu: maDon, maMH, soLuongDat, giaDat, ngayDatHang, maKH

2. Tính toán:
   ├─ thanhTien = soLuongDat × giaDat
   ├─ sk_thoiGian = _make_time_key(ngayDatHang)
   └─ sk_khachHang, sk_matHang = lookup từ Dim_KhachHang, Dim_MatHang

3. Kết quả: Các cột surrogate keys + measures
```

**Schema:**
```
sk_khachHang (INT)  — FK → Dim_KhachHang
sk_matHang (INT)    — FK → Dim_MatHang
sk_thoiGian (INT)   — FK → Dim_ThoiGian
maDon (STR)         — Khóa tự nhiên (Order ID)
soLuongDat (INT)    — Số lượng đặt
giaDat (DECIMAL)    — Giá đặt (unit price)
thanhTien (DECIMAL) — Tổng tiền = soLuongDat × giaDat ✓
```

**Example:**
```
| sk_khachHang | sk_matHang | sk_thoiGian | maDon | soLuongDat | giaDat | thanhTien |
|--------------|------------|-------------|-------|-----------|--------|-----------|
| 5            | 2          | 20250115    | ĐH001 | 10        | 50000  | 500000    |
| 5            | 3          | 20250115    | ĐH001 | 5         | 75000  | 375000    |
| 8            | 2          | 20250116    | ĐH002 | 20        | 50000  | 1000000   |
```

---

#### 6️⃣ **Fact_TonKho** (Inventory Fact Table)

Bảng sự kiện: **tồn kho tại 1 cửa hàng của 1 sản phẩm**

**Xử lý (phức tạp):**
```
1. Source: MatHangDuocTru
   ├─ Dữ liệu: maMH, maCH, soLuongTonKho, thoiGianNhap

2. Chuyển đổi thoiGianNhap:
   ├─ Input: thoiGianNhap = số ngày trước hôm nay (INT)
   │  Example: 5 = 5 ngày trước
   │
   ├─ Logic: ngayNhap = TODAY - timedelta(days=thoiGianNhap)
   │  Example: TODAY = 2025-01-20
   │           thoiGianNhap = 5
   │           → ngayNhap = 2025-01-15
   │
   └─ Output: sk_thoiGian = _make_time_key(ngayNhap)

3. Lookup surrogate keys:
   ├─ sk_matHang từ Dim_MatHang (maMH)
   └─ sk_cuaHang từ Dim_CuaHang (maCH)
```

**Schema:**
```
sk_cuaHang (INT)       — FK → Dim_CuaHang
sk_matHang (INT)       — FK → Dim_MatHang
sk_thoiGian (INT)      — FK → Dim_ThoiGian (ngày nhập)
soLuongTonKho (INT)    — Số lượng tồn kho (measure)
```

**Example:**
```
| sk_cuaHang | sk_matHang | sk_thoiGian | soLuongTonKho |
|------------|------------|-------------|---------------|
| 1          | 2          | 20250115    | 50            |
| 1          | 3          | 20250115    | 100           |
| 2          | 2          | 20250110    | 75            |
```

---

### 🔑 Surrogate Key Pattern

**Tại sao dùng surrogate key?**
```
✓ Tăng tốc độ queries (int vs string)
✓ Tiết kiệm không gian lưu trữ
✓ Dễ handle schema changes
✓ Chuẩn hóa DW design
```

**Cách tạo:**
```python
# Cách 1: Row index (sử dụng cho Dims)
sk = range(1, len(df) + 1)

# Cách 2: Lookup từ Dims (sử dụng cho Facts)
fact_df = fact_df.merge(
    dim_df[['sk_key', 'natural_key']], 
    on='natural_key', 
    how='left'
)
```

---

### 📊 Transformation Summary

Sau khi transform, pipeline in ra:
```
=== TRANSFORMATION SUMMARY ===
  Dim_ThoiGian         :     73 rows
  Dim_MatHang          :     50 rows
  Dim_CuaHang          :     10 rows
  Dim_KhachHang        :    200 rows
  Fact_DonDatHang      :   1250 rows
  Fact_TonKho          :    500 rows
```

---

## Cài đặt & Chạy

### 📦 Requirements

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `pandas==2.1.4` — Data manipulation
- `sqlalchemy==2.0.25` — PostgreSQL connection
- `psycopg2-binary==2.9.9` — PostgreSQL adapter
- `clickhouse-connect==0.7.0` — ClickHouse client
- `python-dateutil==2.8.2` — Date utilities
- `faker==22.0.0` — Generate test data

### ▶️ Chạy ETL Pipeline

```bash
# Chạy toàn bộ pipeline
python run_etl.py

# Hoặc chạy từng giai đoạn riêng
python -c "from extract import extract_from_postgres; extract_from_postgres()"
python -c "from extract import extract_from_postgres; from transform import transform_data; transform_data(extract_from_postgres())"
python -c "from load import load_to_clickhouse; ..."
```

### 🐳 Docker Environment

**Thường chạy cùng với:**
- PostgreSQL (OLTP) — port 5433
- ClickHouse (DW) — port 8123

Xem `docker-compose.yml` tại root project

---

## 📊 Mapping: IDB → Data Warehouse

### IDB Source Tables (OLTP Schema)

```
┌─────────────────────────────────────────────────────────────┐
│  IDB Database (PostgreSQL - Operational)                    │
├─────────────────────────────────────────────────────────────┤
│ ├─ VanPhongDaiDien (Regional Office)                        │
│ ├─ CuaHang (Store) → FK to VanPhongDaiDien                  │
│ ├─ MatHang (Product)                                        │
│ ├─ MatHangDuocTru (Inventory) → FK to MatHang, CuaHang      │
│ ├─ KhachHang (Customer) → FK to VanPhongDaiDien             │
│ ├─ KhachHangDuiLich (Travel Customer) → FK to KhachHang     │
│ ├─ KhachHangBuiDien (Mail Customer) → FK to KhachHang       │
│ ├─ DonDatHang (Order) → FK to KhachHang                     │
│ └─ MatHangDuocDat (Order Line Item) → FK to MatHang, Order  │
└─────────────────────────────────────────────────────────────┘
```

### Field-by-Field Mapping: IDB → StarSchema DW

#### **Dim_ThoiGian** ◀ [Generated]

| DW Field | Phút | Công thức |
|----------|------|----------|
| `sk_thoiGian` | — | Generated: MIN-MAX(ngayDatHang, ngayCapNhat) → generate daily YYYYMMDD |
| `ngay` | DATE | From MIN-MAX dates |
| `thang` | INT | MONTH(ngay) |
| `quy` | INT | QUARTER(ngay) |
| `nam` | INT | YEAR(ngay) |

**Nhận xét:** ✅ Hợp lý - Bảng chiều được sinh ra bằng cách generate continuous date range

---

#### **Dim_MatHang** ◀ `idb.MatHang`

| DW Field | IDB Source | Kiểu | Chú thích |
|----------|-----------|-------|----------|
| `sk_matHang` | — | INT | Generated surrogate key |
| `maMH` | `MatHang.maMH` | VARCHAR | Natural key (Product ID) |
| `tenMH` | `MatHang.tenMH` | VARCHAR | Product name |
| `moTa` | `MatHang.moTa` | VARCHAR | Description |
| `kichCo` | `MatHang.kichCo` | VARCHAR | Size/dimension |
| `trongLuong` | `MatHang.trongLuong` | FLOAT | Weight |
| `gia` | `MatHang.Gia` | FLOAT | Price (catalog) |
| `ngayCapNhat` | `MatHang.ngayCapNhat` | DATE | Last update |

**Nhận xét:**
- ✅ Direct 1-1 mapping từ MatHang table
- ⚠️ Giá (`gia`) là từ bảng MatHang - đây là giá danh mục/list price, không phải giá đặt hàng thực tế
- ❌ **THIẾU:** Historical price tracking (nếu giá thay đổi, không có record cũ)
- ❌ **THIẾU:** Thông tin loại hàng, category, subcategory
- ❌ **THIẾU:** Nhà cung cấp (supplier info)

---

#### **Dim_CuaHang** ◀ `idb.CuaHang` + `idb.VanPhongDaiDien`

| DW Field | IDB Source | Extract SQL | Chú thích |
|----------|-----------|-----------|----------|
| `sk_cuaHang` | — | — | Generated surrogate key |
| `maCH` | `CuaHang.maCH` | ✓ | Natural key (Store ID) |
| `soDienThoai` | `CuaHang.soDienThoai` | ✓ | Phone number |
| `ngayThanhLapCH` | `CuaHang.ngayThanhLapCH` | ✓ | Store founding date |
| `tenThanhPho` | `VanPhongDaiDien.tenThanhPho` | ✓ | City name (via JOIN) |
| `mien` | `VanPhongDaiDien.mien` | ✓ | State/Province |
| `diaChiVP` | `VanPhongDaiDien.diaChiVP` | ✓ | Office address |

**Extract Logic:**
```sql
SELECT ch.maCH, ch.soDienThoai, ch.ngayThanhLapCH, 
       vp.tenThanhPho, vp.diaChiVP, vp.mien
FROM idb.CuaHang ch
LEFT JOIN idb.VanPhongDaiDien vp 
  ON ch.VanPhongDaiDienmaTP = vp.maTP
```

**Nhận xét:**
- ✅ Geographic info được join sẵn cho easy analytics
- ⚠️ Giữ `VanPhongDaiDien` (office) address, không phải store address
- ❌ **THIẾU:** `VanPhongDaiDien.ngayThanhLapVP` (office founding date) được extract nhưng **KHÔNG** đưa vào DW
- ❌ **THIẾU:** Store address riêng - chỉ có office address
- ❌ **THIẾU:** Manager/contact person info
- ❌ **THIẾU:** Store type (branch, outlet, franchise, etc.)
- 🤔 Tại sao lại join với VanPhongDaiDien? Cửa hàng có địa chỉ riêng không?

---

#### **Dim_KhachHang** ◀ `idb.KhachHang` + 2 Subtypes

| DW Field | IDB Source | JOIN Type | Chú thích |
|----------|-----------|---------|----------|
| `sk_khachHang` | — | — | Generated surrogate key |
| `maKH` | `KhachHang.maKH` | — | Natural key |
| `tenKH` | `KhachHang.tenKH` | — | Customer name |
| `ngayDatHangDauTien` | `KhachHang.ngayDatDauTien` | — | First order date |
| `huongDanVien` | `KhachHangDuiLich.hdvDuLich` | LEFT JOIN | Hướng dẫn viên du lịch (travel guide) |
| `diaChiBuuDien` | `KhachHangBuiDien.diaChiBuuDien` | LEFT JOIN | Địa chỉ bưu điện (mail address) |
| `loaiKhachHang` | [DERIVED] | — | See below |
| `tenThanhPho` | `VanPhongDaiDien.tenThanhPho` | LEFT JOIN | City |
| `mien` | `VanPhongDaiDien.mien` | LEFT JOIN | State |

**Extract & Transform Logic:**
```sql
-- Extract KhachHang with office location
SELECT kh.maKH, kh.tenKH, kh.ngayDatDauTien, 
       vp.tenThanhPho, vp.mien
FROM idb.KhachHang kh
LEFT JOIN idb.VanPhongDaiDien vp 
  ON kh.VanPhongDaiDienmaTP = vp.maTP
```

**Derive loaiKhachHang Logic:**
```python
IF huongDanVien IS NOT NULL AND diaChiBuuDien IS NOT NULL:
    loaiKhachHang = 'Ca hai'           # BOTH types
ELSE IF huongDanVien IS NOT NULL:
    loaiKhachHang = 'Du lich'          # Travel only
ELSE IF diaChiBuuDien IS NOT NULL:
    loaiKhachHang = 'Buu dien'         # Mail only
ELSE:
    loaiKhachHang = 'Khong phan loai'  # Unclassified
```

**Nhận xét:**
- ✅ Customer type derivation hợp lý cho phân cấp
- ✅ Tên cột đã được sửa: `hdvDuLich` (travel guide) và `diaChiBuuDien` (mail address)
- ❌ **THIẾU:** Customer address/location info (chỉ có office location)
- ❌ **THIẾU:** Customer segment/tier (VIP, regular, etc.)
- ❌ **THIẾU:** Contact person, email, phone
- ❌ **THIẾU:** Credit limit, payment terms
- ❌ **THIẾU:** Customer status (active/inactive)
- 🤔 Subtypes như DuiLich/BuiDien là yang sẽ là customer categories hay use cases?

---

#### **Fact_DonDatHang** ◀ `idb.MatHangDuocDat` + Dims lookup

| DW Field | IDB Source | Transform | Chú thích |
|----------|-----------|----------|----------|
| `sk_khachHang` | `MatHangDuocDat.DonDatHang→KhachHang→sk_khachHang` | LOOKUP | FK to Dim_KhachHang |
| `sk_matHang` | `MatHangDuocDat.MatHang→sk_matHang` | LOOKUP | FK to Dim_MatHang |
| `sk_thoiGian` | `MatHangDuocDat.DonDatHang.ngayDatHang` | YYYYMMDD | FK to Dim_ThoiGian |
| `maDon` | `MatHangDuocDat.DonDatHangmaDon` | — | Natural key (Order ID) |
| `soLuongDat` | `MatHangDuocDat.soLuongDat` | — | Order quantity |
| `giaDat` | `MatHangDuocDat.giaDat` | — | Unit price at order time ✓ |
| `thanhTien` | — | **CALCULATED** | `soLuongDat × giaDat` |

**Extract Logic:**
```sql
SELECT mhdd.soLuongDat, mhdd.giaDat,
       mhdd.MatHangmaMH, mhdd.DonDatHangmaDon,
       ddh.ngayDatHang, ddh.KhachHangmaKH
FROM idb.MatHangDuocDat mhdd
INNER JOIN idb.MatHang mh ON mhdd.MatHangmaMH = mh.maMH
INNER JOIN idb.DonDatHang ddh ON mhdd.DonDatHangmaDon = ddh.maDon
```

**Nhận xét:**
- ✅ Tính toán `thanhTien = soLuongDat × giaDat` hợp lý
- ✅ Giá đặt hàng (`giaDat`) được lưu từ thời điểm order → Historical accuracy
- ❌ **THIẾU:** Store info - không biết cửa hàng nào thực hiện đơn hàng
- ❌ **THIẾU:** Discount/promotion applied
- ❌ **THIẾU:** Delivery date, order status, fulfillment info
- ❌ **THIẾU:** Payment info
- ❌ **THIẾU:** Shipping cost
- 🤔 Granularity là 1 sản phẩm/1 đơn. Nên có order-level fact thêm không?

---

#### **Fact_TonKho** ◀ `idb.MatHangDuocTru` + Dims lookup

| DW Field | IDB Source | Transform | Chú thích |
|----------|----------|----------|----------|
| `sk_cuaHang` | `MatHangDuocTru.CuaHang→sk_cuaHang` | LOOKUP | FK to Dim_CuaHang |
| `sk_matHang` | `MatHangDuocTru.MatHang→sk_matHang` | LOOKUP | FK to Dim_MatHang |
| `sk_thoiGian` | `MatHangDuocTru.thoiGianNhap` | **CONVERT** | See below → FK to Dim_ThoiGian |
| `soLuongTonKho` | `MatHangDuocTru.soLuongTrongKho` | — | Current inventory |

**⚠️ CRITICAL Transform Logic:**
```python
# thoiGianNhap = số ngày trước hôm nay (INT)
# Example: thoiGianNhap = 5 → nhập 5 ngày trước

ref_date = TODAY  # e.g., 2025-01-20
ngayNhap = ref_date - timedelta(days=thoiGianNhap)
# If thoiGianNhap = 5 → ngayNhap = 2025-01-15
sk_thoiGian = int(ngayNhap.strftime('%Y%m%d'))
# sk_thoiGian = 20250115
```

**Nhận xét:**
- ✅ Có relationship với store và product
- ⚠️ **DESIGN ISSUE:** `thoiGianNhap` là offset từ hôm nay, làm dữ liệu **dynamic**
  - **Problem:** Report hôm qua sẽ khác hôm nay (vì ref_date thay đổi)
  - **Problem:** Reloading dữ liệu cũ sẽ có ngày khác
  - **Recommendation:** Nên lưu absolute date, không offset
  
- ❌ **MISSING TEMPORAL GRANULARITY:** Chỉ lưu 1 snapshot tại thời điểm nhập
  - Không track daily inventory changes
  - Không biết khi nào hết hàng, khi nào nhập thêm
  - **Recommendation:** Nên có daily inventory snapshots hoặc transaction log
  
- ❌ **MISSING WAREHOUSE INFO:** Inventory table không có warehouse/location detail
  - Chỉ biết cửa hàng nào có bao nhiêu, không biết vị trí kho trong cửa hàng
  
- ❌ **MISSING MEASURES:**
  - Reorder point, economic order quantity
  - Stock status (low, normal, overstock)
  - Cost of inventory

---

## 🔍 Đánh Giá Thiết Kế: Phù Hợp Nghiệp Vụ?

### Điểm Mạnh ✅

| Aspect | Tình trạng | Giải thích |
|--------|----------|----------|
| **Star Schema** | ✅ Tốt | Dễ query, dễ understand, performance tốt |
| **Surrogate Keys** | ✅ Hợp lý | INT keys tốc độ, không phụ thuộc business logic |
| **Fact Granularity** | ✅ OK | 1 dòng = 1 line item trong order → atomic |
| **Time Dimension** | ✅ Tốt | Phủ toàn bộ date range, every day →  temporal analytics |
| **Price Capture** | ✅ Hợp lý | `giaDat` lưu giá lúc order → Historical cost tracking |
| **Customer Segmentation** | ✅ Dễ | `loaiKhachHang` derived để segment analysis |

### Điểm Yếu & Khuyến Nghị ⚠️

#### **1. Thiếu Store (Cửa hàng) trong Order Fact**
```
❌ Hiện tại: Fact_DonDatHang(khachHang, matHang, thoiGian)
✅ Nên có: Fact_DonDatHang(khachHang, matHang, thoiGian, cuaHang)
```
**Tại sao?** Để biết "cửa hàng nào đã bán bao nhiêu"
**Cách fix:** Một là extend Fact_DonDatHang, hoặc tạo Order Header Fact riêng

---

#### **2. Thiếu Địa Chỉ Khách Hàng**
```
❌ Hiện tại: Chỉ có VanPhongDaiDien address
✅ Nên có: Customer physical address, billing/shipping address
```
**Tại sao?** Để phân tích khách hàng theo địa lý, delivery optimization
**Cách fix:** Thêm Cột trong Dim_KhachHang hoặc tạo Dim_Location riêng

---

#### **3. Naming Inconsistency**
```
❌ diaChiBuuDien → Thực tế là hoiDuLich (association name)
✅ Nên đổi thành: hoiBuuDien hoặc diaChiBuuDien (nếu định sửa source)
```

---

#### **4. Inventory Fact KHÔNG Temporal & KHÔNG Replayable**
```
❌ Hiện tại: thoiGianNhap = days offset → DYNAMIC
✅ Nên có: Absolute date (thoiGianNhap = actual DATE)

❌ Hiện tại: 1 record per product-store
✅ Nên có: Daily snapshots để track inventory fluctuations

Example:
  2025-01-10: MatHang 1, CuaHang 1 → 100 units
  2025-01-11: MatHang 1, CuaHang 1 → 95 units (sold 5)
  2025-01-12: MatHang 1, CuaHang 1 → 120 units (restocked)
```

---

#### **5. Chỉ lưu Giá Hiện Tại, Không có Product Master History**
```
❌ Hiện tại: Dim_MatHang.gia = current price
✅ Nên có: Product price history (SCD Type 2)

Nếu giá thay đổi hôm nay:
  - Report hôm qua sẽ sai (vì cost changed)
  - Không thể tính exact profit
```

---

#### **6. Thiếu Order-Level Measures**
```
Fact_DonDatHang có:
  ❌ Discount/promotion
  ❌ Shipping cost
  ❌ Tax
  ❌ Order total (sum of line items)
  ❌ Order status, fulfillment date
  ❌ Payment method, payment date
```

---

#### **7. Dim_CuaHang Không Bao Gồm Store Address**
```
❌ Hiện tại: Chỉ có VanPhongDaiDien.diaChiVP
✅ Nên có: CuaHang.diaChiCH (store's own address)

Vấn đề: Giả sử 1 VanPhongDaiDien quản lý 10 stores
  → Tất cả stores đều có cùng address (office address)
  → Không biết store location thực tế
```

---

#### **8. Thiếu Geographic Hierarchy**
```
Hiện tại chỉ có: City (tenThanhPho) + State (mien)

Nên thêm:
  ├─ Country
  ├─ Region
  ├─ District
  └─ Postal code

Ứng dụng: Sales by Region, Territory management
```

---

### 📋 Recommendation Checklist

**Trước khi production, cần review:**

- [ ] **Order Processing:** Cửa hàng nào xử lý order? (missing in fact)
- [ ] **Inventory Management:** Cần daily snapshots hay chỉ last state?
- [ ] **Product Pricing:** Cần track giá lịch sử hay current price đủ?
- [ ] **Customer Location:** Khách hàng ở đâu? VanPhong or thực tế?
- [ ] **Order Fulfillment:** Cần track status, shipping date, delivery?
- [ ] **Financial Data:** Discount, tax, shipping cost ở đâu?
- [ ] **Data Quality:** Offset-based date (thoiGianNhap) có ổn không?

---

## Notes Kỹ Thuật

### ⚠️ Xử lý NULL & Missing Values

```python
# Dimension tables: chuẩn hóa index
mh['sk_matHang'] = range(1, len(mh) + 1)  # Không có NULL

# Fact tables: **NO NULLS** khi merge with dims
fact_df = fact_df.merge(dim_df, on='key', how='left')
# how='left' → có thể có NULL SK nếu no match
```

### 📅 Time Key Format

```python
# Integer key dạng YYYYMMDD
sk_thoiGian = 20250115  # 15 Jan 2025

# Reverse engineering:
year = 20250115 // 10000      # 2025
month = (20250115 % 10000) // 100  # 01
day = 20250115 % 100          # 15
```

### 🔗 Foreign Key Relationships

```
Fact_DonDatHang:
  ├─ sk_thoiGian → Dim_ThoiGian.sk_thoiGian
  ├─ sk_khachHang → Dim_KhachHang.sk_khachHang
  └─ sk_matHang → Dim_MatHang.sk_matHang

Fact_TonKho:
  ├─ sk_thoiGian → Dim_ThoiGian.sk_thoiGian
  ├─ sk_cuaHang → Dim_CuaHang.sk_cuaHang
  └─ sk_matHang → Dim_MatHang.sk_matHang
```

---

## Troubleshooting

| Vấn đề | Nguyên nhân | Giải pháp |
|--------|-----------|----------|
| `ConnectionRefusedError` (PostgreSQL) | PostgreSQL not running | Chạy `docker-compose up postgres` |
| `clickhouse_connect.driver.exceptions.Error` | ClickHouse not ready | Chạy `docker-compose up clickhouse` |
| NULL surrogate keys trong Facts | Missing dimension records | Kiểm tra data integrity trong Extract |
| Duplicate rows after merge | Khóa không duy nhất | Verify natural keys trong source data |

---

## References

- **Source System:** PostgreSQL OLTP (idb schema)
- **Target System:** ClickHouse DW
- **Schema Pattern:** Kimball's Star Schema
- **Surrogate Keys:** Integer-based (INT) for performance
