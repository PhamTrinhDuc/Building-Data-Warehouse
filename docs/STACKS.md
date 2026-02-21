## Instruction hoàn chỉnh: Mục 5 — Cài đặt các khối dữ liệu

---

## Tổng quan kiến trúc

```
PostgreSQL (OLTP)     Python ETL          ClickHouse (DW)      Superset
─────────────────     ──────────          ───────────────      ────────
schema: vanphong_db → extract.py    →     dim_thoi_gian    →   Dashboard
schema: banhang_db    transform.py        dim_khach_hang       9 OLAP
                      load.py             dim_mat_hang         reports
                      generate_data.py    dim_cua_hang
                                          dim_dia_diem
                                          fact_don_dat_hang
                                          fact_ton_kho
```

---

## Stack công nghệ

| Thành phần | Công nghệ | Lý do |
|------------|-----------|-------|
| OLTP | PostgreSQL | Quen thuộc, nhẹ, đủ cho data nguồn |
| ETL | Python (pandas + SQLAlchemy) | Dễ đọc, dễ debug, dễ giải thích trong báo cáo |
| Data Generator | Python Faker | Sinh hàng triệu bản ghi có phân phối thực tế |
| DW | ClickHouse | Columnar DB, OLAP query nhanh dù data triệu dòng |
| Dashboard | Apache Superset | BI tool open-source, kết nối ClickHouse sẵn, dashboard đẹp |
| Orchestration | Docker Compose | 1 lệnh `docker-compose up` chạy toàn bộ |

---

## Cấu trúc project

```
project/
├── docker-compose.yml
├── db/
│   ├── postgres/
│   │   ├── init_vanphong_db.sql     # DDL + data mẫu DB văn phòng
│   │   └── init_banhang_db.sql      # DDL + data mẫu DB bán hàng
│   └── clickhouse/
│       └── init_dw.sql              # DDL tạo Dim + Fact tables
├── etl/
│   ├── generate_data.py             # Sinh data mẫu hàng triệu dòng
│   ├── extract.py                   # Đọc từ Postgres OLTP
│   ├── transform.py                 # Xử lý logic, SCD, surrogate key
│   ├── load.py                      # Ghi vào ClickHouse DW
│   ├── run_etl.py                   # Entry point chạy toàn bộ pipeline
│   └── requirements.txt
├── superset/
│   └── dashboards/                  # Export config dashboard
└── report/
    └── BTL_Report.docx
```

---

## Docker Compose

```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: oltp
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin
    volumes:
      - ./db/postgres:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    volumes:
      - ./db/clickhouse:/docker-entrypoint-initdb.d
    ports:
      - "8123:8123"   # HTTP interface
      - "9000:9000"   # Native interface

  etl:
    build: ./etl
    depends_on:
      - postgres
      - clickhouse
    command: python run_etl.py

  superset:
    image: apache/superset:latest
    ports:
      - "8088:8088"
    depends_on:
      - clickhouse
```

---

## Thứ tự thực hiện

```
1. OLTP có data
       ↓
2. DW schema rỗng
       ↓
3. ETL chạy
       ↓
4. DW có data
       ↓
5. Superset kết nối DW → tạo dashboard
```

---

### Bước 1 — OLTP có data

Viết 2 file SQL tạo schema + insert data mẫu vào Postgres. Data mẫu ở mức vừa đủ để ETL có nghĩa:

```
vanphong_db:
├── ~30 khách hàng du lịch
├── ~30 khách hàng bưu điện
└── ~10 khách hàng cả hai loại

banhang_db:
├── 5 thành phố, 3 bang
├── 15 cửa hàng
├── 50 mặt hàng
├── 200 đơn hàng
└── tồn kho cho từng cửa hàng
```

Data OLTP **không cần lớn** — vì hàng triệu bản ghi sẽ được sinh ở bước tiếp theo.

---

### Bước 2 — Sinh data hàng triệu bản ghi

Đây là điểm khác biệt quan trọng — data lớn không đến từ OLTP mà được **generate trực tiếp vào Fact table** dựa trên Dimension đã có:

```python
# generate_data.py
# Logic: mỗi khách hàng đặt hàng nhiều lần trong 2 năm
# 1000 KH × trung bình 500 đơn/KH × 3 mặt hàng/đơn
# = ~1.5 triệu dòng trong FACT_DonDatHang

from faker import Faker
import random
import pandas as pd

def generate_orders(customers, products, stores, n_months=24):
    rows = []
    for kh in customers:
        # Mỗi KH đặt hàng ngẫu nhiên 200-800 lần
        n_orders = random.randint(200, 800)
        for _ in range(n_orders):
            n_items = random.randint(1, 5)
            for _ in range(n_items):
                rows.append({
                    'ma_kh': kh['ma_kh'],
                    'ma_mh': random.choice(products)['ma_mh'],
                    'ma_ch': random.choice(stores)['ma_ch'],
                    'ngay_dat': fake.date_between('-2y', 'today'),
                    'so_luong': random.randint(1, 10),
                    'gia_dat': random.uniform(50000, 5000000)
                })
    return pd.DataFrame(rows)
```

---

### Bước 3 — DDL tạo DW schema trong ClickHouse

```sql
-- ClickHouse syntax
CREATE TABLE dim_thoi_gian (
    sk_thoi_gian  UInt32,
    ngay          Date,
    thang         UInt8,
    quy           UInt8,
    nam           UInt16,
    thu           String
) ENGINE = MergeTree()
ORDER BY sk_thoi_gian;

CREATE TABLE dim_khach_hang (
    sk_khach_hang    UInt32,
    ma_kh            String,
    ten_kh           String,
    loai_kh          String,   -- 'DuLich' | 'BuuDien' | 'CaHai'
    huong_dan_vien   Nullable(String),
    dia_chi_buu_dien Nullable(String),
    ngay_dat_dau     Date,
    ma_tp            String
) ENGINE = MergeTree()
ORDER BY sk_khach_hang;

CREATE TABLE fact_don_dat_hang (
    sk_thoi_gian  UInt32,
    sk_khach_hang UInt32,
    sk_mat_hang   UInt32,
    sk_cua_hang   UInt32,
    ma_don        String,
    so_luong_dat  UInt32,
    gia_dat       Float64,
    thanh_tien    Float64
) ENGINE = MergeTree()
ORDER BY (sk_thoi_gian, sk_khach_hang, sk_mat_hang);
```

---

### Bước 4 — ETL Pipeline

**extract.py** — đọc từ Postgres, không transform gì:
```python
from sqlalchemy import create_engine
import pandas as pd

pg_engine = create_engine('postgresql://admin:admin@postgres/oltp')

def extract():
    khach_hang = pd.read_sql('SELECT * FROM vanphong_db.khachhang', pg_engine)
    kh_dulich  = pd.read_sql('SELECT * FROM vanphong_db.khachhang_dulich', pg_engine)
    kh_buudien = pd.read_sql('SELECT * FROM vanphong_db.khachhang_buudien', pg_engine)
    cuahang    = pd.read_sql('SELECT * FROM banhang_db.cuahang', pg_engine)
    vanphong   = pd.read_sql('SELECT * FROM banhang_db.vanphongdaidien', pg_engine)
    mathang    = pd.read_sql('SELECT * FROM banhang_db.mathang', pg_engine)
    return khach_hang, kh_dulich, kh_buudien, cuahang, vanphong, mathang
```

**transform.py** — xử lý logic phức tạp:
```python
def transform_dim_khachhang(khach_hang, kh_dulich, kh_buudien):
    # Xác định loại KH
    du_lich_ids  = set(kh_dulich['ma_kh'])
    buu_dien_ids = set(kh_buudien['ma_kh'])

    def get_loai(ma_kh):
        in_dl = ma_kh in du_lich_ids
        in_bd = ma_kh in buu_dien_ids
        if in_dl and in_bd: return 'CaHai'
        if in_dl:            return 'DuLich'
        if in_bd:            return 'BuuDien'

    khach_hang['loai_kh'] = khach_hang['ma_kh'].apply(get_loai)

    # Merge thêm thông tin HDV và địa chỉ bưu điện
    df = khach_hang.merge(kh_dulich[['ma_kh','huong_dan_vien']],
                          on='ma_kh', how='left')
    df = df.merge(kh_buudien[['ma_kh','dia_chi_buu_dien']],
                  on='ma_kh', how='left')

    # Generate surrogate key
    df['sk_khach_hang'] = range(1, len(df)+1)
    return df

def transform_dim_cuahang(cuahang, vanphong):
    # Denormalize VP vào CuaHang
    df = cuahang.merge(vanphong[['ma_tp','ten_tp','bang','dia_chi_vp']],
                       on='ma_tp', how='left')
    df['sk_cua_hang'] = range(1, len(df)+1)
    return df

def generate_dim_thoigian(start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq='D')
    df = pd.DataFrame({
        'ngay': dates,
        'thang': dates.month,
        'quy': dates.quarter,
        'nam': dates.year,
        'thu': dates.day_name()
    })
    df['sk_thoi_gian'] = range(1, len(df)+1)
    return df
```

**load.py** — ghi vào ClickHouse, **Dimension trước Fact sau**:
```python
from clickhouse_driver import Client

ch_client = Client('clickhouse')

def load_dimension(df, table_name):
    ch_client.insert_dataframe(f'INSERT INTO {table_name} VALUES', df)

def load():
    # Thứ tự load quan trọng
    load_dimension(dim_thoigian,  'dim_thoi_gian')
    load_dimension(dim_diadiem,   'dim_dia_diem')
    load_dimension(dim_mathang,   'dim_mat_hang')
    load_dimension(dim_cuahang,   'dim_cua_hang')
    load_dimension(dim_khachhang, 'dim_khach_hang')

    # Fact sau cùng
    load_dimension(fact_dondathang, 'fact_don_dat_hang')
    load_dimension(fact_tonkho,     'fact_ton_kho')
```

---

### Bước 5 — Kiểm tra data đã vào DW

```sql
-- Chạy trong ClickHouse, chụp màn hình dán vào báo cáo
SELECT 'dim_thoi_gian'     AS bang, COUNT(*) AS so_dong FROM dim_thoi_gian
UNION ALL
SELECT 'dim_khach_hang',            COUNT(*) FROM dim_khach_hang
UNION ALL
SELECT 'dim_mat_hang',              COUNT(*) FROM dim_mat_hang
UNION ALL
SELECT 'dim_cua_hang',              COUNT(*) FROM dim_cua_hang
UNION ALL
SELECT 'fact_don_dat_hang',         COUNT(*) FROM fact_don_dat_hang
UNION ALL
SELECT 'fact_ton_kho',              COUNT(*) FROM fact_ton_kho
```

Kết quả mong đợi:
```
dim_thoi_gian       730 dòng  (2 năm)
dim_khach_hang      70  dòng
dim_mat_hang        50  dòng
dim_cua_hang        15  dòng
fact_don_dat_hang   1,500,000+ dòng
fact_ton_kho        500,000+   dòng
```

---

## Phân công nhóm 3 người

| Member | Phụ trách |
|--------|-----------|
| **Member 1** | `init_oltp.sql` (2 schema Postgres) + `generate_data.py` + viết báo cáo mục 1,2,3 |
| **Member 2** | `init_dw.sql` (ClickHouse schema) + `extract.py` + `transform.py` + `load.py` |
| **Member 3** | `docker-compose.yml` + Superset setup + 9 OLAP queries + dashboard |
