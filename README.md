# Data Warehouse Project - PTIT KPDL

Hệ thống Data Warehouse với kiến trúc OLTP → ETL → DW → BI Dashboard

## 📋 Kiến trúc hệ thống

```
PostgreSQL (OLTP)     →     Python ETL     →     ClickHouse (DW)     →     Superset
─────────────────           ──────────           ───────────────           ────────
- vanphong_db                extract.py           Dimension Tables          Dashboard
- banhang_db                 transform.py         Fact Tables               OLAP Reports
                             load.py
```

## 🔧 Stack công nghệ

| Thành phần | Công nghệ | Port | Mô tả |
|------------|-----------|------|-------|
| **OLTP** | PostgreSQL 15 | 5432 | Database nguồn (2 schemas) |
| **ETL** | Python 3.11 | - | Extract, Transform, Load |
| **DW** | ClickHouse | 8123, 9000 | Data Warehouse (Star Schema) |
| **BI** | Apache Superset | 8088 | Dashboard & OLAP Reports |

## 📁 Cấu trúc project

```
PTIT-KPDL/
├── docker-compose.yml          # Orchestration toàn bộ services
├── create_database.py          # Script tạo database (optional)
│
├── db/                         # Database initialization scripts
│   ├── postgres/
│   │   ├── 01_init_vanphong_db.sql     # Schema Văn phòng đại diện
│   │   └── 02_init_banhang_db.sql      # Schema Bán hàng
│   └── clickhouse/
│       └── init_dw.sql                  # Data Warehouse schema
│
├── etl/                        # ETL Pipeline
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── extract.py              # Đọc từ PostgreSQL
│   ├── transform.py            # Chuyển đổi sang Star Schema
│   ├── load.py                 # Ghi vào ClickHouse
│   └── run_etl.py              # Entry point
│
└── docs/                       # Documentation
    ├── INSTRUCTION.md
    └── STACKS.md
```

## 🚀 Hướng dẫn sử dụng

### 1. Yêu cầu hệ thống

- Docker & Docker Compose
- 4GB RAM trở lên
- 10GB disk space

### 2. Khởi động hệ thống

```bash
# Clone/navigate to project
cd PTIT-KPDL

# Khởi động tất cả services
docker-compose up -d

# Xem logs
docker-compose logs -f

# Xem logs của một service cụ thể
docker-compose logs -f etl
docker-compose logs -f postgres
docker-compose logs -f clickhouse
```

### 3. Kiểm tra services

```bash
# Kiểm tra trạng thái
docker-compose ps

# Tất cả services phải ở trạng thái "running" hoặc "exited (0)" cho ETL
```

### 4. Truy cập các services

#### PostgreSQL (OLTP)
```bash
# Kết nối qua psql
docker exec -it postgres_oltp psql -U admin -d oltp

# Truy vấn dữ liệu
\c oltp
SET search_path TO vanphong_db;
SELECT * FROM khachhang LIMIT 5;

SET search_path TO banhang_db;
SELECT * FROM dondathang LIMIT 5;
```

#### ClickHouse (Data Warehouse)
```bash
# Kết nối qua clickhouse-client
docker exec -it clickhouse_dw clickhouse-client

# Truy vấn Data Warehouse
SHOW TABLES;
SELECT * FROM dim_khach_hang LIMIT 5;
SELECT * FROM fact_don_dat_hang LIMIT 5;
```

Hoặc qua HTTP API:
```bash
# Query qua curl
curl 'http://localhost:8123/' --data 'SELECT COUNT(*) FROM dim_khach_hang'
```

#### Apache Superset
```bash
# Truy cập: http://localhost:8088

# Lần đầu cần setup:
docker exec -it superset_dashboard superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@superset.com \
    --password admin

docker exec -it superset_dashboard superset db upgrade
docker exec -it superset_dashboard superset init
```

### 5. Chạy lại ETL

```bash
# Chạy lại ETL pipeline
docker-compose restart etl

# Hoặc chạy manual
docker-compose run --rm etl python run_etl.py
```

### 6. Dừng hệ thống

```bash
# Dừng tất cả services
docker-compose down

# Dừng và xóa volumes (dữ liệu)
docker-compose down -v

# Dừng và xóa images
docker-compose down --rmi all
```

## 📊 Data Warehouse Schema

### Dimension Tables

1. **dim_thoi_gian** - Dimension thời gian (2024-2026)
   - thoi_gian_key (PK), ngay_day_du, ngay, thang, quy, nam, ...

2. **dim_khach_hang** - Dimension khách hàng (SCD Type 2)
   - khach_hang_key (PK), ma_khachhang, ten_khachhang, loai_khach_hang, ...

3. **dim_mat_hang** - Dimension mặt hàng
   - mat_hang_key (PK), ma_mathang, ten_mathang, danh_muc, don_gia, ...

4. **dim_cua_hang** - Dimension cửa hàng
   - cua_hang_key (PK), ma_cuahang, ten_cuahang, ma_vanphong, ...

5. **dim_dia_diem** - Dimension địa điểm
   - dia_diem_key (PK), thanh_pho, bang, vung_mien, ...

### Fact Tables

1. **fact_don_dat_hang** - Fact đơn đặt hàng
   - don_hang_key (PK), khach_hang_key (FK), mat_hang_key (FK), 
     cua_hang_key (FK), thoi_gian_dat_hang_key (FK), so_luong, thanh_tien, ...

2. **fact_ton_kho** - Fact tồn kho
   - ton_kho_key (PK), mat_hang_key (FK), cua_hang_key (FK),
     thoi_gian_key (FK), so_luong_ton, ...

## 🔍 Truy vấn OLAP mẫu

### 1. Tổng doanh thu theo tháng
```sql
SELECT 
    dt.thang, 
    dt.nam,
    SUM(f.thanh_tien) as tong_doanh_thu
FROM fact_don_dat_hang f
JOIN dim_thoi_gian dt ON f.thoi_gian_dat_hang_key = dt.thoi_gian_key
GROUP BY dt.thang, dt.nam
ORDER BY dt.nam, dt.thang;
```

### 2. Top sản phẩm bán chạy
```sql
SELECT 
    mh.ten_mathang,
    mh.danh_muc,
    SUM(f.so_luong) as tong_so_luong,
    SUM(f.thanh_tien) as tong_tien
FROM fact_don_dat_hang f
JOIN dim_mat_hang mh ON f.mat_hang_key = mh.mat_hang_key
GROUP BY mh.ten_mathang, mh.danh_muc
ORDER BY tong_tien DESC
LIMIT 10;
```

### 3. Phân tích khách hàng theo loại
```sql
SELECT 
    kh.loai_khach_hang,
    COUNT(DISTINCT f.khach_hang_key) as so_khach_hang,
    SUM(f.thanh_tien) as tong_doanh_thu
FROM fact_don_dat_hang f
JOIN dim_khach_hang kh ON f.khach_hang_key = kh.khach_hang_key
WHERE kh.la_hien_tai = 1
GROUP BY kh.loai_khach_hang
ORDER BY tong_doanh_thu DESC;
```

### 4. Tồn kho theo cửa hàng
```sql
SELECT 
    ch.ten_cuahang,
    ch.thanh_pho_cuahang,
    mh.ten_mathang,
    f.so_luong_ton
FROM fact_ton_kho f
JOIN dim_cua_hang ch ON f.cua_hang_key = ch.cua_hang_key
JOIN dim_mat_hang mh ON f.mat_hang_key = mh.mat_hang_key
WHERE f.so_luong_ton > 0
ORDER BY ch.ten_cuahang, f.so_luong_ton DESC;
```

## 🐛 Troubleshooting

### ETL fails với "Connection refused"
```bash
# Kiểm tra PostgreSQL đã sẵn sàng
docker-compose logs postgres | grep "ready to accept connections"

# Kiểm tra ClickHouse đã sẵn sàng
docker-compose logs clickhouse | grep "Ready for connections"

# Chạy lại ETL
docker-compose restart etl
```

### PostgreSQL không khởi động
```bash
# Xem logs chi tiết
docker-compose logs postgres

# Xóa volume và khởi động lại
docker-compose down -v
docker-compose up -d postgres
```

### ClickHouse không khởi động
```bash
# Xem logs
docker-compose logs clickhouse

# Kiểm tra quyền truy cập file
ls -la db/clickhouse/

# Restart
docker-compose restart clickhouse
```

## 📝 TODO / Mở rộng

- [ ] Thêm data generator để tạo dữ liệu lớn (hàng triệu records)
- [ ] Thêm scheduler (Airflow) để chạy ETL định kỳ
- [ ] Thêm monitoring với Prometheus & Grafana
- [ ] Tạo dashboard mẫu trong Superset
- [ ] Viết unit tests cho ETL
- [ ] Thêm data quality checks
- [ ] Implement incremental load thay vì full load

## 📚 Tài liệu tham khảo

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Apache Superset Documentation](https://superset.apache.org/docs/intro)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## 👥 Thành viên nhóm

- [Thêm thông tin thành viên]

## 📄 License

Project học tập - PTIT KPDL
