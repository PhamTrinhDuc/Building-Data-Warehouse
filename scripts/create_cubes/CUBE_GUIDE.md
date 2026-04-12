# 📊 Hướng Dẫn Tạo Cube ClickHouse

## 📝 Giới Thiệu

File `create_cubes.py` cung cấp một cách **lập trình** để tạo và quản lý các Cube (aggregation tables) trong ClickHouse, thay vì chạy SQL scripts trực tiếp khi container start.

## 🏗️ Cấu Trúc Cube

```
Cube 0D (0 chiều)
├─ Tổng doanh thu
├─ Tổng số lượng
└─ Không phân chia

Cube 1D (1 chiều)
├─ Theo cửa hàng (maCH)
├─ Theo sản phẩm (maMH)
└─ Theo thời gian (ngayDatHang)

Cube 2D (2 chiều)
├─ Cửa hàng + Sản phẩm
├─ Cửa hàng + Thời gian
└─ Sản phẩm + Thời gian

Cube 3D (3 chiều)
└─ Cửa hàng + Sản phẩm + Thời gian
```

## 📦 Cầu Hình

Chỉnh sửa `CLICKHOUSE_CONFIG` trong file:

```python
CLICKHOUSE_CONFIG = {
    'host': 'localhost',              # ClickHouse sever host
    'port': 9000,                     # ClickHouse native protocol port
    'database': 'default',            # Database name
    'user': 'default',                # Username
    'password': '',                   # Password
    'settings': {
        'allow_experimental_v2_agg_for_initial_query': 1,
    }
}
```

## 🚀 Cách Sử Dụng

### 1. **Cài đặt dependency**

```bash
pip install clickhouse-driver
```

### 2. **Chạy script để tạo các cube**

```bash
python create_cubes.py
```

### 3. **Output mong đợi**

```
✅ Kết nối ClickHouse thành công: localhost:9000
📊 BẮT ĐẦU TẠO CÁC CUBE
========================================================
📊 Tạo Cube_0D (Tổng Doanh Thu & Số Lượng)...
✅ Thực thi thành công
...
========================================================
✅ HOÀN THÀNH TẠO CÁC CUBE
========================================================
```

## ✏️ Viết Cube Của Bạn

Script cung cấp placeholder (`TODO`) cho mỗi cube. Ví dụ:

```python
def create_cube_1d_store(self, drop_if_exists: bool = False) -> None:
    """Tạo Cube 1D: Theo cửa hàng"""
    logger.info("📊 [TODO] Tạo Cube_1D_Store (Theo Cửa Hàng)...")
    
    # ============================================
    # PLACEHOLDER: VIẾT CUBE 1D THEO CỬA HÀNG
    # ============================================
    # TODO: Viết cube của bạn ở đây
    pass
```

### Cách viết Cube (Ví dụ: Cube 1D Store)

```python
def create_cube_1d_store(self, drop_if_exists: bool = False) -> None:
    """Tạo Cube 1D: Theo cửa hàng"""
    logger.info("📊 Tạo Cube_1D_Store (Theo Cửa Hàng)...")
    
    table_name = 'cube_1d_store_storage'
    view_name = 'cube_1d_store'
    
    if drop_if_exists:
        self.execute(f"DROP TABLE IF EXISTS {view_name}")
        self.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Tạo bảng lưu trữ
    create_storage_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        maCH VARCHAR(10),
        tongDoanhThu Float64,
        tongSoLuong Int64,
        soLanDat Int64
    ) ENGINE = SummingMergeTree()
    ORDER BY maCH;
    """
    self.execute(create_storage_sql)
    
    # Tạo materialized view
    create_view_sql = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
    TO {table_name} AS
    SELECT 
        maCH,
        SUM(thanhTien) as tongDoanhThu,
        SUM(soLuongDat) as tongSoLuong,
        COUNT(*) as soLanDat
    FROM Fact_DonDatHang
    GROUP BY maCH;
    """
    self.execute(create_view_sql)
```

## 📋 Danh Sách Cube Cần Viết

| Cube | Hàm | Chiều | Nhóm Theo |
|------|-----|-------|-----------|
| Cube 0D ✅ | `create_cube_0d()` | 0 | (không) |
| Cube 1D Store | `create_cube_1d_store()` | 1 | maCH |
| Cube 1D Product | `create_cube_1d_product()` | 1 | maMH |
| Cube 1D Time | `create_cube_1d_time()` | 1 | toDate(ngayDatHang) |
| Cube 2D Store+Product | `create_cube_2d_store_product()` | 2 | maCH, maMH |
| Cube 2D Store+Time | `create_cube_2d_store_time()` | 2 | maCH, toDate(ngayDatHang) |
| Cube 2D Product+Time | `create_cube_2d_product_time()` | 2 | maMH, toDate(ngayDatHang) |
| Cube 3D Full | `create_cube_3d_store_product_time()` | 3 | maCH, maMH, toDate(ngayDatHang) |

## 🔍 Kiểm Tra Cube Đã Tạo

Sau khi chạy script, kiểm tra trong ClickHouse:

```bash
# Kết nối tới ClickHouse
clickhouse-client -h localhost -p 9000

# Xem các bảng
SHOW TABLES;

# Xem dữ liệu Cube 0D
SELECT * FROM cube_0d;

# Xem dãy tables (storage + views)
SELECT name, engine FROM system.tables WHERE database = 'default' AND name LIKE 'cube%';
```

## 📊 Ví Dụ: Cube 1D theo cửa hàng

```python
# SQL tương đương
CREATE TABLE cube_1d_store_storage (
    maCH VARCHAR(10),
    tongDoanhThu Float64,
    tongSoLuong Int64,
    soLanDat Int64
) ENGINE = SummingMergeTree()
ORDER BY maCH;

CREATE MATERIALIZED VIEW cube_1d_store
TO cube_1d_store_storage AS
SELECT 
    maCH,
    SUM(thanhTien) as tongDoanhThu,
    SUM(soLuongDat) as tongSoLuong,
    COUNT(*) as soLanDat
FROM Fact_DonDatHang
GROUP BY maCH;
```

## 💡 Lưu Ý

1. **SummingMergeTree** - Tự động cộng dồn (sum) các metric. Dùng cho các giá trị có thể cộng.
2. **Materialized View** - Cập nhật tự động khi dữ liệu mới được thêm vào `Fact_DonDatHang`
3. **ORDER BY** - Rất quan trọng để tối ưu truy vấn. Chọn cột được filter nhiều nhất.
4. **drop_if_exists** - Khi set `True`, xóa cube cũ trước khi tạo mới. Dùng khi muốn làm mới.

## 🐛 Troubleshooting

### Lỗi: "Kết nối tới ClickHouse thất bại"
```bash
# Kiểm tra ClickHouse có chạy không
docker ps | grep clickhouse
# Hoặc
clickhouse-client -h localhost -p 9000 --query "SELECT 1"
```

### Lỗi: "Table Fact_DonDatHang not found"
```bash
# Kiểm tra table tồn tại
clickhouse-client --query "SELECT * FROM Fact_DonDatHang LIMIT 1"
```

### Lỗi: "SummingMergeTree column mismatch"
Đảm bảo các column trong `CREATE TABLE` khớp với `SELECT` trong materialized view.

## 📚 Tham Khảo

- **ClickHouse Python Client**: https://github.com/mymarilyn/clickhouse-driver
- **SummingMergeTree**: https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/summingmergetree
- **Materialized Views**: https://clickhouse.com/docs/en/sql-reference/statements/create/view
