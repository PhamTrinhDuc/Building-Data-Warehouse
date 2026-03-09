# Hướng dẫn sử dụng Integrated Database (IDB)

## 📋 Các file đã tạo

1. **03_init_idb.sql** - Script SQL tạo schema và các bảng cho database tích hợp
2. **03_generate_idb_data.py** - Script Python để tạo dữ liệu mô phỏng
3. **requirements_idb.txt** - Các thư viện Python cần thiết

## 🚀 Hướng dẫn sử dụng

### Bước 1: Cài đặt dependencies

```bash
pip install -r requirements_idb.txt
```

### Bước 2: Tạo database schema

Chạy script SQL để tạo các bảng:

```bash
# Nếu dùng PostgreSQL trong Docker
docker exec -i <postgres_container_name> psql -U postgres -d postgres < db/postgres/03_init_idb.sql

# Hoặc nếu PostgreSQL cài local
psql -U postgres -d postgres -f db/postgres/03_init_idb.sql
```

### Bước 3: Điều chỉnh số lượng dữ liệu

Mở file `03_generate_idb_data.py` và chỉnh sửa các biến sau (dòng 12-17):

```python
NUM_VAN_PHONG = 10          # Số văn phòng đại diện
NUM_CUA_HANG = 30           # Số cửa hàng
NUM_MAT_HANG = 100          # Số mặt hàng
NUM_KHACH_HANG = 500        # Số khách hàng
NUM_DON_HANG = 1000         # Số đơn hàng
NUM_MAT_HANG_PER_DON = 3    # Số mặt hàng trung bình mỗi đơn
```

### Bước 4: Cấu hình kết nối database

Chỉnh sửa thông tin kết nối tại dòng 19-25:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}
```

### Bước 5: Chạy script tạo dữ liệu

```bash
cd db/postgres
python 03_generate_idb_data.py
```

## 📊 Cấu trúc Database

Database tích hợp (IDB) bao gồm các bảng sau:

### Bảng chính:
- **VanPhongDaiDien** - Văn phòng đại diện
- **CuaHang** - Cửa hàng
- **MatHang** - Mặt hàng
- **KhachHang** - Khách hàng
- **DonDatHang** - Đơn đặt hàng

### Bảng quan hệ:
- **MatHangDuocTru** - Mặt hàng được trữ tại cửa hàng (tồn kho)
- **MatHangDuocDat** - Mặt hàng trong đơn đặt hàng
- **KhachHangDuiLich** - Khách hàng du lịch
- **KhachHangBuiDien** - Khách hàng bưu điện

## 🔍 Kiểm tra dữ liệu

Sau khi tạo dữ liệu, có thể kiểm tra bằng SQL:

```sql
-- Đặt search path
SET search_path TO idb;

-- Kiểm tra số lượng bản ghi
SELECT 'VanPhongDaiDien' as table_name, COUNT(*) as count FROM VanPhongDaiDien
UNION ALL
SELECT 'CuaHang', COUNT(*) FROM CuaHang
UNION ALL
SELECT 'MatHang', COUNT(*) FROM MatHang
UNION ALL
SELECT 'KhachHang', COUNT(*) FROM KhachHang
UNION ALL
SELECT 'DonDatHang', COUNT(*) FROM DonDatHang
UNION ALL
SELECT 'MatHangDuocTru', COUNT(*) FROM MatHangDuocTru
UNION ALL
SELECT 'MatHangDuocDat', COUNT(*) FROM MatHangDuocDat;
```

## ⚙️ Tùy chỉnh nâng cao

Có thể tùy chỉnh thêm trong script Python:

- **Dữ liệu địa lý**: Sửa danh sách `cities` trong hàm `generate_vanphong_daidien()`
- **Danh mục sản phẩm**: Sửa danh sách `categories` trong hàm `generate_mathang()`
- **Tỷ lệ khách hàng**: Sửa xác suất trong hàm `generate_khachhang()` (dòng 259, 262)

## 🐛 Xử lý lỗi

Nếu gặp lỗi:
- Kiểm tra kết nối database
- Đảm bảo schema `idb` đã được tạo
- Kiểm tra quyền user database
- Xem logs chi tiết trong terminal

## 📝 Ghi chú

- Script tự động xóa dữ liệu cũ trước khi tạo mới
- Dữ liệu được tạo ngẫu nhiên sử dụng thư viện Faker
- Có thể chạy script nhiều lần để tạo lại dữ liệu
