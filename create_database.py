#!/usr/bin/env python3
"""
Script để tạo dữ liệu mô phỏng cho Integrated Database (IDB)

Chức năng:
1. Kiểm tra và tạo database 'idb' nếu chưa tồn tại
2. Tạo schema và tất cả các bảng database
3. Tạo dữ liệu mô phỏng

Cách sử dụng:
  python create_database.py

Điều chỉnh số lượng bản ghi bằng cách thay đổi các biến NUM_* bên dưới.
"""

import psycopg2
from datetime import datetime, timedelta
import random
from faker import Faker

# ============================================
# ĐIỀU CHỈNH SỐ LƯỢNG BẢN GHI TẠI ĐÂY
# ============================================
NUM_VAN_PHONG = 30          # Số văn phòng đại diện
NUM_CUA_HANG = 100        # Số cửa hàng
NUM_MAT_HANG = 5000          # Số mặt hàng
NUM_KHACH_HANG = 5000        # Số khách hàng
NUM_DON_HANG = 20000       # Số đơn hàng
NUM_MAT_HANG_PER_DON = 4    # Số mặt hàng trung bình mỗi đơn

# ============================================
# CẤU HÌNH KẾT NỐI DATABASE
# ============================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'oltp',  # Thay đổi nếu cần
    'user': 'admin',      # Thay đổi nếu cần
    'password': 'admin'   # Thay đổi nếu cần
}

# Khởi tạo Faker cho dữ liệu tiếng Việt
fake = Faker('vi_VN')

def get_connection():
    """Tạo kết nối đến database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return None

def check_database_exists(host, port, user, password, database='oltp'):
    """Kiểm tra xem database đã tồn tại hay chưa"""
    try:
        # Kết nối tới database postgres để kiểm tra
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,  # Kết nối tới database mặc định
            user=user,
            password=password
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database}';")
        exists = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"❌ Lỗi kiểm tra database: {e}")
        return False

def create_idb_database(host, port, user, password, database='idb'):
    """Tạo database 'idb' nếu chưa tồn tại"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database='oltp',
            user=user,
            password=password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Kiểm tra xem database đã tồn tại
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database}';")
        if cursor.fetchone():
            print(f"✅ Database '{database}' đã tồn tại")
        else:
            cursor.execute(f"CREATE DATABASE {database} ENCODING 'UTF8';")
            print(f"✅ Database '{database}' đã được tạo")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Lỗi tạo database: {e}")
        return False

def create_schema_and_tables(conn):
    """Tạo schema và tất cả các tables"""
    cursor = conn.cursor()
    try:
        # Tạo schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS idb;")
        cursor.execute("SET search_path TO idb;")
        
        print("📋 Tạo các bảng...")
        
        # Bảng Văn phòng đại diện
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS VanPhongDaiDien (
                maTP VARCHAR(10) PRIMARY KEY,
                tenThanhPho VARCHAR(255) NOT NULL,
                diaChiVP VARCHAR(255),
                bang VARCHAR(255),
                ngayThanhLapVP DATE
            );
        """)
        
        # Bảng Cửa hàng
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CuaHang (
                maCH VARCHAR(10) PRIMARY KEY,
                soDienThoai VARCHAR(10),
                ngayThanhLapCH DATE,
                VanPhongDaiDienmaTP VARCHAR(10),
                FOREIGN KEY (VanPhongDaiDienmaTP) REFERENCES VanPhongDaiDien(maTP) ON DELETE CASCADE
            );
        """)
        
        # Bảng Mặt hàng
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS MatHang (
                maMH VARCHAR(10) PRIMARY KEY,
                tenMH VARCHAR(255),
                moTa VARCHAR(255),
                kichCo VARCHAR(255),
                trongLuong FLOAT,
                Gia FLOAT,
                ngayCapNhat DATE
            );
        """)
        
        # Bảng Mặt hàng được trữ
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS MatHangDuocTru (
                soLuongTrongKho INTEGER,
                thoiGianNhap INTEGER,
                MatHangmaMH VARCHAR(10),
                CuaHangmaCH VARCHAR(10),
                PRIMARY KEY (MatHangmaMH, CuaHangmaCH),
                FOREIGN KEY (MatHangmaMH) REFERENCES MatHang(maMH) ON DELETE CASCADE,
                FOREIGN KEY (CuaHangmaCH) REFERENCES CuaHang(maCH) ON DELETE CASCADE
            );
        """)
        
        # Bảng Khách hàng
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS KhachHang (
                maKH VARCHAR(10) PRIMARY KEY,
                tenKH VARCHAR(255),
                ngayDatDauTien DATE,
                VanPhongDaiDienmaTP VARCHAR(10),
                FOREIGN KEY (VanPhongDaiDienmaTP) REFERENCES VanPhongDaiDien(maTP) ON DELETE CASCADE
            );
        """)
        
        # Bảng Khách hàng du lịch
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS KhachHangDuiLich (
                hoiDuLich VARCHAR(10),
                ngayDangKy DATE,
                KhachHangmaKH VARCHAR(10) PRIMARY KEY,
                FOREIGN KEY (KhachHangmaKH) REFERENCES KhachHang(maKH) ON DELETE CASCADE
            );
        """)
        
        # Bảng Khách hàng bưu điện
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS KhachHangBuiDien (
                hoiDuLich VARCHAR(10),
                ngayDangKy DATE,
                KhachHangmaKH VARCHAR(10) PRIMARY KEY,
                FOREIGN KEY (KhachHangmaKH) REFERENCES KhachHang(maKH) ON DELETE CASCADE
            );
        """)
        
        # Bảng Đơn đặt hàng
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DonDatHang (
                maDon VARCHAR(10) PRIMARY KEY,
                ngayDatHang DATE,
                KhachHangmaKH VARCHAR(10),
                FOREIGN KEY (KhachHangmaKH) REFERENCES KhachHang(maKH) ON DELETE CASCADE
            );
        """)
        
        # Bảng Mặt hàng được đặt
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS MatHangDuocDat (
                soLuongDat INTEGER,
                giaDat FLOAT,
                MatHangmaMH VARCHAR(10),
                DonDatHangmaDon VARCHAR(10),
                PRIMARY KEY (MatHangmaMH, DonDatHangmaDon),
                FOREIGN KEY (MatHangmaMH) REFERENCES MatHang(maMH) ON DELETE CASCADE,
                FOREIGN KEY (DonDatHangmaDon) REFERENCES DonDatHang(maDon) ON DELETE CASCADE
            );
        """)
        
        # Tạo indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cuahang_vanphong ON CuaHang(VanPhongDaiDienmaTP);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_khachhang_vanphong ON KhachHang(VanPhongDaiDienmaTP);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_donhang_khachhang ON DonDatHang(KhachHangmaKH);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mathang_tru_cuahang ON MatHangDuocTru(CuaHangmaCH);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mathang_dat_donhang ON MatHangDuocDat(DonDatHangmaDon);")
        
        conn.commit()
        print("✅ Schema và các bảng đã được tạo thành công")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi tạo schema/bảng: {e}")
        return False
    finally:
        cursor.close()

def clear_data(conn):
    """Xóa dữ liệu cũ"""
    cursor = conn.cursor()
    try:
        cursor.execute("SET search_path TO idb;")
        
        # Xóa theo thứ tự ngược để tránh lỗi foreign key
        tables = [
            'MatHangDuocDat',
            'DonDatHang',
            'KhachHangBuiDien',
            'KhachHangDuiLich',
            'KhachHang',
            'MatHangDuocTru',
            'MatHang',
            'CuaHang',
            'VanPhongDaiDien'
        ]
        
        for table in tables:
            cursor.execute(f"DELETE FROM {table};")
        
        conn.commit()
        print("✅ Đã xóa dữ liệu cũ")
    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi khi xóa dữ liệu: {e}")
    finally:
        cursor.close()

def generate_vanphong_daidien(conn, num_records):
    """Tạo dữ liệu Văn phòng đại diện"""
    cursor = conn.cursor()
    cities = ['Ha Noi', 'Ho Chi Minh', 'Da Nang', 'Hai Phong', 'Can Tho', 
              'Nha Trang', 'Hue', 'Vung Tau', 'Bien Hoa', 'Buon Ma Thuot', 'Quy Nhon', 'Thai Nguyen', 'Nam Dinh', 'Phan Thiet', 'Rach Gia', 'Long Xuyen']
    
    print(f"📍 Tạo {num_records} văn phòng đại diện...")
    
    for i in range(1, num_records + 1):
        ma_tp = f'TP{i:03d}'
        thanh_pho = random.choice(cities)
        dia_chi = fake.street_address()
        bang = thanh_pho
        ngay_thanh_lap = fake.date_between(start_date='-5y', end_date='today')
        
        cursor.execute("""
            INSERT INTO VanPhongDaiDien (maTP, tenThanhPho, diaChiVP, bang, ngayThanhLapVP)
            VALUES (%s, %s, %s, %s, %s)
        """, (ma_tp, thanh_pho, dia_chi, bang, ngay_thanh_lap))
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} văn phòng đại diện")

def generate_cuahang(conn, num_records, num_vanphong):
    """Tạo dữ liệu Cửa hàng"""
    cursor = conn.cursor()
    
    print(f"🏪 Tạo {num_records} cửa hàng...")
    
    for i in range(1, num_records + 1):
        ma_ch = f'CH{i:03d}'
        so_dien_thoai = fake.phone_number()[:10]
        ngay_thanh_lap = fake.date_between(start_date='-3y', end_date='today')
        ma_vp = f'TP{random.randint(1, num_vanphong):03d}'
        
        cursor.execute("""
            INSERT INTO CuaHang (maCH, soDienThoai, ngayThanhLapCH, VanPhongDaiDienmaTP)
            VALUES (%s, %s, %s, %s)
        """, (ma_ch, so_dien_thoai, ngay_thanh_lap, ma_vp))
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} cửa hàng")

def generate_mathang(conn, num_records):
    """Tạo dữ liệu Mặt hàng"""
    cursor = conn.cursor()
    sizes = ['Nhỏ', 'Vừa', 'Lớn', 'S', 'M', 'L', 'XL', 'XXL', 'Freesize']
    
    print(f"📦 Tạo {num_records} mặt hàng...")
    
    for i in range(1, num_records + 1):
        ma_mh = f'MH{i:04d}'
        ten_mh = fake.word().capitalize()
        mo_ta = fake.catch_phrase()
        kich_co = random.choice(sizes)
        trong_luong = round(random.uniform(0.1, 50.0), 2)
        gia = round(random.uniform(10000, 5000000), 2)
        ngay_cap_nhat = fake.date_between(start_date='-2y', end_date='today')
        
        cursor.execute("""
            INSERT INTO MatHang (maMH, tenMH, moTa, kichCo, trongLuong, Gia, ngayCapNhat)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (ma_mh, ten_mh, mo_ta, kich_co, trong_luong, gia, ngay_cap_nhat))
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} mặt hàng")

def generate_mathang_duoctru(conn, num_mathang, num_cuahang):
    """Tạo dữ liệu Mặt hàng được trữ (tồn kho)"""
    cursor = conn.cursor()
    
    # Mỗi cửa hàng có khoảng 30-50% số mặt hàng
    num_records = int(num_cuahang * num_mathang * 0.4)
    print(f"📊 Tạo {num_records} bản ghi mặt hàng được trữ...")
    
    generated = set()
    count = 0
    
    while count < num_records:
        ma_mh = f'MH{random.randint(1, num_mathang):04d}'
        ma_ch = f'CH{random.randint(1, num_cuahang):03d}'
        
        # Tránh trùng lặp
        if (ma_mh, ma_ch) in generated:
            continue
        
        generated.add((ma_mh, ma_ch))
        
        so_luong = random.randint(0, 1000)
        thoi_gian_nhap = random.randint(1, 365)
        
        try:
            cursor.execute("""
                INSERT INTO MatHangDuocTru (soLuongTrongKho, thoiGianNhap, MatHangmaMH, CuaHangmaCH)
                VALUES (%s, %s, %s, %s)
            """, (so_luong, thoi_gian_nhap, ma_mh, ma_ch))
            count += 1
        except:
            continue
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {count} bản ghi mặt hàng được trữ")

def generate_khachhang(conn, num_records, num_vanphong):
    """Tạo dữ liệu Khách hàng"""
    cursor = conn.cursor()
    
    print(f"👥 Tạo {num_records} khách hàng...")
    
    khach_hang_dulich = []
    khach_hang_buudien = []
    
    for i in range(1, num_records + 1):
        ma_kh = f'KH{i:05d}'
        ten_kh = fake.name()
        ngay_dat_dau_tien = fake.date_between(start_date='-2y', end_date='today')
        ma_vp = f'TP{random.randint(1, num_vanphong):03d}'
        
        cursor.execute("""
            INSERT INTO KhachHang (maKH, tenKH, ngayDatDauTien, VanPhongDaiDienmaTP)
            VALUES (%s, %s, %s, %s)
        """, (ma_kh, ten_kh, ngay_dat_dau_tien, ma_vp))
        
        # 30% là khách hàng du lịch
        if random.random() < 0.3:
            khach_hang_dulich.append(ma_kh)
        # 20% là khách hàng bưu điện
        elif random.random() < 0.5:
            khach_hang_buudien.append(ma_kh)
    
    conn.commit()
    
    # Tạo khách hàng du lịch
    for ma_kh in khach_hang_dulich:
        hoi_du_lich = f'HDL{random.randint(1, 50):03d}'
        ngay_dang_ky = fake.date_between(start_date='-1y', end_date='today')
        cursor.execute("""
            INSERT INTO KhachHangDuiLich (hoiDuLich, ngayDangKy, KhachHangmaKH)
            VALUES (%s, %s, %s)
        """, (hoi_du_lich, ngay_dang_ky, ma_kh))
    
    # Tạo khách hàng bưu điện
    for ma_kh in khach_hang_buudien:
        hoi_du_lich = f'PO{random.randint(1, 100):04d}'
        ngay_dang_ky = fake.date_between(start_date='-1y', end_date='today')
        cursor.execute("""
            INSERT INTO KhachHangBuiDien (hoiDuLich, ngayDangKy, KhachHangmaKH)
            VALUES (%s, %s, %s)
        """, (hoi_du_lich, ngay_dang_ky, ma_kh))
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} khách hàng ({len(khach_hang_dulich)} du lịch, {len(khach_hang_buudien)} bưu điện)")

def generate_donhang(conn, num_records, num_khachhang, num_mathang, num_mh_per_don):
    """Tạo dữ liệu Đơn hàng và Mặt hàng được đặt"""
    cursor = conn.cursor()
    
    print(f"🛒 Tạo {num_records} đơn hàng...")
    
    for i in range(1, num_records + 1):
        ma_don = f'DON{i:06d}'
        ngay_dat_hang = fake.date_between(start_date='-1y', end_date='today')
        ma_kh = f'KH{random.randint(1, num_khachhang):05d}'
        
        try:
            cursor.execute("""
                INSERT INTO DonDatHang (maDon, ngayDatHang, KhachHangmaKH)
                VALUES (%s, %s, %s)
            """, (ma_don, ngay_dat_hang, ma_kh))
            
            # Tạo mặt hàng trong đơn
            num_items = random.randint(1, num_mh_per_don * 2)
            mat_hang_trong_don = set()
            
            for _ in range(num_items):
                ma_mh = f'MH{random.randint(1, num_mathang):04d}'
                
                # Tránh trùng mặt hàng trong cùng đơn
                if ma_mh in mat_hang_trong_don:
                    continue
                
                mat_hang_trong_don.add(ma_mh)
                
                so_luong = random.randint(1, 20)
                gia = round(random.uniform(10000, 5000000), 2)
                
                cursor.execute("""
                    INSERT INTO MatHangDuocDat (soLuongDat, giaDat, MatHangmaMH, DonDatHangmaDon)
                    VALUES (%s, %s, %s, %s)
                """, (so_luong, gia, ma_mh, ma_don))
        
        except Exception as e:
            continue
        
        if i % 100 == 0:
            print(f"  ⏳ Đã tạo {i}/{num_records} đơn hàng...")
            conn.commit()
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} đơn hàng")

def main():
    """Hàm chính"""
    print("=" * 60)
    print("🚀 BẮT ĐẦU KHỞI TẠO DATABASE VÀ DỮ LIỆU MÔ PHỎNG CHO IDB")
    print("=" * 60)
    
    # BƯỚC 1: Tạo database nếu chưa tồn tại
    print("\n[BƯỚC 1] Kiểm tra và tạo database 'idb'...")
    if not create_idb_database(
        DB_CONFIG['host'],
        DB_CONFIG['port'],
        DB_CONFIG['user'],
        DB_CONFIG['password'],
        'idb'
    ):
        print("❌ Không thể tạo database. Dừng lại.")
        return
    
    # BƯỚC 2: Kết nối tới database 'idb'
    print("\n[BƯỚC 2] Kết nối tới database 'idb'...")
    # Cập nhật DB_CONFIG để kết nối tới database 'idb' thay vì 'oltp'
    db_config = DB_CONFIG.copy()
    db_config['database'] = 'idb'
    
    try:
        conn = psycopg2.connect(**db_config)
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return
    
    # BƯỚC 3: Tạo schema và tables
    print("\n[BƯỚC 3] Tạo schema và các bảng...")
    if not create_schema_and_tables(conn):
        print("❌ Không thể tạo schema/tables. Dừng lại.")
        conn.close()
        return
    
    # BƯỚC 4: Tạo dữ liệu mẫu
    print("\n[BƯỚC 4] Tạo dữ liệu mô phỏng...")
    print(f"📊 Cấu hình:")
    print(f"  - Văn phòng đại diện: {NUM_VAN_PHONG}")
    print(f"  - Cửa hàng: {NUM_CUA_HANG}")
    print(f"  - Mặt hàng: {NUM_MAT_HANG}")
    print(f"  - Khách hàng: {NUM_KHACH_HANG}")
    print(f"  - Đơn hàng: {NUM_DON_HANG}")
    print("=" * 60)
    
    try:
        # Đặt search path
        cursor = conn.cursor()
        cursor.execute("SET search_path TO idb;")
        cursor.close()
        
        # Xóa dữ liệu cũ (nếu cần)
        clear_data(conn)
        
        # Tạo dữ liệu mới
        generate_vanphong_daidien(conn, NUM_VAN_PHONG)
        generate_cuahang(conn, NUM_CUA_HANG, NUM_VAN_PHONG)
        generate_mathang(conn, NUM_MAT_HANG)
        generate_mathang_duoctru(conn, NUM_MAT_HANG, NUM_CUA_HANG)
        generate_khachhang(conn, NUM_KHACH_HANG, NUM_VAN_PHONG)
        generate_donhang(conn, NUM_DON_HANG, NUM_KHACH_HANG, NUM_MAT_HANG, NUM_MAT_HANG_PER_DON)
        
        print("=" * 60)
        print("✅ HOÀN THÀNH KHỞI TẠO DATABASE VÀ DỮ LIỆU MÔ PHỎNG!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    main()
