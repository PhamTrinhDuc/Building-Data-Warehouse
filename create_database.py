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
import psycopg2.extras
from datetime import datetime, timedelta
import random
from faker import Faker

# ============================================
# ĐIỀU CHỈNH SỐ LƯỢNG BẢN GHI TẠI ĐÂY
# ============================================
NUM_VAN_PHONG = 80          # Số văn phòng đại diện
NUM_CUA_HANG = 300        # Số cửa hàng
NUM_MAT_HANG = 2000          # Số mặt hàng
NUM_KHACH_HANG = 5000        # Số khách hàng
NUM_DON_HANG = 10000       # Số đơn hàng
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
                mien VARCHAR(255),
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
                thoiGianNhap DATE,
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
                hdvDuLich VARCHAR(10),
                ngayDangKy DATE,
                KhachHangmaKH VARCHAR(10) PRIMARY KEY,
                FOREIGN KEY (KhachHangmaKH) REFERENCES KhachHang(maKH) ON DELETE CASCADE
            );
        """)
        
        # Bảng Khách hàng bưu điện
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS KhachHangBuiDien (
                diaChiBuuDien VARCHAR(10),
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
    
    mien_map = {
        'Ha Noi': 'Mien Bac', 'Hai Phong': 'Mien Bac', 'Thai Nguyen': 'Mien Bac', 'Nam Dinh': 'Mien Bac',
        'Da Nang': 'Mien Trung', 'Nha Trang': 'Mien Trung', 'Hue': 'Mien Trung', 'Quy Nhon': 'Mien Trung', 'Phan Thiet': 'Mien Trung', 'Buon Ma Thuot': 'Mien Trung',
        'Ho Chi Minh': 'Mien Nam', 'Can Tho': 'Mien Nam', 'Vung Tau': 'Mien Nam', 'Bien Hoa': 'Mien Nam', 'Rach Gia': 'Mien Nam', 'Long Xuyen': 'Mien Nam'
    }

    print(f"📍 Tạo {num_records} văn phòng đại diện...")
    
    start_date_vp = datetime(2020, 1, 1).date()
    
    for i in range(1, num_records + 1):
        ma_tp = f'TP{i:03d}'
        thanh_pho = cities[i % len(cities)]
        dia_chi = fake.street_address()
        mien = mien_map[thanh_pho]
        ngay_thanh_lap = start_date_vp + timedelta(days=(i * 17) % 1000)
        
        cursor.execute("""
            INSERT INTO VanPhongDaiDien (maTP, tenThanhPho, diaChiVP, mien, ngayThanhLapVP)
            VALUES (%s, %s, %s, %s, %s)
        """, (ma_tp, thanh_pho, dia_chi, mien, ngay_thanh_lap))
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} văn phòng đại diện")

def generate_cuahang(conn, num_records, num_vanphong):
    """Tạo dữ liệu Cửa hàng"""
    cursor = conn.cursor()
    
    print(f"🏪 Tạo {num_records} cửa hàng...")
    
    start_date_ch = datetime(2021, 1, 1).date()
    
    for i in range(1, num_records + 1):
        ma_ch = f'CH{i:03d}'
        so_dien_thoai = fake.phone_number()[:10]
        ngay_thanh_lap = start_date_ch + timedelta(days=(i * 23) % 1000)
        ma_vp = f'TP{(i % num_vanphong) + 1:03d}'
        
        cursor.execute("""
            INSERT INTO CuaHang (maCH, soDienThoai, ngayThanhLapCH, VanPhongDaiDienmaTP)
            VALUES (%s, %s, %s, %s)
        """, (ma_ch, so_dien_thoai, ngay_thanh_lap, ma_vp))
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} cửa hàng")

def generate_product_name():
    """Tạo tên mặt hàng logic dựa trên loại sản phẩm"""
    # Danh sách loại sản phẩm
    categories = {
        'Áo': {
            'types': ['phông', 'sơ mi', 'khoác', 'cardigan', 'áo lót'],
            'features': ['cotton 100%', 'vải lụa', 'quick-dry', 'thấm hút mồ hôi'],
            'target': ['nam', 'nữ', 'trẻ em'],
            'style': ['casual', 'thể thao', 'chuyên dụng', 'công sở'],
            'price_range': (50000, 500000)
        },
        'Quần': {
            'types': ['jean', 'kaki', 'shorts', 'legging', 'tây vải'],
            'features': ['co giãn', 'chống nhăn', 'thấm hút', 'bền bỉ'],
            'target': ['nam', 'nữ', 'trẻ em'],
            'style': ['casual', 'công sở', 'thể thao'],
            'price_range': (100000, 800000)
        },
        'Giày': {
            'types': ['sneaker', 'giày lười', 'boot', 'sandal', 'giày da'],
            'features': ['lót mềm', 'chống trơn', 'thoáng khí', 'nhẹ nhàng'],
            'target': ['nam', 'nữ', 'trẻ em'],
            'style': ['thể thao', 'casual', 'công sở'],
            'price_range': (200000, 2000000)
        },
        'Phụ kiện': {
            'types': ['túi', 'thắt lưng', 'nón', 'khăn', 'đồng hồ', 'kính'],
            'features': ['bền bỉ', 'đa năng', 'cao cấp', 'thời trang'],
            'target': ['nam', 'nữ', 'unisex'],
            'style': ['casual', 'công sở', 'sang trọng'],
            'price_range': (50000, 3000000)
        },
        'Đồ lót': {
            'types': ['quần lót', 'áo lót', 'tất', 'áo ngủ'],
            'features': ['cotton', 'thoáng khí', 'co giãn', 'mềm mại'],
            'target': ['nam', 'nữ'],
            'style': ['thường ngày', 'cao cấp'],
            'price_range': (30000, 300000)
        },
        'Thể thao': {
            'types': ['áo tập', 'quần tập', 'túi xách', 'nón thể thao', 'giày chạy'],
            'features': ['quick-dry', 'co giãn', 'nhẹ', 'thoáng khí'],
            'target': ['nam', 'nữ', 'unisex'],
            'style': ['chuyên dụng', 'casual'],
            'price_range': (150000, 2000000)
        },
        'Túi xách': {
            'types': ['balo', 'túi tote', 'túi công sở', 'túi du lịch', 'ví cầm tay'],
            'features': ['chống nước', 'chất liệu cao cấp', 'ngăn đa năng', 'độc đáo'],
            'target': ['nam', 'nữ', 'unisex'],
            'style': ['casual', 'công sở', 'sang trọng', 'thể thao'],
            'price_range': (200000, 3000000)
        },
        'Mũ Nón': {
            'types': ['mũ lưỡi trai', 'nón len', 'nón phớt', 'mũ beanie', 'mũ bucket'],
            'features': ['chống tia UV', 'thoáng khí', 'dễ giặt', 'bền bỉ'],
            'target': ['nam', 'nữ', 'trẻ em', 'unisex'],
            'style': ['casual', 'thể thao', 'du lịch', 'thời trang'],
            'price_range': (50000, 500000)
        },
        'Trang sức': {
            'types': ['vòng tay', 'dây chuyền', 'vành tai', 'nhẫn', 'lắc chân'],
            'features': ['bạc 925', 'mạ vàng', 'đá quý', 'thiết kế tinh tế'],
            'target': ['nam', 'nữ', 'unisex'],
            'style': ['cổ điển', 'hiện đại', 'sang trọng', 'tối giản'],
            'price_range': (100000, 5000000)
        },
        'Chăn Ga Gối': {
            'types': ['chăn ga', 'gối nằm', 'gối ôm', 'túi chăn', 'drap giường'],
            'features': ['cotton 100%', 'nano', 'chống dị ứng', 'thân thiện da'],
            'target': ['gia đình', 'người lớn', 'trẻ em', 'unisex'],
            'style': ['cổ điển', 'hiện đại', 'sang trọng'],
            'price_range': (150000, 2000000)
        },
        'Mỹ phẩm': {
            'types': ['kem dưỡng', 'sữa rửa mặt', 'toner', 'serum', 'dưỡng thể'],
            'features': ['organic', 'không hóa chất', 'dưỡng ẩm sâu', 'chính hãng'],
            'target': ['nam', 'nữ', 'da nhạy cảm', 'da dầu'],
            'style': ['tự nhiên', 'cao cấp', 'chuyên dụng'],
            'price_range': (100000, 2000000)
        },
        'Khăn': {
            'types': ['khăn tắm', 'khăn mặt', 'khăn quàng cổ', 'khăn lạnh', 'khăn đa năng'],
            'features': ['thấm hút tốt', 'mềm mại', 'nhanh khô', 'không xù'],
            'target': ['gia đình', 'nam', 'nữ', 'bé'],
            'style': ['casual', 'cao cấp', 'thể thao'],
            'price_range': (30000, 800000)
        },
        'Quần lót': {
            'types': ['quần gen', 'quần bơi', 'quần biker', 'quần tai cá sấu'],
            'features': ['co giãn cao', 'thoáng khí', 'form chuẩn', 'Spandex'],
            'target': ['nam', 'nữ'],
            'style': ['casual', 'thể thao', 'cao cấp'],
            'price_range': (50000, 400000)
        },
        'Đồ Bơi': {
            'types': ['áo tắm', 'quần bơi', 'bộ bơi', 'áo lội', 'áo chống nắng'],
            'features': ['quick-dry', 'chống UVA/UVB', 'liền mình', 'hoa văn đẹp'],
            'target': ['nam', 'nữ', 'trẻ em', 'unisex'],
            'style': ['thể thao', 'sang trọng', 'casual'],
            'price_range': (150000, 1500000)
        },
        'Tất Vớ': {
            'types': ['tất thể thao', 'tất công sở', 'tất nylon', 'tất lạnh', 'tất len'],
            'features': ['cotton tự nhiên', 'co giãn vừa phải', 'thoáng khí', 'chống tồn'],
            'target': ['nam', 'nữ', 'trẻ em'],
            'style': ['casual', 'công sở', 'thể thao'],
            'price_range': (20000, 200000)
        }
    }
    
    # Chọn loại sản phẩm
    category = random.choice(list(categories.keys()))
    cat_info = categories[category]
    
    # Tạo tên
    product_type = random.choice(cat_info['types'])
    feature = random.choice(cat_info['features'])
    target = random.choice(cat_info['target'])
    style = random.choice(cat_info['style'])
    
    # Tạo tên sản phẩm logic: "Loại + Đặc điểm + Đối tượng + Phong cách"
    # Ví dụ: "Áo phông cotton nam casual", "Quần jean co giãn nữ công sở"
    names = [
        f"{category} {product_type} {feature} {target} {style}",
        f"{category} {product_type} {target} {style} - {feature.capitalize()}",
        f"{product_type.capitalize()} {feature} {target} {category.lower()}",
    ]
    
    return random.choice(names)

def generate_mathang(conn, num_records):
    """Tạo dữ liệu Mặt hàng"""
    cursor = conn.cursor()
    sizes = ['Nhỏ', 'Vừa', 'Lớn', 'S', 'M', 'L', 'XL', 'XXL', 'Freesize']
    
    print(f"📦 Tạo {num_records} mặt hàng...")
    
    for i in range(1, num_records + 1):
        ma_mh = f'MH{i:04d}'
        ten_mh = generate_product_name()
        mo_ta = fake.catch_phrase()
        kich_co = random.choice(sizes)
        trong_luong = round(random.uniform(0.1, 50.0), 2)
        gia = round(random.uniform(10000, 5000000), 2)
        ngay_cap_nhat = datetime(2025, 1, 1).date() + timedelta(days=(i * 13) % 730)
        
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
    
    # Mỗi mặt hàng có mặt ở 40% cửa hàng được chọn ngẫu nhiên
    print(f"📊 Tạo dữ liệu mặt hàng được trữ...")
    
    records = []
    start_date_nhap = datetime(2025, 1, 1).date()
    
    # Đảm bảo mỗi mặt hàng có mặt ở nhiều cửa hàng phân bổ qua nhiều vùng miền
    for i in range(1, num_mathang + 1):
        ma_mh = f'MH{i:04d}'
        # Cửa hàng được lấy mẫu random từ toàn bộ tập hợp, đảm bảo rải rác
        selected_stores = random.sample(range(1, num_cuahang + 1), int(num_cuahang * 0.4))
        for j in selected_stores:
            ma_ch = f'CH{j:03d}'
            so_luong = random.randint(10, 1000)
            thoi_gian_nhap = start_date_nhap + timedelta(days=random.randint(0, 729))
            records.append((so_luong, thoi_gian_nhap, ma_mh, ma_ch))
            
            if len(records) % 10000 == 0:
                print(f"  ⏳ Đang tạo {len(records)} bản ghi...")
    
    print("  🚀 Đang Insert vào DB...")
    query = """
        INSERT INTO MatHangDuocTru (soLuongTrongKho, thoiGianNhap, MatHangmaMH, CuaHangmaCH)
        VALUES %s ON CONFLICT DO NOTHING
    """
    psycopg2.extras.execute_values(cursor, query, records, page_size=5000)
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {len(records)} bản ghi mặt hàng được trữ")

def generate_khachhang(conn, num_records, num_vanphong):
    """Tạo dữ liệu Khách hàng"""
    cursor = conn.cursor()
    
    print(f"👥 Tạo {num_records} khách hàng...")
    
    khach_hang_dulich = []
    khach_hang_buudien = []
    
    start_date_kh = datetime(2025, 1, 1).date()
    start_date_dk = datetime(2025, 1, 1).date()
    
    for i in range(1, num_records + 1):
        ma_kh = f'KH{i:05d}'
        ten_kh = fake.name()
        ngay_dat_dau_tien = start_date_kh + timedelta(days=i % 730)
        ma_vp = f'TP{(i % num_vanphong) + 1:03d}'
        
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
        ngay_dang_ky = start_date_dk + timedelta(days=random.randint(0, 729))
        cursor.execute("""
            INSERT INTO KhachHangDuiLich (hdvDuLich, ngayDangKy, KhachHangmaKH)
            VALUES (%s, %s, %s)
        """, (hoi_du_lich, ngay_dang_ky, ma_kh))
    
    # Tạo khách hàng bưu điện
    for ma_kh in khach_hang_buudien:
        hoi_du_lich = f'PO{random.randint(1, 100):04d}'
        ngay_dang_ky = start_date_dk + timedelta(days=random.randint(0, 729))
        cursor.execute("""
            INSERT INTO KhachHangBuiDien (diaChiBuuDien, ngayDangKy, KhachHangmaKH)
            VALUES (%s, %s, %s)
        """, (hoi_du_lich, ngay_dang_ky, ma_kh))
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} khách hàng ({len(khach_hang_dulich)} du lịch, {len(khach_hang_buudien)} bưu điện)")

def generate_donhang(conn, num_records, num_khachhang, num_mathang, num_mh_per_don):
    """Tạo dữ liệu Đơn hàng và Mặt hàng được đặt"""
    cursor = conn.cursor()
    
    print(f"🛒 Tạo {num_records} đơn hàng...")
    
    don_hang_records = []
    mh_dat_records = []

    start_date_dh = datetime(2025, 1, 1).date()

    for i in range(1, num_records + 1):
        ma_don = f'DON{i:06d}'
        ngay_dat_hang = start_date_dh + timedelta(days=i % 730)
        ma_kh = f'KH{(i % num_khachhang) + 1:05d}'
        
        don_hang_records.append((ma_don, ngay_dat_hang, ma_kh))
        
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
            mh_dat_records.append((so_luong, gia, ma_mh, ma_don))
            
        if i % 2000 == 0:
            print(f"  ⏳ Đã chuẩn bị {i}/{num_records} đơn hàng...")
            
    print("  🚀 Đang Insert DonDatHang vào DB...")
    psycopg2.extras.execute_values(cursor, """
        INSERT INTO DonDatHang (maDon, ngayDatHang, KhachHangmaKH)
        VALUES %s ON CONFLICT DO NOTHING
    """, don_hang_records, page_size=5000)
    
    print("  🚀 Đang Insert MatHangDuocDat vào DB...")
    psycopg2.extras.execute_values(cursor, """
        INSERT INTO MatHangDuocDat (soLuongDat, giaDat, MatHangmaMH, DonDatHangmaDon)
        VALUES %s ON CONFLICT DO NOTHING
    """, mh_dat_records, page_size=5000)
    
    conn.commit()
    cursor.close()
    print(f"✅ Đã tạo {num_records} đơn hàng với {len(mh_dat_records)} chi tiết mặt hàng")

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
