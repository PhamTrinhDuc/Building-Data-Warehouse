"""
Extract module - Đọc dữ liệu từ PostgreSQL OLTP (Schema: oltp)
"""
import pandas as pd
from sqlalchemy import create_engine, text
import time


def wait_for_postgres(connection_string, max_retries=3):
    """Đợi PostgreSQL sẵn sàng"""
    for i in range(max_retries):
        try:
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✓ PostgreSQL is ready!")
            return engine
        except Exception as e:
            print(f"Waiting for PostgreSQL... ({i+1}/{max_retries}): {e}")
            time.sleep(2)
    raise Exception("PostgreSQL not available")


def extract_from_postgres():
    """Extract dữ liệu từ schema idb trong PostgreSQL"""
    
    # Connection string
    postgres_conn = "postgresql://admin:admin@localhost:5433/idb"
    
    print("\n=== EXTRACTING DATA FROM POSTGRESQL (Schema: idb) ===")
    
    # Đợi PostgreSQL sẵn sàng
    engine = wait_for_postgres(postgres_conn)
    
    data = {}
    
    # ============================================
    # Extract các bảng chính cho mô hình mới
    # ============================================
    
    print("\n[1/6] Extracting VanPhongDaiDien...")
    data['vanphongdaidien'] = pd.read_sql(
        """
        SELECT 
            maTP,
            tenThanhPho,
            diaChiVP,
            mien,
            ngayThanhLapVP
        FROM idb.VanPhongDaiDien
        """, 
        engine
    )
    
    print("[2/6] Extracting MatHang...")
    data['mathang'] = pd.read_sql(
        """
        SELECT 
            maMH,
            tenMH,
            moTa,
            kichCo,
            trongLuong,
            Gia,
            ngayCapNhat
        FROM idb.MatHang
        """, 
        engine
    )
    
    print("[3/6] Extracting KhachHang...")
    data['khachhang'] = pd.read_sql(
        """
        SELECT 
            maKH,
            tenKH,
            ngayDatDauTien,
            VanPhongDaiDienmaTP
        FROM idb.KhachHang
        """, 
        engine
    )
    
    print("[4/6] Extracting KhachHangDuiLich...")
    data['khachhang_dulich'] = pd.read_sql(
        """
        SELECT 
            KhachHangmaKH,
            hdvDuLich,
            ngayDangKy
        FROM idb.KhachHangDuiLich
        """, 
        engine
    )
    
    print("[5/6] Extracting KhachHangBuiDien...")
    data['khachhang_buudien'] = pd.read_sql(
        """
        SELECT 
            KhachHangmaKH,
            diaChiBuuDien,
            ngayDangKy
        FROM idb.KhachHangBuiDien
        """, 
        engine
    )
    
    print("[6/8] Extracting CuaHang...")
    data['cuahang'] = pd.read_sql(
        """
        SELECT 
            maCH,
            soDienThoai,
            ngayThanhLapCH,
            VanPhongDaiDienmaTP
        FROM idb.CuaHang
        """, 
        engine
    )

    print("[7/8] Extracting MatHangDuocTru...")
    data['mathang_duoctru'] = pd.read_sql(
        """
        SELECT 
            soLuongTrongKho,
            thoiGianNhap,
            MatHangmaMH,
            CuaHangmaCH
        FROM idb.MatHangDuocTru
        """, 
        engine
    )

    print("[8/8] Extracting DonDatHang & MatHangDuocDat...")
    data['dondathang'] = pd.read_sql(
        """
        SELECT 
            ddh.maDon,
            ddh.ngayDatHang,
            ddh.KhachHangmaKH,
            kh.tenKH,
            kh.VanPhongDaiDienmaTP
        FROM idb.DonDatHang ddh
        INNER JOIN idb.KhachHang kh ON ddh.KhachHangmaKH = kh.maKH
        """, 
        engine
    )
    
    data['mathang_duocdat'] = pd.read_sql(
        """
        SELECT 
            mhdd.soLuongDat,
            mhdd.giaDat,
            mhdd.MatHangmaMH,
            mhdd.DonDatHangmaDon,
            mh.tenMH,
            mh.moTa,
            mh.kichCo,
            mh.trongLuong,
            ddh.ngayDatHang,
            ddh.KhachHangmaKH
        FROM idb.MatHangDuocDat mhdd
        INNER JOIN idb.MatHang mh ON mhdd.MatHangmaMH = mh.maMH
        INNER JOIN idb.DonDatHang ddh ON mhdd.DonDatHangmaDon = ddh.maDon
        """, 
        engine
    )
    
    # Print summary
    print("\n=== EXTRACTION SUMMARY ===")
    for table_name, df in data.items():
        print(f"  {table_name:25s}: {len(df):6d} rows")
    
    return data


if __name__ == "__main__":
    data = extract_from_postgres()
    print("\n✓ Extraction completed!")
