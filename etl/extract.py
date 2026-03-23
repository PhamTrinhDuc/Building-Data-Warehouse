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
    # Extract các bảng chính với column names rõ ràng
    # ============================================
    
    print("\n[1/9] Extracting VanPhongDaiDien...")
    data['vanphongdaidien'] = pd.read_sql(
        """
        SELECT 
            maTP,
            tenThanhPho,
            diaChiVP,
            bang,
            ngayThanhLapVP
        FROM idb.VanPhongDaiDien
        """, 
        engine
    )
    
    print("[2/9] Extracting CuaHang...")
    data['cuahang'] = pd.read_sql(
        """
        SELECT 
            ch.maCH,
            ch.soDienThoai,
            ch.ngayThanhLapCH,
            ch.VanPhongDaiDienmaTP,
            vp.tenThanhPho,
            vp.diaChiVP,
            vp.bang,
            vp.ngayThanhLapVP
        FROM idb.CuaHang ch
        LEFT JOIN idb.VanPhongDaiDien vp ON ch.VanPhongDaiDienmaTP = vp.maTP
        """, 
        engine
    )
    
    print("[3/9] Extracting MatHang...")
    data['mathang'] = pd.read_sql(
        """
        SELECT 
            maMH,
            moTa,
            loXuong,
            trongLuong,
            Gia,
            ngayMoBan
        FROM idb.MatHang
        """, 
        engine
    )
    
    print("[4/9] Extracting MatHangDuocTru...")
    data['mathang_duoctru'] = pd.read_sql(
        """
        SELECT 
            mhdt.soLuongTrongKho,
            mhdt.thoiGianNhap,
            mhdt.MatHangmaMH,
            mhdt.CuaHangmaCH,
            mh.moTa,
            mh.loXuong,
            mh.trongLuong,
            mh.Gia,
            ch.soDienThoai,
            ch.VanPhongDaiDienmaTP
        FROM idb.MatHangDuocTru mhdt
        INNER JOIN idb.MatHang mh ON mhdt.MatHangmaMH = mh.maMH
        INNER JOIN idb.CuaHang ch ON mhdt.CuaHangmaCH = ch.maCH
        """, 
        engine
    )
    
    print("[5/9] Extracting KhachHang...")
    data['khachhang'] = pd.read_sql(
        """
        SELECT 
            kh.maKH,
            kh.tenKH,
            kh.ngayDatDauTien,
            kh.VanPhongDaiDienmaTP,
            vp.tenThanhPho,
            vp.bang
        FROM idb.KhachHang kh
        LEFT JOIN idb.VanPhongDaiDien vp ON kh.VanPhongDaiDienmaTP = vp.maTP
        """, 
        engine
    )
    
    print("[6/9] Extracting KhachHangDuiLich...")
    data['khachhang_dulich'] = pd.read_sql(
        """
        SELECT 
            khdl.KhachHangmaKH,
            khdl.hoiDuLich,
            kh.tenKH,
            kh.ngayDatDauTien,
            kh.VanPhongDaiDienmaTP
        FROM idb.KhachHangDuiLich khdl
        INNER JOIN idb.KhachHang kh ON khdl.KhachHangmaKH = kh.maKH
        """, 
        engine
    )
    
    print("[7/9] Extracting KhachHangBuiDien...")
    data['khachhang_buudien'] = pd.read_sql(
        """
        SELECT 
            khbd.KhachHangmaKH,
            khbd.hoiDuLich,
            kh.tenKH,
            kh.ngayDatDauTien,
            kh.VanPhongDaiDienmaTP
        FROM idb.KhachHangBuiDien khbd
        INNER JOIN idb.KhachHang kh ON khbd.KhachHangmaKH = kh.maKH
        """, 
        engine
    )
    
    print("[8/9] Extracting DonDatHang...")
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
    
    print("[9/9] Extracting MatHangDuocDat...")
    data['mathang_duocdat'] = pd.read_sql(
        """
        SELECT 
            mhdd.soLuongDat,
            mhdd.giaDat,
            mhdd.MatHangmaMH,
            mhdd.DonDatHangmaDon,
            mh.moTa,
            mh.loXuong,
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
