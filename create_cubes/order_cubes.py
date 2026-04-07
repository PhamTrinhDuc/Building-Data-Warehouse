#!/usr/bin/env python3
"""
Script để tạo các cube (aggregation tables) trong ClickHouse
- Kết nối tới ClickHouse
- Tạo các cube theo yêu cầu
- Hỗ trợ các loại engine: SummingMergeTree, AggregatingMergeTree, etc.
"""

import logging
import sys
from typing import Optional
from clickhouse_driver import Client

# ============================================
# CẤU HÌNH LOGGING
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# CẤU HÌNH CLICKHOUSE
# ============================================
CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 9000,
    'database': 'default',
    'user': 'admin',
    'password': 'admin',
    'settings': {
        'allow_experimental_v2_agg_for_initial_query': 1,
    }
}

# ============================================
# CLASSS: ClickHouse Cube Manager
# ============================================
class OrderCubeManager:
    """Quản lý tạo và quản lý cube trong ClickHouse"""
    
    def __init__(self, config: dict):
        """
        Khởi tạo kết nối tới ClickHouse
        
        Args:
            config: Dictionary chứa host, port, database, user, password
        """
        self.config = config
        self.client = None
        self.connect()
    
    def connect(self) -> None:
        """Tạo kết nối tới ClickHouse"""
        try:
            self.client = Client(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config.get('user', 'default'),
                password=self.config.get('password', ''),
                settings=self.config.get('settings', {})
            )
            logger.info(f"✅ Kết nối ClickHouse thành công: {self.config['host']}:{self.config['port']}")
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối ClickHouse: {e}")
            sys.exit(1)
    
    def disconnect(self) -> None:
        """Đóng kết nối"""
        if self.client:
            self.client.disconnect()
            logger.info("✅ Đã đóng kết nối ClickHouse")
    
    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """
        Thực thi câu lệnh SQL
        
        Args:
            query: Câu lệnh SQL
            params: Tham số (optional)
        """
        try:
            if params:
                self.client.execute(query, params)
            else:
                self.client.execute(query)
            logger.info(f"✅ Thực thi thành công")
        except Exception as e:
            logger.error(f"❌ Lỗi thực thi SQL: {e}")
            raise
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        Kiểm tra bảng có tồn tại không
        
        Args:
            table_name: Tên bảng
            
        Returns:
            True nếu bảng tồn tại, False nếu không
        """
        try:
            result = self.client.execute(
                f"SELECT 1 FROM system.tables WHERE database = '{self.config['database']}' AND name = '{table_name}' LIMIT 1"
            )
            return len(result) > 0
        except Exception as e:
            logger.error(f"❌ Lỗi kiểm tra bảng: {e}")
            return False
    
    def create_cube_0d(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 0D: Tổng doanh thu và tổng số lượng (không chiều)
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_0D (Tổng Doanh Thu & Số Lượng)...")
        
        table_name = 'cube_0d_storage'
        view_name = 'cube_0d'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree()
        ORDER BY tuple();
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT 
            SUM(thanhTien) as tongDoanhThu,
            SUM(soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")

        # insert dữ liệu vào storage bằng cách chạy lại view
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT
            SUM(thanhTien)  AS tongDoanhThu,
            SUM(soLuongDat) AS tongSoLuong
        FROM Fact_DonDatHang;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name} từ {view_name}")
    
    def create_cube_1d_year(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo năm
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_1D_Year (Theo Năm)...")
        
        table_name = 'cube_1d_nam_storage'
        view_name = 'cube_1d_nam'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY nam;
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT t.nam, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name}")

    def create_cube_1d_quarter(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo quý
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_1D_Quarter (Theo Quý)...")
        
        table_name = 'cube_1d_quy_storage'
        view_name = 'cube_1d_quy'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        # group by theo (nam, quy) để tránh gộp chung quy của các nam khác nhau vào cùng 1 nhóm quy
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            quy          Int32,
            nam          Int32,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy);
        """

        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.quy, t.nam, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam, t.quy;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT t.quy, t.nam, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam, t.quy;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name}")

    def create_cube_1d_month(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo tháng
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_1D_Month (Theo Tháng)...")
        
        table_name = 'cube_1d_thang_storage'
        view_name = 'cube_1d_thang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        # group by theo (nam, thang) để tránh gộp chung tháng của các nam khác nhau vào cùng 1 nhóm tháng
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang);
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam, t.thang;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam, t.thang;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name}")

    def create_cube_1d_customer_type(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo loại khách hàng
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_1D_Customer_Type (Theo Loại KH)...")
        
        table_name = 'cube_1d_loai_kh_storage'
        view_name = 'cube_1d_loai_kh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            loaiKhachHang       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64,
        ) ENGINE = SummingMergeTree() ORDER BY loaiKhachHang;
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY c.loaiKhachHang;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY c.loaiKhachHang;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name}")

    def create_cube_1d_customer_id(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo mã khách hàng
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_1D_Customer_ID (Theo Mã KH)...")
        
        table_name = 'cube_1d_ma_kh_storage'
        view_name = 'cube_1d_ma_kh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maKH         String,
            loaiKhachHang       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY maKH;
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        # group by theo (maKH, loaiKhachHang) để tránh gộp chung mã khách hàng của các loại khác nhau vào cùng 1 nhóm mã khách hàng
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKH, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY c.maKH, c.loaiKhachHang;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT c.maKH, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY c.maKH, c.loaiKhachHang;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name}")

    def create_cube_1d_product(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo mã mặt hàng
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_1D_Product (Theo Mã MH)...")
        
        table_name = 'cube_1d_ma_mh_storage'
        view_name = 'cube_1d_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maMH         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY maMH;
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT p.maMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY p.maMH;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT p.maMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY p.maMH;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name}")

    def create_cube_1d_state(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo mien/tỉnh
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_1D_State (Theo Bang)...")
        
        table_name = 'cube_1d_bang_storage'
        view_name = 'cube_1d_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY mien;
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY l.mien;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY l.mien;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name}")

    def create_cube_1d_city(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo thành phố
        
        Args:
            drop_if_exists: Xóa bảng nếu đã tồn tại
        """
        logger.info("📊 Tạo Cube_1D_City (Theo Thành Phố)...")
        
        table_name = 'cube_1d_thanhpho_storage'
        view_name = 'cube_1d_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"🗑️  Đã xóa {view_name} và {table_name}")
        
        # Tạo bảng lưu trữ cube
        create_storage_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (mien, tenThanhPho);
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY l.mien, l.tenThanhPho;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY l.mien, l.tenThanhPho;
        """
        self.execute(insert_sql)
        logger.info(f"📥 Đã insert dữ liệu vào {table_name}")

    # ============================================================
    # CUBE 2D: THỜI GIAN + KHÁCH HÀNG
    # ============================================================
    
    def create_cube_2d_year_customer_type(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Năm - Loại Khách Hàng"""
        logger.info("📊 Tạo Cube_2D_Year_CustomerType...")
        table_name = 'cube_2d_nam_loai_kh_storage'
        view_name = 'cube_2d_nam_loai_kh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            loaiKhachHang       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64,
        ) ENGINE = SummingMergeTree() ORDER BY (nam, loaiKhachHang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.loaiKhachHang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.loaiKhachHang;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_year_customer_id(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Năm - Mã Khách Hàng"""
        logger.info("📊 Tạo Cube_2D_Year_CustomerID...")
        table_name = 'cube_2d_nam_ma_kh_storage'
        view_name = 'cube_2d_nam_ma_kh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            maKH         String,
            loaiKhachHang       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maKH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.maKH, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.maKH, c.loaiKhachHang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.maKH, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.maKH, c.loaiKhachHang;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_quarter_customer_type(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Quý - Loại Khách Hàng"""
        logger.info("📊 Tạo Cube_2D_Quarter_CustomerType...")
        table_name = 'cube_2d_quy_loai_kh_storage'
        view_name = 'cube_2d_quy_loai_kh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            loaiKhachHang String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, loaiKhachHang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.quy, c.loaiKhachHang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.quy, c.loaiKhachHang;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_quarter_customer_id(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Quý - Mã Khách Hàng"""
        logger.info("📊 Tạo Cube_2D_Quarter_CustomerID...")
        table_name = 'cube_2d_quy_ma_kh_storage'
        view_name = 'cube_2d_quy_ma_kh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            maKH         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maKH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.maKH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.quy, c.maKH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.maKH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.quy, c.maKH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    
    def create_cube_2d_month_customer_type(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Tháng - Loại Khách Hàng"""
        logger.info("📊 Tạo Cube_2D_Month_CustomerType...")
        table_name = 'cube_2d_thang_loai_kh_storage'
        view_name = 'cube_2d_thang_loai_kh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            thang        Int32,
            loaiKhachHang       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, loaiKhachHang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.thang, c.loaiKhachHang;
        """)
        # group by theo (nam, thang) để tránh gộp chung tháng của các nam khác nhau vào cùng 1 nhóm tháng, đồng thời group by thêm theo loại khách hàng để tránh gộp chung loại khách hàng của các tháng khác nhau vào cùng 1 nhóm loại khách hàng
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, c.loaiKhachHang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.thang, c.loaiKhachHang;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_month_customer_id(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Tháng - Mã Khách Hàng"""
        logger.info("📊 Tạo Cube_2D_Month_CustomerID...")
        table_name = 'cube_2d_thang_ma_kh_storage'
        view_name = 'cube_2d_thang_ma_kh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            thang        Int32,
            maKH         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maKH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, c.maKH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.thang, c.maKH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, c.maKH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.thang, c.maKH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    # ============================================================
    # CUBE 2D: THỜI GIAN + MẶT HÀNG
    # ============================================================
    
    def create_cube_2d_year_product(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Năm - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_2D_Year_Product...")
        table_name = 'cube_2d_nam_ma_mh_storage'
        view_name = 'cube_2d_nam_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_quarter_product(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Quý - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_2D_Quarter_Product...")
        table_name = 'cube_2d_quy_ma_mh_storage'
        view_name = 'cube_2d_quy_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_month_product(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Tháng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_2D_Month_Product...")
        table_name = 'cube_2d_thang_ma_mh_storage'
        view_name = 'cube_2d_thang_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            thang        Int32,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    # ============================================================
    # CUBE 2D: THỜI GIAN + ĐỊA ĐIỂM
    # ============================================================
    
    def create_cube_2d_year_state(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Năm - Bang"""
        logger.info("📊 Tạo Cube_2D_Year_State...")
        table_name = 'cube_2d_nam_bang_storage'
        view_name = 'cube_2d_nam_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_year_city(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Năm - Thành Phố"""
        logger.info("📊 Tạo Cube_2D_Year_City...")
        table_name = 'cube_2d_nam_thanhpho_storage'
        view_name = 'cube_2d_nam_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_quarter_state(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Quý - Bang"""
        logger.info("📊 Tạo Cube_2D_Quarter_State...")
        table_name = 'cube_2d_quy_bang_storage'
        view_name = 'cube_2d_quy_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_quarter_city(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Quý - Thành Phố"""
        logger.info("📊 Tạo Cube_2D_Quarter_City...")
        table_name = 'cube_2d_quy_thanhpho_storage'
        view_name = 'cube_2d_quy_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_month_state(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Tháng - Bang"""
        logger.info("📊 Tạo Cube_2D_Month_State...")
        table_name = 'cube_2d_thang_bang_storage'
        view_name = 'cube_2d_thang_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            thang        Int32,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_month_city(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Tháng - Thành Phố"""
        logger.info("📊 Tạo Cube_2D_Month_City...")
        table_name = 'cube_2d_thang_thanhpho_storage'
        view_name = 'cube_2d_thang_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            thang        Int32,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem

        GROUP BY t.nam, t.thang, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    # ============================================================
    # CUBE 2D: KHÁCH HÀNG + MẶT HÀNG
    # ============================================================
    
    def create_cube_2d_customer_type_product(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Loại Khách Hàng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_2D_CustomerType_Product...")
        table_name = 'cube_2d_loai_kh_ma_mh_storage'
        view_name = 'cube_2d_loai_kh_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            loaiKhachHang       String,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKhachHang, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKhachHang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKhachHang, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKhachHang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKhachHang, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_customer_id_product(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Mã Khách Hàng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_2D_CustomerID_Product...")
        table_name = 'cube_2d_ma_kh_ma_mh_storage'
        view_name = 'cube_2d_ma_kh_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maKH         String,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKH, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKH, p.maMH, p.tenMH, SUM(f.thanhTien) AS tongDoanhThu, SUM(f.soLuongDat) AS tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKH, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.maKH, p.maMH, p.tenMH, SUM(f.thanhTien) AS tongDoanhThu, SUM(f.soLuongDat) AS tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKH, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    # ============================================================
    # CUBE 2D: KHÁCH HÀNG + ĐỊA ĐIỂM
    # ============================================================
    
    def create_cube_2d_customer_type_state(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Loại Khách Hàng - Bang"""
        logger.info("📊 Tạo Cube_2D_CustomerType_State...")
        table_name = 'cube_2d_loai_kh_bang_storage'
        view_name = 'cube_2d_loai_kh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            loaiKhachHang       String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKhachHang, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKhachHang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY c.loaiKhachHang, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKhachHang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY c.loaiKhachHang, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_customer_type_city(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Loại Khách Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_2D_CustomerType_City...")
        table_name = 'cube_2d_loai_kh_thanhpho_storage'
        view_name = 'cube_2d_loai_kh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            loaiKhachHang       String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKhachHang, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKhachHang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY c.loaiKhachHang, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKhachHang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY c.loaiKhachHang, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_customer_id_city(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Mã Khách Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_2D_CustomerID_City...")
        table_name = 'cube_2d_ma_kh_thanhpho_storage'
        view_name = 'cube_2d_ma_kh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maKH         String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKH, l.tenThanhPho, l.mien, SUM(f.thanhTien) AS tongDoanhThu, SUM(f.soLuongDat) AS tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY c.maKH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name} 
        SELECT c.maKH, l.tenThanhPho, l.mien, SUM(f.thanhTien) AS tongDoanhThu, SUM(f.soLuongDat) AS tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY c.maKH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_customer_id_state(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Mã Khách Hàng - Bang"""
        logger.info("📊 Tạo Cube_2D_CustomerID_State...")
        table_name = 'cube_2d_ma_kh_bang_storage'
        view_name = 'cube_2d_ma_kh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maKH         String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY c.maKH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.maKH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY c.maKH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    # ============================================================
    # CUBE 2D: MẶT HÀNG + ĐỊA ĐIỂM
    # ============================================================
    
    def create_cube_2d_product_state(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Mã Mặt Hàng - Bang"""
        logger.info("📊 Tạo Cube_2D_Product_State...")
        table_name = 'cube_2d_ma_mh_bang_storage'
        view_name = 'cube_2d_ma_mh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maMH         String,
            tenMH        String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maMH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY p.maMH, p.tenMH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY p.maMH, p.tenMH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_2d_product_city(self, drop_if_exists: bool = False) -> None:
        """Cube 2D: Mã Mặt Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_2D_Product_City...")
        table_name = 'cube_2d_ma_mh_thanhpho_storage'
        view_name = 'cube_2d_ma_mh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maMH         String,
            tenMH        String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maMH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    # ============================================================
    # CUBE 3D: THỜI GIAN + KHÁCH HÀNG + MẶT HÀNG
    # ============================================================
    
    def create_cube_3d_year_customer_type_product(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Năm - Loại Khách Hàng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_3D_Year_CustomerType_Product...")
        table_name = 'cube_3d_nam_loai_kh_ma_mh_storage'
        view_name = 'cube_3d_nam_loai_kh_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam           Int32,
            loaiKhachHang String,
            maMH          String,
            tenMH         String,
            tongDoanhThu  Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, loaiKhachHang, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.loaiKhachHang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, c.loaiKhachHang, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.loaiKhachHang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, c.loaiKhachHang, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_month_customer_type_product(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Tháng - Loại Khách Hàng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_3D_Month_CustomerType_Product...")
        table_name = 'cube_3d_thang_loai_kh_ma_mh_storage'
        view_name = 'cube_3d_thang_loai_kh_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            loaiKhachHang       String,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, loaiKhachHang, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.loaiKhachHang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, c.loaiKhachHang, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.loaiKhachHang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, c.loaiKhachHang, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_quarter_customer_type_product(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Quý - Loại Khách Hàng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_3D_Quarter_CustomerType_Product...")
        table_name = 'cube_3d_quy_loai_kh_ma_mh_storage'
        view_name = 'cube_3d_quy_loai_kh_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            loaiKhachHang       String,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, loaiKhachHang, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.loaiKhachHang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, c.loaiKhachHang, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.loaiKhachHang, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, c.loaiKhachHang, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_year_customer_id_product(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Năm - Mã Khách Hàng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_3D_Year_CustomerID_Product...")
        table_name = 'cube_3d_nam_ma_kh_ma_mh_storage'
        view_name = 'cube_3d_nam_ma_kh_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            maKH         String,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maKH, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.maKH, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, c.maKH, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.maKH, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, c.maKH, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_month_customer_id_product(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Tháng - Mã Khách Hàng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_3D_Month_CustomerID_Product...")
        table_name = 'cube_3d_thang_ma_kh_ma_mh_storage'
        view_name = 'cube_3d_thang_ma_kh_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            maKH         String,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maKH, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.maKH, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, c.maKH, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.maKH, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, c.maKH, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_quarter_customer_id_product(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Quý - Mã Khách Hàng - Mã Mặt Hàng"""
        logger.info("📊 Tạo Cube_3D_Quarter_CustomerID_Product...")
        table_name = 'cube_3d_quy_ma_kh_ma_mh_storage'
        view_name = 'cube_3d_quy_ma_kh_ma_mh'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            maKH         String,
            maMH         String,
            tenMH        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maKH, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.maKH, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, c.maKH, p.maMH, p.tenMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.maKH, p.maMH, p.tenMH, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, c.maKH, p.maMH, p.tenMH;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    # ============================================================
    # CUBE 3D: THỜI GIAN + KHÁCH HÀNG + ĐỊA ĐIỂM
    # ============================================================
    
    def create_cube_3d_year_customer_type_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Năm - Loại Khách Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Year_CustomerType_State...")
        table_name = 'cube_3d_nam_loai_kh_bang_storage'
        view_name = 'cube_3d_nam_loai_kh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            loaiKhachHang       String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, loaiKhachHang, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.loaiKhachHang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, c.loaiKhachHang, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.loaiKhachHang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, c.loaiKhachHang, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    
    def create_cube_3d_month_customer_type_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Tháng - Loại Khách Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Month_CustomerType_State...")
        table_name = 'cube_3d_thang_loai_kh_bang_storage'
        view_name = 'cube_3d_thang_loai_kh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            loaiKhachHang       String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, loaiKhachHang, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.loaiKhachHang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, c.loaiKhachHang, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.loaiKhachHang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, c.loaiKhachHang, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    
    def create_cube_3d_quarter_customer_type_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Quý - Loại Khách Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Quarter_CustomerType_State...")
        table_name = 'cube_3d_quy_loai_kh_bang_storage'
        view_name = 'cube_3d_quy_loai_kh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            loaiKhachHang       String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, loaiKhachHang, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.loaiKhachHang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, c.loaiKhachHang, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.loaiKhachHang, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, c.loaiKhachHang, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    
    def create_cube_3d_year_customer_id_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Năm - Mã Khách Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Year_CustomerID_State...")
        table_name = 'cube_3d_nam_ma_kh_bang_storage'
        view_name = 'cube_3d_nam_ma_kh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            maKH         String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maKH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.maKH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, c.maKH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.maKH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, c.maKH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    
    def create_cube_3d_month_customer_id_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Tháng - Mã Khách Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Month_CustomerID_State...")
        table_name = 'cube_3d_thang_ma_kh_bang_storage'
        view_name = 'cube_3d_thang_ma_kh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            maKH         String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maKH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.maKH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, c.maKH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.maKH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, c.maKH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    
    def create_cube_3d_quarter_customer_id_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Quý - Mã Khách Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Quarter_CustomerID_State...")
        table_name = 'cube_3d_quy_ma_kh_bang_storage'
        view_name = 'cube_3d_quy_ma_kh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            maKH         String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maKH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.maKH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, c.maKH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.maKH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, c.maKH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    
    def create_cube_3d_year_customer_id_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Năm - Mã Khách Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Year_CustomerID_City...")
        table_name = 'cube_3d_nam_ma_kh_thanhpho_storage'
        view_name = 'cube_3d_nam_ma_kh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            maKH         String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maKH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.maKH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, c.maKH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.maKH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, c.maKH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_month_customer_id_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Tháng - Mã Khách Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Month_CustomerID_City...")
        table_name = 'cube_3d_thang_ma_kh_thanhpho_storage'
        view_name = 'cube_3d_thang_ma_kh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            maKH         String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maKH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.maKH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, c.maKH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.maKH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, c.maKH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_quarter_customer_id_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Quý - Mã Khách Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Quarter_CustomerID_City...")
        table_name = 'cube_3d_quy_ma_kh_thanhpho_storage'
        view_name = 'cube_3d_quy_ma_kh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            maKH         String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maKH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.maKH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, c.maKH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.maKH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, c.maKH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_year_customer_type_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Năm - Loại Khách Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Year_CustomerType_City...")
        table_name = 'cube_3d_nam_loai_kh_thanhpho_storage'
        view_name = 'cube_3d_nam_loai_kh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            loaiKhachHang       String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, loaiKhachHang, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.loaiKhachHang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, c.loaiKhachHang, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.loaiKhachHang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, c.loaiKhachHang, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_month_customer_type_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Tháng - Loại Khách Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Month_CustomerType_City...")
        table_name = 'cube_3d_thang_loai_kh_thanhpho_storage'
        view_name = 'cube_3d_thang_loai_kh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            loaiKhachHang       String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, loaiKhachHang, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.loaiKhachHang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, c.loaiKhachHang, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.loaiKhachHang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, c.loaiKhachHang, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_quarter_customer_type_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Quý - Loại Khách Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Quarter_CustomerType_City...")
        table_name = 'cube_3d_quy_loai_kh_thanhpho_storage'
        view_name = 'cube_3d_quy_loai_kh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            loaiKhachHang       String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, loaiKhachHang, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.loaiKhachHang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, c.loaiKhachHang, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.loaiKhachHang, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, c.loaiKhachHang, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    # ============================================================
    # CUBE 3D: THỜI GIAN + MẶT HÀNG + ĐỊA ĐIỂM
    # ============================================================
    
    def create_cube_3d_year_product_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Năm - Mã Mặt Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Year_Product_State...")
        table_name = 'cube_3d_nam_ma_mh_bang_storage'
        view_name = 'cube_3d_nam_ma_mh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            maMH         String,
            tenMH        String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maMH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, p.maMH, p.tenMH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, p.maMH, p.tenMH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")
    
    def create_cube_3d_month_product_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Tháng - Mã Mặt Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Month_Product_State...")
        table_name = 'cube_3d_thang_ma_mh_bang_storage'
        view_name = 'cube_3d_thang_ma_mh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            maMH         String,
            tenMH        String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maMH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, p.maMH, p.tenMH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, p.maMH, p.tenMH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_quarter_product_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Quý - Mã Mặt Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_Quarter_Product_State...")
        table_name = 'cube_3d_quy_ma_mh_bang_storage'
        view_name = 'cube_3d_quy_ma_mh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            maMH         String,
            tenMH        String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maMH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, p.maMH, p.tenMH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, p.maMH, p.tenMH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_year_product_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Năm - Mã Mặt Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Year_Product_City...")
        table_name = 'cube_3d_nam_ma_mh_thanhpho_storage'
        view_name = 'cube_3d_nam_ma_mh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            maMH         String,
            tenMH        String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maMH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_month_product_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Tháng - Mã Mặt Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Month_Product_City...")
        table_name = 'cube_3d_thang_ma_mh_thanhpho_storage'
        view_name = 'cube_3d_thang_ma_mh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang        Int32,
            nam          Int32,
            maMH         String,
            tenMH        String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maMH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.thang, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_quarter_product_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Quý - Mã Mặt Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_Quarter_Product_City...")
        table_name = 'cube_3d_quy_ma_mh_thanhpho_storage'
        view_name = 'cube_3d_quy_ma_mh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam          Int32,
            quy          Int32,
            maMH         String,
            tenMH        String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maMH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        GROUP BY t.nam, t.quy, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    # ============================================================
    # CUBE 3D: KHÁCH HÀNG + MẶT HÀNG + ĐỊA ĐIỂM
    # ============================================================

    def create_cube_3d_customer_type_product_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Loại Khách Hàng - Mã Mặt Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_CustomerType_Product_State...")
        table_name = 'cube_3d_loai_kh_ma_mh_bang_storage'
        view_name = 'cube_3d_loai_kh_ma_mh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            loaiKhachHang       String,
            maMH         String,
            tenMH        String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKhachHang, maMH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKhachHang, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKhachHang, p.maMH, p.tenMH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKhachHang, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKhachHang, p.maMH, p.tenMH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_customer_type_product_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Loại Khách Hàng - Mã Mặt Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_CustomerType_Product_City...")
        table_name = 'cube_3d_loai_kh_ma_mh_thanhpho_storage'
        view_name = 'cube_3d_loai_kh_ma_mh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            loaiKhachHang       String,
            maMH         String,
            tenMH        String,
            tenThanhPho  String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKhachHang, maMH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKhachHang, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKhachHang, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKhachHang, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKhachHang, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_customer_id_product_state(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Mã Khách Hàng - Mã Mặt Hàng - Bang"""
        logger.info("📊 Tạo Cube_3D_CustomerID_Product_State...")
        table_name = 'cube_3d_ma_kh_ma_mh_bang_storage'
        view_name = 'cube_3d_ma_kh_ma_mh_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maKH         String,
            maMH         String,
            tenMH        String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKH, maMH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKH, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKH, p.maMH, p.tenMH, l.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.maKH, p.maMH, p.tenMH, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKH, p.maMH, p.tenMH, l.mien;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_cube_3d_customer_id_product_city(self, drop_if_exists: bool = False) -> None:
        """Cube 3D: Mã Khách Hàng - Mã Mặt Hàng - Thành Phố"""
        logger.info("📊 Tạo Cube_3D_CustomerID_Product_City...")
        table_name = 'cube_3d_ma_kh_ma_mh_thanhpho_storage'
        view_name = 'cube_3d_ma_kh_ma_mh_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maKH         String,
            maMH         String,
            tenMH        String,
            tenThanhPho     String,
            mien         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKH, maMH, mien, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKH, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKH, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.maKH, p.maMH, p.tenMH, l.tenThanhPho, l.mien, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_DiaDiem l ON c.sk_diaDiem = l.sk_diaDiem
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKH, p.maMH, p.tenMH, l.mien, l.tenThanhPho;
        """)
        logger.info(f"✅ {table_name} tạo thành công")

    def create_all_cubes(self, drop_if_exists: bool = False) -> None:
        """
        Tạo tất cả các cube
        
        Args:
            drop_if_exists: Xóa nếu đã tồn tại
        """
        logger.info("=" * 60)
        logger.info("📊 BẮT ĐẦU TẠO CÁC CUBE")
        logger.info("=" * 60)
        
        try:
            # Cube 0D
            self.create_cube_0d(drop_if_exists=drop_if_exists)
            
            # Cube 1D - Thời Gian
            logger.info("\n--- CUBE 1D: THỜI GIAN ---")
            self.create_cube_1d_year(drop_if_exists=drop_if_exists)
            self.create_cube_1d_quarter(drop_if_exists=drop_if_exists)
            self.create_cube_1d_month(drop_if_exists=drop_if_exists)
            
            # Cube 1D - Khách Hàng
            logger.info("\n--- CUBE 1D: KHÁCH HÀNG ---")
            self.create_cube_1d_customer_type(drop_if_exists=drop_if_exists)
            self.create_cube_1d_customer_id(drop_if_exists=drop_if_exists)
            
            # Cube 1D - Mặt Hàng
            logger.info("\n--- CUBE 1D: MẶT HÀNG ---")
            self.create_cube_1d_product(drop_if_exists=drop_if_exists)
            
            # Cube 1D - Địa Điểm
            logger.info("\n--- CUBE 1D: ĐỊA ĐIỂM ---")
            self.create_cube_1d_state(drop_if_exists=drop_if_exists)
            self.create_cube_1d_city(drop_if_exists=drop_if_exists)
            
            # Cube 2D - Thời Gian + Khách Hàng
            logger.info("\n--- CUBE 2D: THỜI GIAN + KHÁCH HÀNG ---")
            self.create_cube_2d_year_customer_type(drop_if_exists=drop_if_exists)
            self.create_cube_2d_year_customer_id(drop_if_exists=drop_if_exists)
            self.create_cube_2d_quarter_customer_type(drop_if_exists=drop_if_exists)
            self.create_cube_2d_quarter_customer_id(drop_if_exists=drop_if_exists)
            self.create_cube_2d_month_customer_type(drop_if_exists=drop_if_exists)
            self.create_cube_2d_month_customer_id(drop_if_exists=drop_if_exists)
            
            # Cube 2D - Thời Gian + Mặt Hàng
            logger.info("\n--- CUBE 2D: THỜI GIAN + MẶT HÀNG ---")
            self.create_cube_2d_year_product(drop_if_exists=drop_if_exists)
            self.create_cube_2d_quarter_product(drop_if_exists=drop_if_exists)
            self.create_cube_2d_month_product(drop_if_exists=drop_if_exists)
            
            # Cube 2D - Thời Gian + Địa Điểm
            logger.info("\n--- CUBE 2D: THỜI GIAN + ĐỊA ĐIỂM ---")
            self.create_cube_2d_year_state(drop_if_exists=drop_if_exists)
            self.create_cube_2d_year_city(drop_if_exists=drop_if_exists)
            self.create_cube_2d_quarter_state(drop_if_exists=drop_if_exists)
            self.create_cube_2d_quarter_city(drop_if_exists=drop_if_exists)
            self.create_cube_2d_month_state(drop_if_exists=drop_if_exists)
            self.create_cube_2d_month_city(drop_if_exists=drop_if_exists)
            
            # Cube 2D - Khách Hàng + Mặt Hàng
            logger.info("\n--- CUBE 2D: KHÁCH HÀNG + MẶT HÀNG ---")
            self.create_cube_2d_customer_type_product(drop_if_exists=drop_if_exists)
            self.create_cube_2d_customer_id_product(drop_if_exists=drop_if_exists)
            
            # Cube 2D - Khách Hàng + Địa Điểm
            logger.info("\n--- CUBE 2D: KHÁCH HÀNG + ĐỊA ĐIỂM ---")
            self.create_cube_2d_customer_type_state(drop_if_exists=drop_if_exists)
            self.create_cube_2d_customer_type_city(drop_if_exists=drop_if_exists)
            self.create_cube_2d_customer_id_state(drop_if_exists=drop_if_exists)
            self.create_cube_2d_customer_id_city(drop_if_exists=drop_if_exists)
            
            # Cube 2D - Mặt Hàng + Địa Điểm
            logger.info("\n--- CUBE 2D: MẶT HÀNG + ĐỊA ĐIỂM ---")
            self.create_cube_2d_product_state(drop_if_exists=drop_if_exists)
            self.create_cube_2d_product_city(drop_if_exists=drop_if_exists)
            
            # Cube 3D - Thời Gian + Khách Hàng + Mặt Hàng
            logger.info("\n--- CUBE 3D: THỜI GIAN + KHÁCH HÀNG + MẶT HÀNG ---")
            self.create_cube_3d_year_customer_type_product(drop_if_exists=drop_if_exists)
            self.create_cube_3d_quarter_customer_type_product(drop_if_exists=drop_if_exists)
            self.create_cube_3d_month_customer_type_product(drop_if_exists=drop_if_exists)
            self.create_cube_3d_year_customer_id_product(drop_if_exists=drop_if_exists)
            self.create_cube_3d_month_customer_id_product(drop_if_exists=drop_if_exists)
            self.create_cube_3d_quarter_customer_id_product(drop_if_exists=drop_if_exists)
            
            # Cube 3D - Thời Gian + Khách Hàng + Địa Điểm (State)
            logger.info("\n--- CUBE 3D: THỜI GIAN + KHÁCH HÀNG + BANG ---")
            self.create_cube_3d_year_customer_type_state(drop_if_exists=drop_if_exists)
            self.create_cube_3d_quarter_customer_type_state(drop_if_exists=drop_if_exists)
            self.create_cube_3d_month_customer_type_state(drop_if_exists=drop_if_exists)
            self.create_cube_3d_year_customer_id_state(drop_if_exists=drop_if_exists)
            self.create_cube_3d_quarter_customer_id_state(drop_if_exists=drop_if_exists)
            self.create_cube_3d_month_customer_id_state(drop_if_exists=drop_if_exists)
            
            # Cube 3D - Thời Gian + Khách Hàng + Địa Điểm (City)
            logger.info("\n--- CUBE 3D: THỜI GIAN + KHÁCH HÀNG + THÀNH PHỐ ---")
            self.create_cube_3d_year_customer_type_city(drop_if_exists=drop_if_exists)
            self.create_cube_3d_quarter_customer_type_city(drop_if_exists=drop_if_exists)
            self.create_cube_3d_month_customer_type_city(drop_if_exists=drop_if_exists)
            self.create_cube_3d_year_customer_id_city(drop_if_exists=drop_if_exists)
            self.create_cube_3d_quarter_customer_id_city(drop_if_exists=drop_if_exists)
            self.create_cube_3d_month_customer_id_city(drop_if_exists=drop_if_exists)
            
            # Cube 3D - Thời Gian + Mặt Hàng + Địa Điểm (State)
            logger.info("\n--- CUBE 3D: THỜI GIAN + MẶT HÀNG + BANG ---")
            self.create_cube_3d_year_product_state(drop_if_exists=drop_if_exists)
            self.create_cube_3d_quarter_product_state(drop_if_exists=drop_if_exists)
            self.create_cube_3d_month_product_state(drop_if_exists=drop_if_exists)
            
            # Cube 3D - Thời Gian + Mặt Hàng + Địa Điểm (City)
            logger.info("\n--- CUBE 3D: THỜI GIAN + MẶT HÀNG + THÀNH PHỐ ---")
            self.create_cube_3d_year_product_city(drop_if_exists=drop_if_exists)
            self.create_cube_3d_quarter_product_city(drop_if_exists=drop_if_exists)
            self.create_cube_3d_month_product_city(drop_if_exists=drop_if_exists)
            
            # Cube 3D - Khách Hàng + Mặt Hàng + Địa Điểm (State)
            logger.info("\n--- CUBE 3D: KHÁCH HÀNG + MẶT HÀNG + BANG ---")
            self.create_cube_3d_customer_type_product_state(drop_if_exists=drop_if_exists)
            self.create_cube_3d_customer_id_product_state(drop_if_exists=drop_if_exists)
            
            # Cube 3D - Khách Hàng + Mặt Hàng + Địa Điểm (City)
            logger.info("\n--- CUBE 3D: KHÁCH HÀNG + MẶT HÀNG + THÀNH PHỐ ---")
            self.create_cube_3d_customer_type_product_city(drop_if_exists=drop_if_exists)
            self.create_cube_3d_customer_id_product_city(drop_if_exists=drop_if_exists)
            
            logger.info("=" * 60)
            logger.info("✅ HOÀN THÀNH TẠO CÁC CUBE")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error(f"❌ Lỗi tạo cube: {e}")
            raise

# ============================================
def main(mode: str = 'all') -> None:
    """Hàm chính"""
    
    logger.info("🚀 BẮT ĐẦU SCRIPT TẠO CUBE CLICKHOUSE")
    
    # Tạo manager
    manager = OrderCubeManager(CLICKHOUSE_CONFIG)
    # Count the number of methods in the OrderCubeManager class
    method_count = len([method for method in dir(manager) if callable(getattr(manager, method)) if method.startswith('create_cube_3d')])
    logger.info(f"📊 Tổng số methods trong OrderCubeManager: {method_count}")
    
    
    try:
        # Tạo tất cả cube
        # Để xóa cube cũ trước khi tạo mới, set drop_if_exists=True
        if mode == 'all':
            manager.create_all_cubes(drop_if_exists=True)

        # create cube 0d: 
        elif mode == '0d':
            manager.create_cube_0d(drop_if_exists=True)

        # create cube 1d:
        elif mode == '1d':
            methods_1d = [method for method in dir(manager) if callable(getattr(manager, method)) and method.startswith('create_cube_1d')]
            for method_name in methods_1d:
                method = getattr(manager, method_name)
                method(drop_if_exists=True)
        
        # create cube 2d:
        elif mode == '2d':
            methods_2d = [method for method in dir(manager) if callable(getattr(manager, method)) and method.startswith('create_cube_2d')]
            for method_name in methods_2d:
                method = getattr(manager, method_name)
                method(drop_if_exists=True)
        
        # create cube 3d:
        elif mode == '3d':
            methods_3d = [method for method in dir(manager) if callable(getattr(manager, method)) and method.startswith('create_cube_3d')]
            for method_name in methods_3d:
                method = getattr(manager, method_name)
                method(drop_if_exists=True)
    
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")
        sys.exit(1)
    
    finally:
        manager.disconnect()

if __name__ == '__main__':
    main(mode='3d')
