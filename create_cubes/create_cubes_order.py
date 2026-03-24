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
        Tạo Cube 1D: Theo bang/tỉnh
        
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
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64,
        ) ENGINE = SummingMergeTree() ORDER BY bang;
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY l.bang;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY l.bang;
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
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (bang, tenThanhPho);
        """
        self.execute(create_storage_sql)
        logger.info(f"🏗️  Tạo bảng storage: {table_name}")
        
        # Tạo materialized view
        create_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT l.tenThanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY l.bang, l.tenThanhPho;
        """
        self.execute(create_view_sql)
        logger.info(f"👁️  Tạo materialized view: {view_name}")
        
        # Insert dữ liệu vào storage
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT l.tenThanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY l.bang, l.tenThanhPho;
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
            loaiKh       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64,
        ) ENGINE = SummingMergeTree() ORDER BY (nam, loaiKh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.loaiKh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.loaiKh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.loaiKh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.loaiKh;
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
            maKh         String,
            loaiKh       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maKh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.maKh, c.loaiKh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.maKh, c.loaiKh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.maKh, c.loaiKh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.maKh, c.loaiKh;
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
            loaiKh       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, loaiKh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.loaiKh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.quy, c.loaiKh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.loaiKh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.quy, c.loaiKh;
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
            maKh         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maKh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.maKh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.quy, c.maKh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.maKh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.quy, c.maKh;
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
            loaiKh       String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, loaiKh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, c.loaiKh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.thang, c.loaiKh;
        """)
        # group by theo (nam, thang) để tránh gộp chung tháng của các nam khác nhau vào cùng 1 nhóm tháng, đồng thời group by thêm theo loại khách hàng để tránh gộp chung loại khách hàng của các tháng khác nhau vào cùng 1 nhóm loại khách hàng
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, c.loaiKh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.thang, c.loaiKh;
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
            maKh         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maKh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, c.maKh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.thang, c.maKh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, c.maKh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, t.thang, c.maKh;
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
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, p.maMh, p.tenMh;
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
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, p.maMh, p.tenMh;
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
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, p.maMh, p.tenMh;
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
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, l.bang;
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
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, l.tenThanhPho, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, l.bang, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, l.tenThanhPho, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, l.bang, l.tenThanhPho;
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
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, l.bang;
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
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, l.tenThanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, l.bang, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, l.tenThanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, l.bang, l.tenThanhPho;
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
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, l.bang;
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
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.thang, l.tenThanhPho, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, l.bang, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.thang, l.tenThanhPho, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, l.bang, l.tenThanhPho;
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
            loaiKh       String,
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKh, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKh, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKh, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKh, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKh, p.maMh, p.tenMh;
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
            maKh         String,
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKh, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKh, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKh, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.maKh, p.maMh, p.tenMh, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKh, p.maMh, p.tenMh;
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
            loaiKh       String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKh, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY c.loaiKh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKh, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY c.loaiKh, l.bang;
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
            loaiKh       String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT l.loaiKh, l.tenThanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY l.loaiKh, l.bang, l.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT l.loaiKh, l.tenThanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY l.loaiKh, l.bang, l.tenThanhPho;
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
            maKh         String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT l.maKh, l.thanhPho, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY l.maKh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT l.maKh, l.thanhPho, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY l.maKh, l.bang, l.thanhPho;
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
            maKh         String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY c.maKh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.maKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY c.maKh, l.bang;
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
            maMh         String,
            tenMh        String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maMh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT p.maMh, p.tenMh, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY p.maMh, p.tenMh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT p.maMh, p.tenMh, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY p.maMh, p.tenMh, l.bang;
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
            maMh         String,
            tenMh        String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maMh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT p.maMh, p.tenMh, l.thanhPho, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY p.maMh, p.tenMh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT p.maMh, p.tenMh, l.thanhPho, l.bang, SUM(f.thanhTien), SUM(f.soLuongDat)
        FROM Fact_DonDatHang f
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY p.maMh, p.tenMh, l.bang, l.thanhPho;
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
            nam          Int32,
            loaiKh       String,
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, loaiKh, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.loaiKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, c.loaiKh, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.loaiKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, c.loaiKh, p.maMh, p.tenMh;
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
            loaiKh       String,
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, loaiKh, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.loaiKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, c.loaiKh, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.loaiKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, c.loaiKh, p.maMh, p.tenMh;
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
            loaiKh       String,
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, loaiKh, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.loaiKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, c.loaiKh, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.loaiKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, c.loaiKh, p.maMh, p.tenMh;
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
            maKh         String,
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maKh, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.maKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, c.maKh, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.maKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, c.maKh, p.maMh, p.tenMh;
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
            maKh         String,
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maKh, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.maKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, c.maKh, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.maKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.thang, c.maKh, p.maMh, p.tenMh;
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
            maKh         String,
            maMh         String,
            tenMh        String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maKh, maMh);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.maKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, c.maKh, p.maMh, p.tenMh;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.maKh, p.maMh, p.tenMh, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY t.nam, t.quy, c.maKh, p.maMh, p.tenMh;
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
            loaiKh       String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, loaiKh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.loaiKh, c.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.loaiKh, c.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.loaiKh, c.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        GROUP BY t.nam, c.loaiKh, c.bang;
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
            loaiKh       String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, loaiKh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.loaiKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, c.loaiKh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.loaiKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, c.loaiKh, l.bang;
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
            loaiKh       String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, loaiKh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.loaiKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, c.loaiKh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.loaiKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, c.loaiKh, l.bang;
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
            maKh         String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maKh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.maKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, c.maKh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.maKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, c.maKh, l.bang;
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
            maKh         String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maKh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.maKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, c.maKh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.maKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, c.maKh, l.bang;
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
            maKh         String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maKh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.maKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, c.maKh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.maKh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, c.maKh, l.bang;
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
            maKh         String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maKh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.maKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, c.maKh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.maKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, c.maKh, l.bang, l.thanhPho;
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
            maKh         String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maKh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.maKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, c.maKh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.maKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, c.maKh, l.bang, l.thanhPho;
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
            maKh         String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maKh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.maKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, c.maKh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.maKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, c.maKh, l.bang, l.thanhPho;
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
            loaiKh       String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, loaiKh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, c.loaiKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, c.loaiKh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, c.loaiKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, c.loaiKh, l.bang, l.thanhPho;
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
            loaiKh       String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, loaiKh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, c.loaiKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, c.loaiKh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, c.loaiKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, c.loaiKh, l.bang, l.thanhPho;
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
            loaiKh       String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, loaiKh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, c.loaiKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, c.loaiKh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, c.loaiKh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, c.loaiKh, l.bang, l.thanhPho;
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
            maMh         String,
            tenMh        String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maMh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, p.maMh, p.tenMh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, p.maMh, p.tenMh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, p.maMh, p.tenMh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, p.maMh, p.tenMh, l.bang;
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
            maMh         String,
            tenMh        String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maMh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, p.maMh, p.tenMh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, p.maMh, p.tenMh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, p.maMh, p.tenMh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, p.maMh, p.tenMh, l.bang;
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
            maMh         String,
            tenMh        String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maMh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, p.maMh, p.tenMh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, p.maMh, p.tenMh, l.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, p.maMh, p.tenMh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, p.maMh, p.tenMh, l.bang;
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
            maMh         String,
            tenMh        String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maMh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, p.maMh, p.tenMh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, p.maMh, p.tenMh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, p.maMh, p.tenMh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, p.maMh, p.tenMh, l.bang, l.thanhPho;
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
            maMh         String,
            tenMh        String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maMh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, p.maMh, p.tenMh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, p.maMh, p.tenMh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, p.maMh, p.tenMh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.thang, p.maMh, p.tenMh, l.bang, l.thanhPho;
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
            maMh         String,
            tenMh        String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maMh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, t.quy, p.maMh, p.tenMh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, p.maMh, p.tenMh, l.bang, l.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, t.quy, p.maMh, p.tenMh, l.thanhPho, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        JOIN Dim_KhachHang l ON f.sk_khachHang = l.sk_khachHang
        GROUP BY t.nam, t.quy, p.maMh, p.tenMh, l.bang, l.thanhPho;
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
            loaiKh       String,
            maMh         String,
            tenMh        String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKh, maMh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKh, p.maMh, p.tenMh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKh, p.maMh, p.tenMh, c.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKh, p.maMh, p.tenMh, l.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKh, p.maMh, p.tenMh, c.bang;
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
            loaiKh       String,
            maMh         String,
            tenMh        String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (loaiKh, maMh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.loaiKh, p.maMh, p.tenMh, c.tenThanhPho, c.bang,, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKh, p.maMh, p.tenMh, c.bang, c.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.loaiKh, p.maMh, p.tenMh, c.tenThanhPho, c.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.loaiKh, p.maMh, p.tenMh, c.bang, c.thanhPho;
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
            maKh         String,
            maMh         String,
            tenMh        String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKh, maMh, bang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKh, p.maMh, p.tenMh, c.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKh, p.maMh, p.tenMh, c.bang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.maKh, p.maMh, p.tenMh, c.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKh, p.maMh, p.tenMh, c.bang;
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
            maKh         String,
            maMh         String,
            tenMh        String,
            thanhPho     String,
            bang         String,
            tongDoanhThu Float64,
            tongSoLuong  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maKh, maMh, bang, thanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT c.maKh, p.maMh, p.tenMh, c.thanhPho, c.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKh, p.maMh, p.tenMh, c.bang, c.thanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT c.maKh, p.maMh, p.tenMh, c.thanhPho, c.bang, SUM(f.thanhTien) as tongDoanhThu, SUM(f.soLuongDat) as tongSoLuong
        FROM Fact_DonDatHang f
        JOIN Dim_KhachHang c ON f.sk_khachHang = c.sk_khachHang
        JOIN Dim_MatHang p ON f.sk_matHang = p.sk_matHang
        GROUP BY c.maKh, p.maMh, p.tenMh, c.bang, c.thanhPho;
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
# HÀM MAIN
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
            manager.create_all_cubes(drop_if_exists=False)

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
    main(mode='2d')
