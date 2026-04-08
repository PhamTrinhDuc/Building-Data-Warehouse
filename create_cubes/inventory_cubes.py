#!/usr/bin/env python3
"""
Script để tạo các cube (aggregation tables) trong ClickHouse cho Tồn Kho
"""

import logging
from typing import Optional
from clickhouse_driver import Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

class InventoryCubeManager:
    """Quản lý tạo và quản lý cube tồn kho trong ClickHouse"""
    
    def __init__(self, config: dict):
        self.config = config
        self.client = None
        self.connect()
    
    def connect(self) -> None:
        try:
            self.client = Client(**self.config)
            self.client.execute('SELECT 1')
            logger.info("✅ Kết nối ClickHouse thành công!")
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối ClickHouse: {e}")
            raise
    
    def disconnect(self) -> None:
        if self.client:
            self.client.disconnect()
            logger.info("Đóng kết nối ClickHouse")
    
    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        try:
            if params:
                self.client.execute(query, params)
            else:
                self.client.execute(query)
        except Exception as e:
            logger.error(f"❌ Lỗi thực thi truy vấn:\n{query[:100]}...\nLỗi: {e}")
            raise
            
    def check_table_exists(self, table_name: str) -> bool:
        try:
            result = self.client.execute(f"EXISTS TABLE {table_name}")
            return result[0][0] == 1
        except Exception as e:
            logger.error(f"Lỗi kiểm tra bảng: {e}")
            return False

    def create_cube_0d(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_0D (Tổng Tồn Kho)...")
        table_name = 'inv_cube_0d_storage'
        view_name = 'inv_cube_0d'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY tuple();
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT SUM(soLuongTonKho) as tongTonKho
        FROM Fact_TonKho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT SUM(soLuongTonKho) as tongTonKho
        FROM Fact_TonKho;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_0D")

    def create_cube_1d_year(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_1D_Year (Theo Năm)...")
        table_name = 'inv_cube_1d_nam_storage'
        view_name = 'inv_cube_1d_nam'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam        Int32,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY nam;
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_1D_Year")

    def create_cube_1d_quarter(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_1D_Quarter (Theo Quý)...")
        table_name = 'inv_cube_1d_quy_storage'
        view_name = 'inv_cube_1d_quy'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            quy        Int32,
            nam        Int32,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.quy, t.nam, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam, t.quy;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.quy, t.nam, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam, t.quy;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_1D_Quarter")

    def create_cube_1d_month(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_1D_Month (Theo Tháng)...")
        table_name = 'inv_cube_1d_thang_storage'
        view_name = 'inv_cube_1d_thang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang      Int32,
            nam        Int32,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam, t.thang;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        GROUP BY t.nam, t.thang;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_1D_Month")

    def create_cube_1d_product(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_1D_Product (Theo Mặt Hàng)...")
        table_name = 'inv_cube_1d_mathang_storage'
        view_name = 'inv_cube_1d_mathang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maMH       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY maMH;
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT m.maMH, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        GROUP BY m.maMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT m.maMH, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        GROUP BY m.maMH;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_1D_Product")

    def create_cube_1d_state(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_1D_State (Theo Bang/Miền)...")
        table_name = 'inv_cube_1d_bang_storage'
        view_name = 'inv_cube_1d_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            mien       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY mien;
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY d.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY d.mien;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_1D_State")

    def create_cube_1d_city(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_1D_City (Theo Thành Phố)...")
        table_name = 'inv_cube_1d_thanhpho_storage'
        view_name = 'inv_cube_1d_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            tenThanhPho String,
            tongTonKho  Int64
        ) ENGINE = SummingMergeTree() ORDER BY tenThanhPho;
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY d.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY d.tenThanhPho;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_1D_City")

    # ============================================================
    # CUBE 2D: THỜI GIAN + MẶT HÀNG
    # ============================================================
    
    def create_cube_2d_year_product(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Year_Product (Năm × Mặt Hàng)...")
        table_name = 'inv_cube_2d_nam_mathang_storage'
        view_name = 'inv_cube_2d_nam_mathang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam        Int32,
            maMH       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, m.maMH, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        GROUP BY t.nam, m.maMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, m.maMH, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        GROUP BY t.nam, m.maMH;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Year_Product")

    def create_cube_2d_quarter_product(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Quarter_Product (Quý × Mặt Hàng)...")
        table_name = 'inv_cube_2d_quy_mathang_storage'
        view_name = 'inv_cube_2d_quy_mathang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            quy        Int32,
            nam        Int32,
            maMH       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.quy, t.nam, m.maMH, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        GROUP BY t.nam, t.quy, m.maMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.quy, t.nam, m.maMH, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        GROUP BY t.nam, t.quy, m.maMH;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Quarter_Product")

    def create_cube_2d_month_product(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Month_Product (Tháng × Mặt Hàng)...")
        table_name = 'inv_cube_2d_thang_mathang_storage'
        view_name = 'inv_cube_2d_thang_mathang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang      Int32,
            nam        Int32,
            maMH       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, maMH);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, m.maMH, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        GROUP BY t.nam, t.thang, m.maMH;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, m.maMH, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        GROUP BY t.nam, t.thang, m.maMH;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Month_Product")

    # ============================================================
    # CUBE 2D: THỜI GIAN + ĐỊA ĐIỂM
    # ============================================================
    
    def create_cube_2d_year_state(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Year_State (Năm × Miền)...")
        table_name = 'inv_cube_2d_nam_bang_storage'
        view_name = 'inv_cube_2d_nam_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam        Int32,
            mien       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, d.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, d.mien;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Year_State")

    def create_cube_2d_year_city(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Year_City (Năm × Thành Phố)...")
        table_name = 'inv_cube_2d_nam_thanhpho_storage'
        view_name = 'inv_cube_2d_nam_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            nam         Int32,
            tenThanhPho String,
            tongTonKho  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.nam, d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, d.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.nam, d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, d.tenThanhPho;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Year_City")

    def create_cube_2d_quarter_state(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Quarter_State (Quý × Miền)...")
        table_name = 'inv_cube_2d_quy_bang_storage'
        view_name = 'inv_cube_2d_quy_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            quy        Int32,
            nam        Int32,
            mien       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.quy, t.nam, d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, t.quy, d.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.quy, t.nam, d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, t.quy, d.mien;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Quarter_State")

    def create_cube_2d_quarter_city(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Quarter_City (Quý × Thành Phố)...")
        table_name = 'inv_cube_2d_quy_thanhpho_storage'
        view_name = 'inv_cube_2d_quy_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            quy         Int32,
            nam         Int32,
            tenThanhPho String,
            tongTonKho  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, quy, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.quy, t.nam, d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, t.quy, d.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.quy, t.nam, d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, t.quy, d.tenThanhPho;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Quarter_City")

    def create_cube_2d_month_state(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Month_State (Tháng × Miền)...")
        table_name = 'inv_cube_2d_thang_bang_storage'
        view_name = 'inv_cube_2d_thang_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang      Int32,
            nam        Int32,
            mien       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, t.thang, d.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, t.thang, d.mien;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Month_State")

    def create_cube_2d_month_city(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Month_City (Tháng × Thành Phố)...")
        table_name = 'inv_cube_2d_thang_thanhpho_storage'
        view_name = 'inv_cube_2d_thang_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            thang       Int32,
            nam         Int32,
            tenThanhPho String,
            tongTonKho  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (nam, thang, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT t.thang, t.nam, d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, t.thang, d.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT t.thang, t.nam, d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_ThoiGian t ON f.sk_thoiGian = t.sk_thoiGian
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY t.nam, t.thang, d.tenThanhPho;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Month_City")

    # ============================================================
    # CUBE 2D: MẶT HÀNG + ĐỊA ĐIỂM
    # ============================================================
    
    def create_cube_2d_product_state(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Product_State (Mặt Hàng × Miền)...")
        table_name = 'inv_cube_2d_mathang_bang_storage'
        view_name = 'inv_cube_2d_mathang_bang'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maMH       String,
            mien       String,
            tongTonKho Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maMH, mien);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT m.maMH, d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY m.maMH, d.mien;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT m.maMH, d.mien, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY m.maMH, d.mien;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Product_State")

    def create_cube_2d_product_city(self, drop_if_exists: bool = False) -> None:
        logger.info("📊 Tạo Inventory_Cube_2D_Product_City (Mặt Hàng × Thành Phố)...")
        table_name = 'inv_cube_2d_mathang_thanhpho_storage'
        view_name = 'inv_cube_2d_mathang_thanhpho'
        
        if drop_if_exists:
            self.execute(f"DROP TABLE IF EXISTS {view_name}")
            self.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        self.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maMH        String,
            tenThanhPho String,
            tongTonKho  Int64
        ) ENGINE = SummingMergeTree() ORDER BY (maMH, tenThanhPho);
        """)
        
        self.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        TO {table_name} AS
        SELECT m.maMH, d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY m.maMH, d.tenThanhPho;
        """)
        
        self.execute(f"""
        INSERT INTO {table_name}
        SELECT m.maMH, d.tenThanhPho, SUM(f.soLuongTonKho) as tongTonKho
        FROM Fact_TonKho f
        JOIN Dim_MatHang m ON f.sk_matHang = m.sk_matHang
        JOIN Dim_CuaHang cy ON f.sk_cuaHang = cy.sk_cuaHang
        JOIN Dim_DiaDiem d ON cy.sk_diaDiem = d.sk_diaDiem
        GROUP BY m.maMH, d.tenThanhPho;
        """)
        logger.info(f"✅ Hoàn tất Inventory_Cube_2D_Product_City")

    def create_all_cubes(self, drop_if_exists: bool = False) -> None:
        logger.info("===== BẮT ĐẦU TẠO TẤT CẢ CUBES TỒN KHO (0D, 1D & 2D) =====")
        # 0D Cubes
        self.create_cube_0d(drop_if_exists)
        # 1D Cubes
        self.create_cube_1d_year(drop_if_exists)
        self.create_cube_1d_quarter(drop_if_exists)
        self.create_cube_1d_month(drop_if_exists)
        self.create_cube_1d_product(drop_if_exists)
        self.create_cube_1d_state(drop_if_exists)
        self.create_cube_1d_city(drop_if_exists)
        # 2D Cubes - Time + Product
        self.create_cube_2d_year_product(drop_if_exists)
        self.create_cube_2d_quarter_product(drop_if_exists)
        self.create_cube_2d_month_product(drop_if_exists)
        # 2D Cubes - Time + State/City
        self.create_cube_2d_year_state(drop_if_exists)
        self.create_cube_2d_year_city(drop_if_exists)
        self.create_cube_2d_quarter_state(drop_if_exists)
        self.create_cube_2d_quarter_city(drop_if_exists)
        self.create_cube_2d_month_state(drop_if_exists)
        self.create_cube_2d_month_city(drop_if_exists)
        # 2D Cubes - Product + Location
        self.create_cube_2d_product_state(drop_if_exists)
        self.create_cube_2d_product_city(drop_if_exists)
        logger.info("===== HOÀN THÀNH TẠO CUBES TỒN KHO (0D, 1D & 2D) =====")


def main() -> None:
    logger.info("🚀 BẮT ĐẦU SCRIPT TẠO CUBE CLICKHOUSE (TỒN KHO 1D)")
    manager = InventoryCubeManager(CLICKHOUSE_CONFIG)
    
    try:
        manager.create_all_cubes(drop_if_exists=True)
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")
    finally:
        manager.disconnect()
        logger.info("✅ Xong!")

if __name__ == '__main__':
    main()
