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
class ClickHouseCubeManager:
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
    
    def create_cube_1d_store(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo cửa hàng
        
        TODO: Hãy viết cube 1D theo cửa hàng
        - Nhóm theo: maCH
        - Metrics: SUM(thanhTien), SUM(soLuongDat), COUNT(*)
        """
        logger.info("📊 [TODO] Tạo Cube_1D_Store (Theo Cửa Hàng)...")
        
        # ============================================
        # PLACEHOLDER: VIẾT CUBE 1D THEO CỬA HÀNG
        # ============================================
        # TODO: Thay thế phần này bằng code của bạn
        # Gợi ý:
        # 1. Tạo bảng storage: cube_1d_store_storage với columns: maCH, tongDoanhThu, tongSoLuong, soLanDat
        # 2. Tạo materialized view: cube_1d_store chọn từ Fact_DonDatHang và nhóm theo maCH
        # 3. Sử dụng SummingMergeTree engine
        
        pass
    
    def create_cube_1d_product(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo sản phẩm (mặt hàng)
        
        TODO: Hãy viết cube 1D theo sản phẩm
        - Nhóm theo: maMH
        - Metrics: SUM(thanhTien), SUM(soLuongDat), COUNT(*)
        """
        logger.info("📊 [TODO] Tạo Cube_1D_Product (Theo Sản Phẩm)...")
        
        # ============================================
        # PLACEHOLDER: VIẾT CUBE 1D THEO SẢN PHẨM
        # ============================================
        # TODO: Thay thế phần này bằng code của bạn
        # Gợi ý:
        # 1. Tạo bảng storage: cube_1d_product_storage với columns: maMH, tongDoanhThu, tongSoLuong, soLanDat
        # 2. Tạo materialized view: cube_1d_product chọn từ Fact_DonDatHang và nhóm theo maMH
        # 3. Sử dụng SummingMergeTree engine
        
        pass
    
    def create_cube_1d_time(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 1D: Theo thời gian (ngày)
        
        TODO: Hãy viết cube 1D theo thời gian
        - Nhóm theo: toDate(ngayDatHang)
        - Metrics: SUM(thanhTien), SUM(soLuongDat), COUNT(*)
        """
        logger.info("📊 [TODO] Tạo Cube_1D_Time (Theo Ngày)...")
        
        # ============================================
        # PLACEHOLDER: VIẾT CUBE 1D THEO THỜI GIAN
        # ============================================
        # TODO: Thay thế phần này bằng code của bạn
        # Gợi ý:
        # 1. Tạo bảng storage: cube_1d_time_storage với columns: ngay (Date), tongDoanhThu, tongSoLuong, soLanDat
        # 2. Tạo materialized view: cube_1d_time chọn từ Fact_DonDatHang, nhóm theo toDate(ngayDatHang)
        # 3. Sử dụng SummingMergeTree engine với ORDER BY ngay
        
        pass
    
    def create_cube_2d_store_product(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 2D: Theo cửa hàng và sản phẩm
        
        TODO: Hãy viết cube 2D theo cửa hàng + sản phẩm
        - Nhóm theo: maCH, maMH
        - Metrics: SUM(thanhTien), SUM(soLuongDat), COUNT(*)
        """
        logger.info("📊 [TODO] Tạo Cube_2D_Store_Product (Cửa Hàng + Sản Phẩm)...")
        
        # ============================================
        # PLACEHOLDER: VIẾT CUBE 2D STORE + PRODUCT
        # ============================================
        # TODO: Thay thế phần này bằng code của bạn
        # Gợi ý:
        # 1. Tạo bảng storage: cube_2d_store_product_storage với columns: maCH, maMH, tongDoanhThu, tongSoLuong, soLanDat
        # 2. Tạo materialized view: cube_2d_store_product từ Fact_DonDatHang, nhóm theo maCH, maMH
        # 3. Sử dụng SummingMergeTree engine với ORDER BY maCH, maMH
        
        pass
    
    def create_cube_2d_store_time(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 2D: Theo cửa hàng và thời gian
        
        TODO: Hãy viết cube 2D theo cửa hàng + thời gian
        - Nhóm theo: maCH, toDate(ngayDatHang)
        - Metrics: SUM(thanhTien), SUM(soLuongDat), COUNT(*)
        """
        logger.info("📊 [TODO] Tạo Cube_2D_Store_Time (Cửa Hàng + Thời Gian)...")
        
        # ============================================
        # PLACEHOLDER: VIẾT CUBE 2D STORE + TIME
        # ============================================
        # TODO: Thay thế phần này bằng code của bạn
        # Gợi ý:
        # 1. Tạo bảng storage: cube_2d_store_time_storage
        # 2. Tạo materialized view: cube_2d_store_time từ Fact_DonDatHang
        # 3. Nhóm theo maCH và ngày
        
        pass
    
    def create_cube_2d_product_time(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 2D: Theo sản phẩm và thời gian
        
        TODO: Hãy viết cube 2D theo sản phẩm + thời gian
        - Nhóm theo: maMH, toDate(ngayDatHang)
        - Metrics: SUM(thanhTien), SUM(soLuongDat), COUNT(*)
        """
        logger.info("📊 [TODO] Tạo Cube_2D_Product_Time (Sản Phẩm + Thời Gian)...")
        
        # ============================================
        # PLACEHOLDER: VIẾT CUBE 2D PRODUCT + TIME
        # ============================================
        # TODO: Thay thế phần này bằng code của bạn
        # Gợi ý:
        # 1. Tạo bảng storage: cube_2d_product_time_storage
        # 2. Tạo materialized view: cube_2d_product_time từ Fact_DonDatHang
        # 3. Nhóm theo maMH và ngày
        
        pass
    
    def create_cube_3d_store_product_time(self, drop_if_exists: bool = False) -> None:
        """
        Tạo Cube 3D: Theo cửa hàng, sản phẩm và thời gian
        
        TODO: Hãy viết cube 3D theo cửa hàng + sản phẩm + thời gian
        - Nhóm theo: maCH, maMH, toDate(ngayDatHang)
        - Metrics: SUM(thanhTien), SUM(soLuongDat), COUNT(*)
        """
        logger.info("📊 [TODO] Tạo Cube_3D_Store_Product_Time (Cửa Hàng + Sản Phẩm + Thời Gian)...")
        
        # ============================================
        # PLACEHOLDER: VIẾT CUBE 3D FULL
        # ============================================
        # TODO: Thay thế phần này bằng code của bạn
        # Gợi ý:
        # 1. Tạo bảng storage: cube_3d_store_product_time_storage
        # 2. Tạo materialized view: cube_3d_store_product_time từ Fact_DonDatHang
        # 3. Nhóm theo maCH, maMH, toDate(ngayDatHang)
        # 4. Có thể sử dụng PARTITION BY maCH để tối ưu
        
        pass
    
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
            
            # Cube 1D
            self.create_cube_1d_store(drop_if_exists=drop_if_exists)
            self.create_cube_1d_product(drop_if_exists=drop_if_exists)
            self.create_cube_1d_time(drop_if_exists=drop_if_exists)
            
            # Cube 2D
            self.create_cube_2d_store_product(drop_if_exists=drop_if_exists)
            self.create_cube_2d_store_time(drop_if_exists=drop_if_exists)
            self.create_cube_2d_product_time(drop_if_exists=drop_if_exists)
            
            # Cube 3D
            self.create_cube_3d_store_product_time(drop_if_exists=drop_if_exists)
            
            logger.info("=" * 60)
            logger.info("✅ HOÀN THÀNH TẠO CÁC CUBE")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error(f"❌ Lỗi tạo cube: {e}")
            raise

# ============================================
# HÀM MAIN
# ============================================
def main():
    """Hàm chính"""
    
    logger.info("🚀 BẮT ĐẦU SCRIPT TẠO CUBE CLICKHOUSE")
    
    # Tạo manager
    manager = ClickHouseCubeManager(CLICKHOUSE_CONFIG)
    
    try:
        # Tạo tất cả cube
        # Để xóa cube cũ trước khi tạo mới, set drop_if_exists=True
        # manager.create_all_cubes(drop_if_exists=False)
        manager.create_cube_0d(drop_if_exists=True)
        
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")
        sys.exit(1)
    
    finally:
        manager.disconnect()

if __name__ == '__main__':
    main()
