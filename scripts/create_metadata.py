import os
import pandas as pd
from sqlalchemy import create_engine, text
import clickhouse_connect
from datetime import datetime

# Cấu hình kết nối
PG_DSN = "postgresql://admin:admin@localhost:5433/idb" # Có thể là IDB hoặc Postgres tùy bạn lưu metadata điều khiển ở đâu
CH_HOST = "localhost"
CH_PORT = 8123

# Từ điển ánh xạ tự động mô tả chung (Business Metadata) để add vào file báo cáo
BUSINESS_DICT = {
    "sk_thoiGian": "Khóa thay thế (Surrogate Key) Dimension Thời Gian",
    "sk_diaDiem": "Khóa thay thế (Surrogate Key) Dimension Địa Điểm",
    "sk_matHang": "Khóa thay thế (Surrogate Key) Dimension Mặt Hàng",
    "sk_khachHang": "Khóa thay thế (Surrogate Key) Dimension Khách Hàng",
    "sk_cuaHang": "Khóa thay thế (Surrogate Key) Dimension Cửa Hàng",
    "maMH": "Mã mặt hàng (Natural Key)",
    "tenMH": "Tên mặt hàng",
    "moTa": "Mô tả chi tiết mặt hàng",
    "kichCo": "Kích cỡ mặt hàng",
    "trongLuong": "Trọng lượng mặt hàng",
    "gia": "Giá bán niêm yết hiện tại",
    "ngayMoBan": "Ngày mặt hàng bắt đầu được phân phối",
    "maTP": "Mã Thành Phố/Văn Phòng Đại Diện (Natural Key)",
    "mien": "Miền / Bang khu vực",
    "tenThanhPho": "Tên thành phố của Cửa hàng/Văn phòng",
    "diaChiVP": "Địa chỉ văn phòng đại diện quản lý",
    "ngayThanhLapVP": "Ngày thành lập văn phòng",
    "maCH": "Mã chi nhánh Cửa Hàng (Natural Key)",
    "soDienThoai": "Số điện thoại cửa hàng",
    "maKH": "Mã Khách Hàng (Natural Key)",
    "tenKH": "Họ tên đầy đủ của khách hàng",
    "loaiKhachHang": "Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai)",
    "huongDanVien": "Tên hướng dẫn viên (Dành cho KH du lịch)",
    "diaChiBuuDien": "Địa chỉ hòm thư (Dành cho KH bưu điện)",
    "ngayDatHangDauTien": "Ngày khách hàng phát sinh đơn đầu tiên",
    "ngay": "Ngày", "thang": "Tháng", "quy": "Quý", "nam": "Năm",
    "soLuongDat": "Số lượng mặt hàng được đặt trong đơn",
    "giaDat": "Đơn giá của mặt hàng tại đúng thời điểm đặt hàng (có thể khác giá niêm yết)",
    "thanhTien": "Doanh thu của mặt hàng trong đơn (Số lượng × Giá đặt)",
    "soLuongTon": "Số lượng tồn kho được ghi nhận tại thời điểm snapshot",
    "tongDoanhThu": "Tổng doanh thu bán hàng sau khi quy đổi",
    "tongSoLuong": "Tổng số lượng hàng hóa được đặt",
    "soLanDat": "Tổng số lượt/đơn phát sinh",
    "soLuongTonKho": "Tổng mức quy mô tồn kho hiện tại được tổng hợp"
}

def get_auto_description(col_name):
    """Lấy mô tả tự động (Business description) từ từ điển định nghĩa sẵn"""
    if col_name in BUSINESS_DICT:
        return BUSINESS_DICT[col_name]
    return "Thuộc tính phân tích"

def create_etl_operational_metadata_table():
    """
    1. Operational Metadata:
    Tạo bảng ghi log các lần chạy ETL. Logs điều khiển ETL thường tạo ở hệ quản trị gốc hoặc hệ quản trị điều khiển (như Postgres).
    """
    engine = create_engine(PG_DSN)
    
    sql_create_table = """
    CREATE SCHEMA IF NOT EXISTS etl_metadata;
    
    CREATE TABLE IF NOT EXISTS etl_metadata.pipeline_runs (
        run_id SERIAL PRIMARY KEY,
        job_name VARCHAR(100),
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        status VARCHAR(20),
        records_extracted INT DEFAULT 0,
        records_loaded INT DEFAULT 0,
        error_message TEXT
    );
    """
    
    with engine.begin() as conn:
        conn.execute(text(sql_create_table))
    
    print("✅ Đã tạo bảng ETL Operational Metadata trên PostgreSQL: etl_metadata.pipeline_runs")

def generate_technical_metadata():
    """
    2. Technical Metadata (Dành cho Data Warehouse - ClickHouse):
    Tự động quét cấu trúc Kho dữ liệu (các bảng Dim, Fact) trong ClickHouse để làm Từ điển dữ liệu (Data Dictionary)
    """
    try:
        # Kết nối tới ClickHouse
        client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username='admin', password='admin')
        
        # Lấy metadata từ hệ thống của ClickHouse
        query = """
        SELECT 
            database AS table_schema, 
            table AS table_name, 
            name AS column_name, 
            type AS data_type, 
            comment AS column_comment
        FROM system.columns 
        WHERE database = 'default' 
          AND table NOT LIKE '.inner%' 
          AND table NOT LIKE 'cube_%'
          AND table NOT LIKE 'inv_cube_%'
        ORDER BY table_name, position
        """
        
        df = client.query_df(query)
        
        if df.empty:
            print("⚠️ Không tìm thấy bảng nào trong Data Warehouse (ClickHouse). Bạn đã chạy init_dw.sql chưa?")
            return
        
        # Bổ sung sẵn vai trò và mô tả vào DataFrame
        df['role'] = df['column_name'].apply(
            lambda col_name: "Measure (Đo lường)" if col_name.startswith('soLuong') or col_name in ['gia', 'thanhTien', 'tongDoanhThu']
            else "Surrogate Key (Khóa nhân tạo)" if 'sk_' in col_name
            else "Dimension Attribute (Thuộc tính)"
        )
        
        df['column_comment'] = df.apply(
            lambda row: row['column_comment'] if pd.notna(row['column_comment']) and str(row['column_comment']).strip() != ''
            else get_auto_description(row['column_name']), axis=1
        )
        
        # Tạo file CSV
        csv_out_path = os.path.join(os.path.dirname(__file__), "../docs/DW_DATA_DICTIONARY.csv")
        df.to_csv(csv_out_path, index=False, encoding='utf-8')
        print(f"✅ Đã tạo file DW Data Dictionary (CSV) tại: {csv_out_path}")
        
        # Bắt đầu ghi file Markdown
        md_content = "# Từ Điển Kho Dữ Liệu (Technical Metadata - Data Warehouse)\n"
        md_content += f"*Được tạo tự động từ ClickHouse vào lúc: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n"
        
        # Lọc ra các bảng Fact và Dim
        for table, grouped_table in df.groupby('table_name'):
            md_content += f"### Bảng DW: `{table}`\n"
            md_content += "| Tên Cột | Kiểu Dữ Liệu (ClickHouse) | Vai Trò (Mapping/Surrogate/Measure) | Ghi Chú |\n"
            md_content += "|---|---|---|---|\n"
            
            for _, row in grouped_table.iterrows():
                md_content += f"| {row['column_name']} | {row['data_type']} | {row['role']} | {row['column_comment']} |\n"
            
            md_content += "\n"
        
        md_out_path = os.path.join(os.path.dirname(__file__), "../docs/DW_DATA_DICTIONARY.md")
        with open(md_out_path, "w", encoding='utf-8') as f:
            f.write(md_content)
            
        print(f"✅ Đã tạo file DW Data Dictionary (Markdown) tại: {md_out_path}")
        
    except Exception as e:
        print(f"❌ Lỗi chạy Technical Metadata (Data Warehouse): {e}")

def generate_cube_metadata():
    """
    3. Cube Metadata (Dành cho OLAP Cubes - ClickHouse):
    Tự động quét cấu trúc các OLAP Cubes đã tạo để làm Từ điển dữ liệu báo cáo
    """
    try:
        # Kết nối tới ClickHouse
        client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username='admin', password='admin')
        
        # Chỉ lấy các bảng/view là Cube
        query = """
        SELECT 
            database AS table_schema, 
            table AS table_name, 
            name AS column_name, 
            type AS data_type, 
            comment AS column_comment
        FROM system.columns 
        WHERE database = 'default' 
          AND (table LIKE 'cube_%' OR table LIKE 'inv_cube_%')
          AND table NOT LIKE '%_storage' -- Bỏ qua bảng storage vật lý nếu chỉ muốn báo cáo View
        ORDER BY table_name, position
        """
        
        df = client.query_df(query)
        
        if df.empty:
            print("⚠️ Không tìm thấy Cube nào trong ClickHouse. Bạn đã chạy script create_cubes chưa?")
            return
            
        # Bổ sung sẵn vai trò và mô tả vào DataFrame
        df['role'] = df['column_name'].apply(
            lambda col_name: "Measure (Đo lường, Tổng/Đếm)" if col_name.startswith('tong') or col_name.startswith('soLan') or col_name.startswith('soLuong') 
            else "Dimension Attribute (Chiều phân tích)"
        )
        
        df['column_comment'] = df.apply(
            lambda row: row['column_comment'] if pd.notna(row['column_comment']) and str(row['column_comment']).strip() != '' 
            else get_auto_description(row['column_name']), axis=1
        )
        
        # Tạo file CSV
        csv_out_path = os.path.join(os.path.dirname(__file__), "../docs/CUBE_DATA_DICTIONARY.csv")
        df.to_csv(csv_out_path, index=False, encoding='utf-8')
        print(f"✅ Đã tạo file OLAP Cube Dictionary (CSV) tại: {csv_out_path}")
        
        # Bắt đầu ghi file Markdown
        md_content = "# Từ Điển OLAP Cubes (Cube Metadata)\n"
        md_content += f"*Được tạo tự động từ ClickHouse vào lúc: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n"
        
        for table, grouped_table in df.groupby('table_name'):
            md_content += f"### Bảng Cube/Materialized View: `{table}`\n"
            md_content += "| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |\n"
            md_content += "|---|---|---|---|\n"
            
            for _, row in grouped_table.iterrows():
                md_content += f"| {row['column_name']} | {row['data_type']} | {row['role']} | {row['column_comment']} |\n"
            
            md_content += "\n"
        
        md_out_path = os.path.join(os.path.dirname(__file__), "../docs/CUBE_DATA_DICTIONARY.md")
        with open(md_out_path, "w", encoding='utf-8') as f:
            f.write(md_content)
        print(f"✅ Đã tạo file OLAP Cube Dictionary (Markdown) tại: {md_out_path}")

    except Exception as e:
        print(f"❌ Lỗi chạy Cube Metadata: {e}")

if __name__ == "__main__":
    print("Chọn chức năng tạo Metadata:")
    print("1: Tạo bảng ghi log ETL (Operational Metadata - trên PostgreSQL)")
    print("2: Xuất tài liệu Cấu trúc KHO DỮ LIỆU (Technical Metadata - từ ClickHouse)")
    print("3: Xuất tài liệu OLAP CUBES (Cube Metadata - từ ClickHouse)")
    print("4: Chạy TẤT CẢ các chức năng trên")
    
    choice = input("Lựa chọn (1, 2, 3 hoặc 4): ")
    if choice == '1':
        create_etl_operational_metadata_table()
    elif choice == '2':
        generate_technical_metadata()
    elif choice == '3':
        generate_cube_metadata()
    elif choice == '4':
        create_etl_operational_metadata_table()
        generate_technical_metadata()
        generate_cube_metadata()
    else:
        print("Lựa chọn không hợp lệ.")
