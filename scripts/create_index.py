import psycopg2
import psycopg2.extras
import time

# Cấu hình kết nối IDB/OLTP (PostgreSQL)
DB_CONFIG = {
    'dbname': 'idb', # Tùy chỉnh thành 'idb' hoặc 'postgres' theo cấu hình trong docker/local
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': '5433'
}

def create_indexes():
    """
    Tạo Index trên các Data Source (OLTP/IDB) để tối ưu hóa quá trình Truy xuất dữ liệu (Extraction) cho ETL.
    """
    print("=" * 50)
    print("🚀 BẮT ĐẦU TẠO INDEX TỐI ƯU HÓA ETL")
    print("=" * 50)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Danh sách các schema và các lệnh tạo Index (Tập trung vào các Khóa ngoại FK và trường tìm kiếm nhiều)
        index_queries = [
            # ==== LƯU Ý: Chỉ định rõ schema name nếu Project của bạn chứa dữ liệu ở sơ đồ cụ thể (ví dụ idb, banhang_db...) ====
            # Bảng CuaHang - Tìm kiếm theo Văn Phòng
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cuahang_vanphong ON idb.CuaHang(VanPhongDaiDienmaTP);",
            # Bảng KhachHang - Tìm kiếm theo Văn Phòng
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_khachhang_vanphong ON idb.KhachHang(VanPhongDaiDienmaTP);",
            # Bảng DonDatHang - Tìm kiếm theo Khách Hàng và Ngày đặt
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_donhang_khachhang ON idb.DonDatHang(KhachHangmaKH);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_donhang_ngaydat ON idb.DonDatHang(ngayDatHang);",
            # Bảng MatHangDuocTru - Tìm kiếm theo Mã Cửa Hàng và Mã Mặt Hàng (tối ưu hóa JOIN cho ETL/Fact Ton Kho)
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mathang_tru_cuahang ON idb.MatHangDuocTru(CuaHangmaCH);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mathang_tru_mamh ON idb.MatHangDuocTru(MatHangmaMH);",
            # Bảng MatHangDuocDat - Tìm kiếm theo Đơn Hàng và Mặt Hàng (Tối ưu hóa JOIN Fact_DonDatHang)
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mathang_dat_donhang ON idb.MatHangDuocDat(DonDatHangmaDon);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mathang_dat_mamh ON idb.MatHangDuocDat(MatHangmaMH);"
            # Nếu có schema banhang_db / vanphong_db thì có thể duplicate list query ở trên, đổi tên schema
        ]

        print("Đang tạo các Indexes (B-Tree) để tăng tốc độ Hash Join/Merge Join trong Pandas SQLAlchemy...")
        start_time = time.time()
        
        success_count = 0
        for i, query in enumerate(index_queries, 1):
            try:
                cursor.execute(query)
                print(f"  [{i}/{len(index_queries)}] ✅ Đã thực thi rà soát/tạo index.")
                success_count += 1
            except Exception as e:
                # Bỏ qua nếu bảng/schema chưa tồn tại (tùy vào luồng chạy trước đó)
                print(f"  [{i}/{len(index_queries)}] ❌ Bỏ qua index này (Lỗi: {str(e).strip()})")

        end_time = time.time()
        print("-" * 50)
        print(f"🎉 Hoàn thành! Quá trình tạo/kiểm tra {success_count}/{len(index_queries)} indexes mất {end_time - start_time:.2f} giây.")
        print("💡 Index B-Tree sẽ giúp quá trình pd.read_sql (Extract) trong ETL pipeline nhanh hơn đáng kể.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n❌ Lỗi kết nối CSDL: {e}")
        print("Vui lòng kiểm tra lại cấu hình DB_CONFIG (dbname, host, port, user, password).")

if __name__ == "__main__":
    create_indexes()