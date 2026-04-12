# Báo cáo Triển khai Kỹ thuật: Indexing & Metadata (Khai Phá Dữ Liệu)

Tài liệu này giải thích chi tiết luồng thực thi và ý nghĩa nghiệp vụ của 2 script hỗ trợ quan trọng trong quá trình xây dựng Kho Dữ Liệu (Data Warehouse): `create_metadata.py` và `create_index.py`. 

Trong kiến trúc hệ thống DW của đồ án, hai thành phần này giải quyết trực tiếp 2 bài toán lớn trong môn học: **Quản lý siêu dữ liệu (Metadata)** và **Tối ưu hóa hiệu năng trích xuất (Indexing)**.

---

## 1. Quản lý Siêu Dữ Liệu (Metadata) - `create_metadata.py`

Siêu dữ liệu (Metadata) là "dữ liệu mô tả về dữ liệu". Quá trình triển khai metadata trong đồ án được chia làm 2 nhánh, hoàn toàn tự động thông qua Python:

### 1.1. Technical Metadata (Từ Điển Dữ Liệu Data Warehouse)
Đây là cấu trúc siêu dữ liệu mô tả trực tiếp các bảng chiều (Dimension), bảng sự kiện (Fact), cột, kiểu dữ liệu và vai trò của chúng trong Kho dữ liệu phân tích.
* **Cách thực thi:** Script kết nối trực tiếp vào **Data Warehouse (ClickHouse)** (qua cổng 8123) và truy vấn bảng hệ thống `system.columns`. Logic của script cũng tự động phân loại cột nào là *Surrogate Key (Khóa nhân tạo)*, cột nào là *Measure (Đo lường)* để ghi vào bản báo cáo.
* **Đầu ra:** Dữ liệu kỹ thuật này được tự động phân tích và xuất ra 2 định dạng:
  * File `docs/DW_DATA_DICTIONARY.md`: Dùng để đính kèm trực tiếp vào báo cáo.
  * File `docs/DW_DATA_DICTIONARY.csv`: Dễ dàng import vào Excel cho các bảng phụ lục.

### 1.2. Operational Metadata (Log Vận hành ETL)
Đây là siêu dữ liệu được sinh ra trong quá trình hệ thống đang chạy (Runtime), phục vụ mục đích Tracking (Giám sát).
* **Cách thực thi:** Script tự động khởi tạo cấu trúc schema `etl_metadata` và bảng `pipeline_runs`.
* **Mục đích:** Theo dõi mỗi lần luồng dữ liệu (ETL pipeline) chạy: Job nào chạy, mất bao lâu (`start_time`, `end_time`), trạng thái lỗi/thành công, và số lượng dòng dữ liệu đã di chuyển (`records_extracted`, `records_loaded`).

### 1.3. Hướng dẫn sử dụng
```bash
cd scripts
python create_metadata.py
# Cửa sổ sẽ hiện ra Menu [1], [2], [3] để bạn lựa chọn loại metadata muốn sinh rà.
```

---

## 2. Tiền xử lý & Tối ưu hiệu năng lấy dữ liệu - `create_index.py`

Việc xây dựng Kho dữ liệu bắt buộc chúng ta phải quét (Scan) và gộp (Join) hàng triệu bản ghi từ Database Nguồn (OLTP / IDB). Nếu không tối ưu, bước **EXTRACT** của ETL sẽ tiêu tốn lượng RAM và Thời gian khổng lồ.

### 2.1. Đánh Index trên Nguồn OLTP/IDB (PostgreSQL)
* **Cách thực thi:** Script sử dụng danh sách các lệnh `CREATE INDEX CONCURRENTLY` (đánh Index mà không lock Table) hướng thẳng vào các **Khóa Ngoại (Foreign Keys)** và các trường hay được truy vấn.
* **Các index tiêu biểu:**
  * Bảng `CuaHang`, `KhachHang`: Đánh Index cột `VanPhongDaiDienmaTP`.
  * Bảng `DonDatHang`: Đánh Index cột `KhachHangmaKH`, `ngayDatHang`.
  * Bảng `MatHangDuocTru`, `MatHangDuocDat`: Đánh Index cho khóa kết nối mặt hàng và đơn hàng/cửa hàng.
* **Lợi ích:** Đưa tốc độ thực hiện lệnh truy xuất `pd.read_sql` có chứa nhiều câu lệnh `JOIN` bên trong (pandas/SQLalchemy) từ độ phức tạp $O(N)$ xuống $O(log N)$ bằng cấu trúc dữ liệu **B-Tree**. 

### 2.2. Trả lời câu hỏi: "Tại sao không đánh CREATE INDEX vào Data Warehouse?"
Kho cơ sở dữ liệu phân tích (OLAP Data Warehouse) của chúng ta được triển khai trên **ClickHouse**. Bản chất kiến trúc của nó là **Columnar Database** nên nó xử lý Index hoàn toàn khác so với PostgreSQL (B-Tree):
1. **Tự động Sparse Index:** Khi định nghĩa DDL tạo bảng `MergeTree` bằng lệnh `ORDER BY sk_khachHang`, ClickHouse tự động tạo ra Primary Sparse Index theo cột đó, giữ dữ liệu được Sort (sắp xếp) sẵn trên đĩa. Do đó, việc chạy `CREATE INDEX` giống hệt DB nguồn là KHÔNG CẦN THIẾT.
2. **Data Skipping Indexes:** Đây là dạng Index chuyên sâu (tùy chọn thêm của ClickHouse) như `bloom_filter` hay `minmax` để trực tiếp bỏ qua các block dữ liệu không thỏa mãn điều kiện bộ lọc `WHERE`. Tuy nhiên, với Scale của sinh viên, chỉ cần Sorting Index sẵn có của `ORDER BY` đã đủ quét vài trăm triệu dòng trong khoảng mili-giây.

*(Lưu ý này được đưa vào báo cáo để ghi điểm về Tư duy Kiến trúc và Hiểu biết sâu sắc về các Hệ quản trị CSDL chuyên biệt).*

### 2.3. Hướng dẫn sử dụng
```bash
cd scripts
python create_index.py
# Kịch bản sẽ tự động chèn B-Tree Index vào IDB/OLTP.
```
