# Từ Điển Kho Dữ Liệu (Technical Metadata - Data Warehouse)
*Được tạo tự động từ ClickHouse vào lúc: 2026-04-12 22:32:37*

### Bảng DW: `Dim_CuaHang`
| Tên Cột | Kiểu Dữ Liệu (ClickHouse) | Vai Trò (Mapping/Surrogate/Measure) | Ghi Chú |
|---|---|---|---|
| sk_cuaHang | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Cửa Hàng |
| maCH | String | Dimension Attribute (Thuộc tính) | Mã chi nhánh Cửa Hàng (Natural Key) |
| soDienThoai | String | Dimension Attribute (Thuộc tính) | Số điện thoại cửa hàng |
| ngayThanhLapCH | Nullable(Date) | Dimension Attribute (Thuộc tính) | Thuộc tính phân tích |
| sk_diaDiem | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Địa Điểm |

### Bảng DW: `Dim_DiaDiem`
| Tên Cột | Kiểu Dữ Liệu (ClickHouse) | Vai Trò (Mapping/Surrogate/Measure) | Ghi Chú |
|---|---|---|---|
| sk_diaDiem | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Địa Điểm |
| maTP | String | Dimension Attribute (Thuộc tính) | Mã Thành Phố/Văn Phòng Đại Diện (Natural Key) |
| mien | String | Dimension Attribute (Thuộc tính) | Miền / Bang khu vực |
| diaChiVP | String | Dimension Attribute (Thuộc tính) | Địa chỉ văn phòng đại diện quản lý |
| ngayThanhLapVP | Nullable(Date) | Dimension Attribute (Thuộc tính) | Ngày thành lập văn phòng |
| tenThanhPho | String | Dimension Attribute (Thuộc tính) | Tên thành phố của Cửa hàng/Văn phòng |

### Bảng DW: `Dim_KhachHang`
| Tên Cột | Kiểu Dữ Liệu (ClickHouse) | Vai Trò (Mapping/Surrogate/Measure) | Ghi Chú |
|---|---|---|---|
| sk_khachHang | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Khách Hàng |
| sk_diaDiem | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Địa Điểm |
| maKH | String | Dimension Attribute (Thuộc tính) | Mã Khách Hàng (Natural Key) |
| tenKH | String | Dimension Attribute (Thuộc tính) | Họ tên đầy đủ của khách hàng |
| ngayDatHangDauTien | Nullable(Date) | Dimension Attribute (Thuộc tính) | Ngày khách hàng phát sinh đơn đầu tiên |
| huongDanVien | Nullable(String) | Dimension Attribute (Thuộc tính) | Tên hướng dẫn viên (Dành cho KH du lịch) |
| diaChiBuuDien | Nullable(String) | Dimension Attribute (Thuộc tính) | Địa chỉ hòm thư (Dành cho KH bưu điện) |
| loaiKhachHang | String | Dimension Attribute (Thuộc tính) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |

### Bảng DW: `Dim_MatHang`
| Tên Cột | Kiểu Dữ Liệu (ClickHouse) | Vai Trò (Mapping/Surrogate/Measure) | Ghi Chú |
|---|---|---|---|
| sk_matHang | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Mặt Hàng |
| tenMH | String | Dimension Attribute (Thuộc tính) | Tên mặt hàng |
| maMH | String | Dimension Attribute (Thuộc tính) | Mã mặt hàng (Natural Key) |
| moTa | String | Dimension Attribute (Thuộc tính) | Mô tả chi tiết mặt hàng |
| kichCo | String | Dimension Attribute (Thuộc tính) | Kích cỡ mặt hàng |
| trongLuong | Float64 | Dimension Attribute (Thuộc tính) | Trọng lượng mặt hàng |
| gia | Float64 | Measure (Đo lường) | Giá bán niêm yết hiện tại |
| ngayMoBan | Nullable(Date) | Dimension Attribute (Thuộc tính) | Ngày mặt hàng bắt đầu được phân phối |

### Bảng DW: `Dim_ThoiGian`
| Tên Cột | Kiểu Dữ Liệu (ClickHouse) | Vai Trò (Mapping/Surrogate/Measure) | Ghi Chú |
|---|---|---|---|
| sk_thoiGian | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Thời Gian |
| ngay | Date | Dimension Attribute (Thuộc tính) | Ngày |
| thang | Int8 | Dimension Attribute (Thuộc tính) | Tháng |
| quy | Int8 | Dimension Attribute (Thuộc tính) | Quý |
| nam | Int16 | Dimension Attribute (Thuộc tính) | Năm |

### Bảng DW: `Fact_DonDatHang`
| Tên Cột | Kiểu Dữ Liệu (ClickHouse) | Vai Trò (Mapping/Surrogate/Measure) | Ghi Chú |
|---|---|---|---|
| sk_khachHang | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Khách Hàng |
| sk_matHang | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Mặt Hàng |
| sk_thoiGian | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Thời Gian |
| maDon | String | Dimension Attribute (Thuộc tính) | Thuộc tính phân tích |
| soLuongDat | Int32 | Measure (Đo lường) | Số lượng mặt hàng được đặt trong đơn |
| giaDat | Float64 | Dimension Attribute (Thuộc tính) | Đơn giá của mặt hàng tại đúng thời điểm đặt hàng (có thể khác giá niêm yết) |
| thanhTien | Float64 | Measure (Đo lường) | Doanh thu của mặt hàng trong đơn (Số lượng × Giá đặt) |

### Bảng DW: `Fact_TonKho`
| Tên Cột | Kiểu Dữ Liệu (ClickHouse) | Vai Trò (Mapping/Surrogate/Measure) | Ghi Chú |
|---|---|---|---|
| sk_thoiGian | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Thời Gian |
| sk_matHang | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Mặt Hàng |
| sk_cuaHang | Int32 | Surrogate Key (Khóa nhân tạo) | Khóa thay thế (Surrogate Key) Dimension Cửa Hàng |
| soLuongTonKho | Int32 | Measure (Đo lường) | Tổng mức quy mô tồn kho hiện tại được tổng hợp |

