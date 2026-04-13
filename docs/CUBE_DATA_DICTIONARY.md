# Từ Điển OLAP Cubes (Cube Metadata)
*Được tạo tự động từ ClickHouse vào lúc: 2026-04-13 20:57:38*

### Bảng Cube/Materialized View: `cube_0d`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_1d_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_1d_loai_kh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_1d_ma_kh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_1d_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_1d_nam`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_1d_quy`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_1d_thang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_1d_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_loai_kh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_loai_kh_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_loai_kh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_ma_kh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_ma_kh_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_ma_kh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_ma_mh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_ma_mh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_nam_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_nam_loai_kh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_nam_ma_kh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_nam_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_nam_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_quy_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_quy_loai_kh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_quy_ma_kh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_quy_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_quy_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_thang_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_thang_loai_kh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_thang_ma_kh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_thang_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_2d_thang_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_loai_kh_ma_mh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_loai_kh_ma_mh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_ma_kh_ma_mh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_ma_kh_ma_mh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_nam_loai_kh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_nam_loai_kh_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_nam_loai_kh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_nam_ma_kh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_nam_ma_kh_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_nam_ma_kh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_nam_ma_mh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_nam_ma_mh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_quy_loai_kh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_quy_loai_kh_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_quy_loai_kh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_quy_ma_kh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_quy_ma_kh_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_quy_ma_kh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_quy_ma_mh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_quy_ma_mh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| quy | Int8 | Dimension Attribute (Chiều phân tích) | Quý |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_thang_loai_kh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_thang_loai_kh_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_thang_loai_kh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| loaiKhachHang | String | Dimension Attribute (Chiều phân tích) | Phân khúc khách hàng (Du Lịch / Bưu Điện / Cả Hai) |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_thang_ma_kh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_thang_ma_kh_ma_mh`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_thang_ma_kh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maKH | String | Dimension Attribute (Chiều phân tích) | Mã Khách Hàng (Natural Key) |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_thang_ma_mh_bang`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

### Bảng Cube/Materialized View: `cube_3d_thang_ma_mh_thanhpho`
| Tên Cột | Kiểu Dữ Liệu | Vai Trò (Dimension/Measure) | Mô Tả Ý Nghĩa Nghiệp Vụ |
|---|---|---|---|
| thang | Int8 | Dimension Attribute (Chiều phân tích) | Tháng |
| nam | Int16 | Dimension Attribute (Chiều phân tích) | Năm |
| maMH | String | Dimension Attribute (Chiều phân tích) | Mã mặt hàng (Natural Key) |
| tenMH | String | Dimension Attribute (Chiều phân tích) | Tên mặt hàng |
| tenThanhPho | String | Dimension Attribute (Chiều phân tích) | Tên thành phố của Cửa hàng/Văn phòng |
| mien | String | Dimension Attribute (Chiều phân tích) | Miền / Bang khu vực |
| tongDoanhThu | Float64 | Measure (Đo lường, Tổng/Đếm) | Tổng doanh thu bán hàng sau khi quy đổi |
| tongSoLuong | Int64 | Measure (Đo lường, Tổng/Đếm) | Tổng số lượng hàng hóa được đặt |

