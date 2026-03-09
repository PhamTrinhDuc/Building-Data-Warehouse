-- ================================================
-- Database: banhang_db
-- Schema cho Hệ thống bán hàng
-- ================================================

-- Tạo schema
CREATE SCHEMA IF NOT EXISTS banhang_db;

-- Sử dụng schema
SET search_path TO banhang_db;

-- Bảng văn phòng đại diện
CREATE TABLE IF NOT EXISTS vanphongdaidien (
    ma_vanphong VARCHAR(10) PRIMARY KEY,
    ten_vanphong VARCHAR(100) NOT NULL,
    dia_chi VARCHAR(255),
    thanh_pho VARCHAR(50),
    bang VARCHAR(50),
    so_dien_thoai VARCHAR(20),
    email VARCHAR(100)
);

-- Bảng cửa hàng
CREATE TABLE IF NOT EXISTS cuahang (
    ma_cuahang VARCHAR(10) PRIMARY KEY,
    ten_cuahang VARCHAR(100) NOT NULL,
    dia_chi VARCHAR(255),
    thanh_pho VARCHAR(50),
    bang VARCHAR(50),
    ma_vanphong VARCHAR(10),
    so_dien_thoai VARCHAR(20),
    dien_tich_m2 DECIMAL(10,2),
    FOREIGN KEY (ma_vanphong) REFERENCES vanphongdaidien(ma_vanphong)
);

-- Bảng mặt hàng
CREATE TABLE IF NOT EXISTS mathang (
    ma_mathang VARCHAR(10) PRIMARY KEY,
    ten_mathang VARCHAR(100) NOT NULL,
    danh_muc VARCHAR(50),
    don_gia DECIMAL(12,2) NOT NULL,
    don_vi_tinh VARCHAR(20),
    mo_ta TEXT
);

-- Bảng lưu trữ mặt hàng (tồn kho)
CREATE TABLE IF NOT EXISTS mathang_luutru (
    ma_luutru SERIAL PRIMARY KEY,
    ma_mathang VARCHAR(10) NOT NULL,
    ma_cuahang VARCHAR(10) NOT NULL,
    so_luong_ton INT DEFAULT 0,
    ngay_nhap_kho DATE,
    ngay_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ma_mathang) REFERENCES mathang(ma_mathang),
    FOREIGN KEY (ma_cuahang) REFERENCES cuahang(ma_cuahang),
    UNIQUE(ma_mathang, ma_cuahang)
);

-- Bảng đơn đặt hàng
CREATE TABLE IF NOT EXISTS dondathang (
    ma_donhang VARCHAR(20) PRIMARY KEY,
    ma_khachhang VARCHAR(10) NOT NULL,
    ngay_dat_hang DATE NOT NULL DEFAULT CURRENT_DATE,
    ngay_giao_hang DATE,
    trang_thai VARCHAR(20) DEFAULT 'Pending',
    tong_tien DECIMAL(15,2),
    ghi_chu TEXT
);

-- Bảng chi tiết đơn hàng (mặt hàng được đặt)
CREATE TABLE IF NOT EXISTS mathang_duocdat (
    ma_chi_tiet SERIAL PRIMARY KEY,
    ma_donhang VARCHAR(20) NOT NULL,
    ma_mathang VARCHAR(10) NOT NULL,
    ma_cuahang VARCHAR(10) NOT NULL,
    so_luong INT NOT NULL,
    don_gia DECIMAL(12,2) NOT NULL,
    thanh_tien DECIMAL(15,2),
    FOREIGN KEY (ma_donhang) REFERENCES dondathang(ma_donhang) ON DELETE CASCADE,
    FOREIGN KEY (ma_mathang) REFERENCES mathang(ma_mathang),
    FOREIGN KEY (ma_cuahang) REFERENCES cuahang(ma_cuahang)
);

-- Tạo index
CREATE INDEX idx_ch_vanphong ON cuahang(ma_vanphong);
CREATE INDEX idx_ch_thanh_pho ON cuahang(thanh_pho);
CREATE INDEX idx_mh_danh_muc ON mathang(danh_muc);
CREATE INDEX idx_ml_mathang ON mathang_luutru(ma_mathang);
CREATE INDEX idx_ml_cuahang ON mathang_luutru(ma_cuahang);
CREATE INDEX idx_ddh_khachhang ON dondathang(ma_khachhang);
CREATE INDEX idx_ddh_ngay ON dondathang(ngay_dat_hang);
CREATE INDEX idx_mdd_donhang ON mathang_duocdat(ma_donhang);

-- Insert dữ liệu mẫu

-- Văn phòng đại diện
INSERT INTO vanphongdaidien (ma_vanphong, ten_vanphong, dia_chi, thanh_pho, bang, so_dien_thoai, email) VALUES
('VP001', 'Van Phong Mien Bac', '100 Hoan Kiem', 'Hanoi', 'Hanoi', '02438253000', 'vpbac@store.vn'),
('VP002', 'Van Phong Mien Nam', '200 District 1', 'Ho Chi Minh', 'Ho Chi Minh', '02838291000', 'vpnam@store.vn'),
('VP003', 'Van Phong Mien Trung', '300 Hai Chau', 'Da Nang', 'Da Nang', '02363827000', 'vptrung@store.vn');

-- Cửa hàng
INSERT INTO cuahang (ma_cuahang, ten_cuahang, dia_chi, thanh_pho, bang, ma_vanphong, so_dien_thoai, dien_tich_m2) VALUES
('CH001', 'Cua Hang Dong Da', '15 Dong Da', 'Hanoi', 'Hanoi', 'VP001', '0241234567', 150.5),
('CH002', 'Cua Hang Ba Dinh', '25 Ba Dinh', 'Hanoi', 'Hanoi', 'VP001', '0241234568', 200.0),
('CH003', 'Cua Hang Quan 1', '35 District 1', 'Ho Chi Minh', 'Ho Chi Minh', 'VP002', '0281234567', 180.0),
('CH004', 'Cua Hang Quan 3', '45 District 3', 'Ho Chi Minh', 'Ho Chi Minh', 'VP002', '0281234568', 220.0),
('CH005', 'Cua Hang Hai Chau', '55 Hai Chau', 'Da Nang', 'Da Nang', 'VP003', '0361234567', 160.0);

-- Mặt hàng
INSERT INTO mathang (ma_mathang, ten_mathang, danh_muc, don_gia, don_vi_tinh, mo_ta) VALUES
('MH001', 'Laptop Dell XPS 13', 'Electronics', 25000000, 'Cai', 'Laptop cao cap'),
('MH002', 'iPhone 15 Pro', 'Electronics', 30000000, 'Cai', 'Dien thoai thong minh'),
('MH003', 'Samsung TV 55 inch', 'Electronics', 15000000, 'Cai', 'Smart TV'),
('MH004', 'Nike Air Max', 'Fashion', 3000000, 'Doi', 'Giay the thao'),
('MH005', 'Adidas Hoodie', 'Fashion', 1500000, 'Cai', 'Ao khoac the thao'),
('MH006', 'Sony Headphones', 'Electronics', 5000000, 'Cai', 'Tai nghe chong on'),
('MH007', 'Canon EOS R6', 'Electronics', 45000000, 'Cai', 'May anh mirrorless'),
('MH008', 'Levi Jeans', 'Fashion', 2000000, 'Cai', 'Quan jeans cao cap');

-- Tồn kho
INSERT INTO mathang_luutru (ma_mathang, ma_cuahang, so_luong_ton, ngay_nhap_kho) VALUES
('MH001', 'CH001', 15, '2024-01-01'),
('MH001', 'CH003', 20, '2024-01-01'),
('MH002', 'CH001', 25, '2024-01-05'),
('MH002', 'CH002', 30, '2024-01-05'),
('MH002', 'CH003', 35, '2024-01-05'),
('MH003', 'CH003', 10, '2024-01-10'),
('MH003', 'CH004', 12, '2024-01-10'),
('MH004', 'CH001', 50, '2024-01-15'),
('MH004', 'CH005', 40, '2024-01-15'),
('MH005', 'CH002', 60, '2024-01-20'),
('MH006', 'CH003', 18, '2024-02-01'),
('MH007', 'CH001', 5, '2024-02-05'),
('MH008', 'CH004', 75, '2024-02-10');

-- Đơn đặt hàng
INSERT INTO dondathang (ma_donhang, ma_khachhang, ngay_dat_hang, ngay_giao_hang, trang_thai, tong_tien) VALUES
('DH001', 'KH001', '2024-03-01', '2024-03-05', 'Completed', 30000000),
('DH002', 'KH002', '2024-03-02', '2024-03-06', 'Completed', 48000000),
('DH003', 'KH003', '2024-03-03', NULL, 'Pending', 5000000),
('DH004', 'KH004', '2024-03-04', '2024-03-08', 'Shipped', 27000000),
('DH005', 'KH005', '2024-03-05', NULL, 'Pending', 16500000);

-- Chi tiết đơn hàng
INSERT INTO mathang_duocdat (ma_donhang, ma_mathang, ma_cuahang, so_luong, don_gia, thanh_tien) VALUES
('DH001', 'MH002', 'CH001', 1, 30000000, 30000000),
('DH002', 'MH002', 'CH003', 1, 30000000, 30000000),
('DH002', 'MH006', 'CH003', 3, 5000000, 15000000),
('DH002', 'MH004', 'CH001', 1, 3000000, 3000000),
('DH003', 'MH006', 'CH003', 1, 5000000, 5000000),
('DH004', 'MH001', 'CH001', 1, 25000000, 25000000),
('DH004', 'MH008', 'CH004', 1, 2000000, 2000000),
('DH005', 'MH003', 'CH003', 1, 15000000, 15000000),
('DH005', 'MH005', 'CH002', 1, 1500000, 1500000);

-- Thông báo
SELECT 'banhang_db schema created successfully!' as status;
