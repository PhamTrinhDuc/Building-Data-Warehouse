-- ================================================
-- Database: idb (Integrated Database)
-- Schema tích hợp - Quản lý tổng hợp
-- ================================================

-- Tạo schema
CREATE SCHEMA IF NOT EXISTS oltp;

-- Sử dụng schema
SET search_path TO oltp;

-- Bảng Văn phòng đại diện
CREATE TABLE IF NOT EXISTS VanPhongDaiDien (
    maTP VARCHAR(10) PRIMARY KEY,
    tenThanhPho VARCHAR(255) NOT NULL,
    diaChiVP VARCHAR(255),
    bang VARCHAR(255),
    ngayThanhLapVP DATE
);

-- Bảng Cửa hàng
CREATE TABLE IF NOT EXISTS CuaHang (
    maCH VARCHAR(10) PRIMARY KEY,
    soDienThoai VARCHAR(10),
    ngayThanhLapCH DATE,
    VanPhongDaiDienmaTP VARCHAR(10),
    FOREIGN KEY (VanPhongDaiDienmaTP) REFERENCES VanPhongDaiDien(maTP) ON DELETE CASCADE
);

-- Bảng Mặt hàng
CREATE TABLE IF NOT EXISTS MatHang (
    maMH VARCHAR(10) PRIMARY KEY,
    moTa VARCHAR(255),
    loXuong VARCHAR(255),
    trongLuong FLOAT,
    Gia FLOAT,
    ngayMoBan DATE
);

-- Bảng Mặt hàng được trữ
CREATE TABLE IF NOT EXISTS MatHangDuocTru (
    soLuongTrongKho INTEGER,
    thoiGianNhap INTEGER,
    MatHangmaMH VARCHAR(10),
    CuaHangmaCH VARCHAR(10),
    PRIMARY KEY (MatHangmaMH, CuaHangmaCH),
    FOREIGN KEY (MatHangmaMH) REFERENCES MatHang(maMH) ON DELETE CASCADE,
    FOREIGN KEY (CuaHangmaCH) REFERENCES CuaHang(maCH) ON DELETE CASCADE
);

-- Bảng Khách hàng
CREATE TABLE IF NOT EXISTS KhachHang (
    maKH VARCHAR(10) PRIMARY KEY,
    tenKH VARCHAR(255),
    ngayDatDauTien DATE,
    VanPhongDaiDienmaTP VARCHAR(10),
    FOREIGN KEY (VanPhongDaiDienmaTP) REFERENCES VanPhongDaiDien(maTP) ON DELETE CASCADE
);

-- Bảng Khách hàng du lịch
CREATE TABLE IF NOT EXISTS KhachHangDuiLich (
    hoiDuLich VARCHAR(10),
    KhachHangmaKH VARCHAR(10) PRIMARY KEY,
    FOREIGN KEY (KhachHangmaKH) REFERENCES KhachHang(maKH) ON DELETE CASCADE
);

-- Bảng Khách hàng bưu điện
CREATE TABLE IF NOT EXISTS KhachHangBuiDien (
    hoiDuLich VARCHAR(10),
    KhachHangmaKH VARCHAR(10) PRIMARY KEY,
    FOREIGN KEY (KhachHangmaKH) REFERENCES KhachHang(maKH) ON DELETE CASCADE
);

-- Bảng Đơn đặt hàng
CREATE TABLE IF NOT EXISTS DonDatHang (
    maDon VARCHAR(10) PRIMARY KEY,
    ngayDatHang DATE,
    KhachHangmaKH VARCHAR(10),
    FOREIGN KEY (KhachHangmaKH) REFERENCES KhachHang(maKH) ON DELETE CASCADE
);

-- Bảng Mặt hàng được đặt
CREATE TABLE IF NOT EXISTS MatHangDuocDat (
    soLuongDat INTEGER,
    giaDat FLOAT,
    MatHangmaMH VARCHAR(10),
    DonDatHangmaDon VARCHAR(10),
    PRIMARY KEY (MatHangmaMH, DonDatHangmaDon),
    FOREIGN KEY (MatHangmaMH) REFERENCES MatHang(maMH) ON DELETE CASCADE,
    FOREIGN KEY (DonDatHangmaDon) REFERENCES DonDatHang(maDon) ON DELETE CASCADE
);

-- Tạo index để tối ưu truy vấn
CREATE INDEX idx_cuahang_vanphong ON CuaHang(VanPhongDaiDienmaTP);
CREATE INDEX idx_khachhang_vanphong ON KhachHang(VanPhongDaiDienmaTP);
CREATE INDEX idx_donhang_khachhang ON DonDatHang(KhachHangmaKH);
CREATE INDEX idx_mathang_tru_cuahang ON MatHangDuocTru(CuaHangmaCH);
CREATE INDEX idx_mathang_dat_donhang ON MatHangDuocDat(DonDatHangmaDon);

-- Insert dữ liệu mẫu
-- Văn phòng đại diện
INSERT INTO VanPhongDaiDien (maTP, tenThanhPho, diaChiVP, bang, ngayThanhLapVP) VALUES
('TP001', 'Ha Noi', '123 Nguyen Trai', 'Ha Noi', '2020-01-15'),
('TP002', 'Ho Chi Minh', '456 Le Loi', 'Ho Chi Minh', '2020-03-20'),
('TP003', 'Da Nang', '789 Tran Phu', 'Da Nang', '2020-05-10');

COMMIT;
