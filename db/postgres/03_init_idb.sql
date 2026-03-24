-- ================================================
-- Database: idb (Integrated Database)
-- Schema tích hợp - Quản lý tổng hợp
-- ================================================

-- Tạo schema
CREATE SCHEMA IF NOT EXISTS idb;

-- Sử dụng schema
SET search_path TO idb;

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
    tenMH VARCHAR(255),
    moTa VARCHAR(255),
    kichCo VARCHAR(255),
    trongLuong FLOAT,
    Gia FLOAT,
    ngayCapNhat DATE
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
    ngayDangKy DATE,
    FOREIGN KEY (KhachHangmaKH) REFERENCES KhachHang(maKH) ON DELETE CASCADE
);

-- Bảng Khách hàng bưu điện
CREATE TABLE IF NOT EXISTS KhachHangBuiDien (
    hoiDuLich VARCHAR(10),
    KhachHangmaKH VARCHAR(10) PRIMARY KEY,
    ngayDangKy DATE,
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

COMMIT;
