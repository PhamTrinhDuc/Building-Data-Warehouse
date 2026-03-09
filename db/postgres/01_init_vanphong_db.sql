-- ================================================
-- Database: vanphong_db
-- Schema cho Văn phòng đại diện - Quản lý khách hàng
-- ================================================

-- Tạo schema
CREATE SCHEMA IF NOT EXISTS vanphong_db;

-- Sử dụng schema
SET search_path TO vanphong_db;

-- Bảng khách hàng chính
CREATE TABLE IF NOT EXISTS khachhang (
    ma_khachhang VARCHAR(10) PRIMARY KEY,
    ten_khachhang VARCHAR(100) NOT NULL,
    dia_chi VARCHAR(255),
    thanh_pho VARCHAR(50),
    bang VARCHAR(50),
    ma_buudien VARCHAR(10),
    so_dien_thoai VARCHAR(20),
    email VARCHAR(100),
    ngay_dang_ky DATE DEFAULT CURRENT_DATE
);

-- Bảng khách hàng du lịch
CREATE TABLE IF NOT EXISTS khachhang_dulich (
    ma_khachhang VARCHAR(10) PRIMARY KEY,
    passport VARCHAR(20) NOT NULL,
    quoc_tich VARCHAR(50),
    ngay_nhap_canh DATE,
    FOREIGN KEY (ma_khachhang) REFERENCES khachhang(ma_khachhang) ON DELETE CASCADE
);

-- Bảng khách hàng bưu điện
CREATE TABLE IF NOT EXISTS khachhang_buudien (
    ma_khachhang VARCHAR(10) PRIMARY KEY,
    ma_hop_buudien VARCHAR(20) NOT NULL,
    loai_hop VARCHAR(20),
    ngay_thue_hop DATE,
    FOREIGN KEY (ma_khachhang) REFERENCES khachhang(ma_khachhang) ON DELETE CASCADE
);

-- Tạo index
CREATE INDEX idx_kh_thanh_pho ON khachhang(thanh_pho);
CREATE INDEX idx_kh_bang ON khachhang(bang);
CREATE INDEX idx_kh_ngay_dang_ky ON khachhang(ngay_dang_ky);