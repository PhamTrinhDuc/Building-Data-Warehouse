-- ================================================
-- ClickHouse Data Warehouse Schema
-- Star Schema theo diagram
-- Dims : Dim_ThoiGian, Dim_DiaDiem, Dim_MatHang, Dim_KhachHang, Dim_CuaHang
-- Facts: Fact_DonDatHang, Fact_TonKho
-- ================================================

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Dimension: Thời gian
CREATE TABLE IF NOT EXISTS Dim_ThoiGian (
    sk_thoiGian  Int32,
    ngay         Date,
    thang        Int8,
    quy          Int8,
    nam          Int16
) ENGINE = MergeTree()
ORDER BY sk_thoiGian;

-- Dimension: Mặt hàng
CREATE TABLE IF NOT EXISTS Dim_MatHang (
    sk_matHang   Int32,
    tenMH        String,
    maMH         String,
    moTa         String,
    kichCo       String,
    trongLuong   Float64,
    gia          Float64,
    ngayMoBan    Nullable(Date)
) ENGINE = MergeTree()
ORDER BY sk_matHang;

-- Dimension: Địa điểm (Từ VanPhongDaiDien)
CREATE TABLE IF NOT EXISTS Dim_DiaDiem (
    sk_diaDiem      Int32,
    maTP            String,
    mien            String,
    diaChiVP        String,
    ngayThanhLapVP  Nullable(Date),
    tenThanhPho     String
) ENGINE = MergeTree()
ORDER BY sk_diaDiem;

-- Dimension: Cửa hàng
CREATE TABLE IF NOT EXISTS Dim_CuaHang (
    sk_cuaHang      Int32,
    maCH            String,
    soDienThoai     String,
    ngayThanhLapCH  Nullable(Date),
    sk_diaDiem      Int32
) ENGINE = MergeTree()
ORDER BY sk_cuaHang;

-- Dimension: Khách hàng
-- loaiKhachHang: 'Du lich' | 'Buu dien' | 'Ca hai' | 'Khong phan loai'
CREATE TABLE IF NOT EXISTS Dim_KhachHang (
    sk_khachHang        Int32,
    sk_diaDiem          Int32,
    maKH                String,
    tenKH               String,
    ngayDatHangDauTien  Nullable(Date),
    huongDanVien        Nullable(String),
    diaChiBuuDien       Nullable(String),
    loaiKhachHang       String
) ENGINE = MergeTree()
ORDER BY sk_khachHang;

-- ============================================
-- FACT TABLES
-- ============================================

-- Fact: Đơn đặt hàng
CREATE TABLE IF NOT EXISTS Fact_DonDatHang (
    sk_khachHang  Int32,
    sk_matHang    Int32,
    sk_thoiGian   Int32,
    maDon         String,
    soLuongDat    Int32,
    giaDat        Float64,
    thanhTien     Float64
) ENGINE = MergeTree()
ORDER BY (sk_thoiGian, sk_khachHang, maDon);

-- Fact: Tồn kho
CREATE TABLE IF NOT EXISTS Fact_TonKho (
    sk_thoiGian   Int32,
    sk_matHang    Int32,
    sk_cuaHang    Int32,
    soLuongTonKho Int32
) ENGINE = MergeTree()
ORDER BY (sk_thoiGian, sk_cuaHang, sk_matHang);

-- Thông báo
SELECT 'Data Warehouse schema created successfully!' as status;
