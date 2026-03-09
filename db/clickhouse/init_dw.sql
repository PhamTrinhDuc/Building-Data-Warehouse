-- ================================================
-- ClickHouse Data Warehouse Schema
-- Star Schema: Dimension & Fact Tables
-- ================================================

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Dimension: Thời gian
CREATE TABLE IF NOT EXISTS dim_thoi_gian (
    thoi_gian_key Int32,
    ngay_day_du Date,
    ngay Int8,
    thang Int8,
    quy Int8,
    nam Int16,
    ten_thang String,
    thu_trong_tuan Int8,
    ten_thu String,
    tuan_trong_nam Int8,
    ngay_trong_nam Int16,
    la_cuoi_tuan UInt8
) ENGINE = MergeTree()
ORDER BY thoi_gian_key;

-- Dimension: Khách hàng (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_khach_hang (
    khach_hang_key Int32,
    ma_khachhang String,
    ten_khachhang String,
    dia_chi String,
    thanh_pho String,
    bang String,
    ma_buudien String,
    so_dien_thoai String,
    email String,
    loai_khach_hang String,  -- 'Du lich', 'Buu dien', 'Ca hai', 'Khong phan loai'
    passport Nullable(String),
    quoc_tich Nullable(String),
    ma_hop_buudien Nullable(String),
    loai_hop Nullable(String),
    ngay_hieu_luc Date,
    ngay_het_han Nullable(Date),
    la_hien_tai UInt8 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY khach_hang_key;

-- Dimension: Mặt hàng
CREATE TABLE IF NOT EXISTS dim_mat_hang (
    mat_hang_key Int32,
    ma_mathang String,
    ten_mathang String,
    danh_muc String,
    don_gia Decimal(12,2),
    don_vi_tinh String,
    mo_ta String
) ENGINE = MergeTree()
ORDER BY mat_hang_key;

-- Dimension: Cửa hàng
CREATE TABLE IF NOT EXISTS dim_cua_hang (
    cua_hang_key Int32,
    ma_cuahang String,
    ten_cuahang String,
    dia_chi_cuahang String,
    thanh_pho_cuahang String,
    bang_cuahang String,
    so_dien_thoai_cuahang String,
    dien_tich_m2 Nullable(Decimal(10,2)),
    ma_vanphong String,
    ten_vanphong String,
    dia_chi_vanphong String,
    thanh_pho_vanphong String,
    bang_vanphong String,
    so_dien_thoai_vanphong String
) ENGINE = MergeTree()
ORDER BY cua_hang_key;

-- Dimension: Địa điểm (hỗ trợ hierarchy)
CREATE TABLE IF NOT EXISTS dim_dia_diem (
    dia_diem_key Int32,
    thanh_pho String,
    bang String,
    vung_mien String,  -- 'Mien Bac', 'Mien Trung', 'Mien Nam'
    quoc_gia String DEFAULT 'Vietnam'
) ENGINE = MergeTree()
ORDER BY dia_diem_key;

-- ============================================
-- FACT TABLES
-- ============================================

-- Fact: Đơn đặt hàng
CREATE TABLE IF NOT EXISTS fact_don_dat_hang (
    don_hang_key Int64,
    ma_donhang String,
    khach_hang_key Int32,
    mat_hang_key Int32,
    cua_hang_key Int32,
    thoi_gian_dat_hang_key Int32,
    thoi_gian_giao_hang_key Nullable(Int32),
    so_luong Int32,
    don_gia Decimal(12,2),
    thanh_tien Decimal(15,2),
    trang_thai String
) ENGINE = MergeTree()
ORDER BY (thoi_gian_dat_hang_key, khach_hang_key, don_hang_key);

-- Fact: Tồn kho
CREATE TABLE IF NOT EXISTS fact_ton_kho (
    ton_kho_key Int64,
    mat_hang_key Int32,
    cua_hang_key Int32,
    thoi_gian_key Int32,
    so_luong_ton Int32,
    ngay_nhap_kho Date
) ENGINE = MergeTree()
ORDER BY (thoi_gian_key, cua_hang_key, mat_hang_key);

-- ============================================
-- INSERT INITIAL DATA
-- ============================================

-- Populate dim_thoi_gian (2024-2026)
INSERT INTO dim_thoi_gian
SELECT
    toInt32(toYYYYMMDD(date)) as thoi_gian_key,
    date as ngay_day_du,
    toDayOfMonth(date) as ngay,
    toMonth(date) as thang,
    toQuarter(date) as quy,
    toYear(date) as nam,
    dateName('month', date) as ten_thang,
    toDayOfWeek(date) as thu_trong_tuan,
    dateName('weekday', date) as ten_thu,
    toWeek(date) as tuan_trong_nam,
    toDayOfYear(date) as ngay_trong_nam,
    if(toDayOfWeek(date) IN (6, 7), 1, 0) as la_cuoi_tuan
FROM (
    SELECT addDays(toDate('2024-01-01'), number) as date
    FROM numbers(1095)  -- 3 years
);

-- Thông báo
SELECT 'Data Warehouse schema created successfully!' as status;
SELECT 'Total dim_thoi_gian records: ' || toString(count(*)) as info FROM dim_thoi_gian;
