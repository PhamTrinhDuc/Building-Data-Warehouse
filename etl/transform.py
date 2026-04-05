"""
Transform module - Xử lý và chuyển đổi dữ liệu sang Star Schema
Mapping từ OLTP schema idb sang Data Warehouse:
  Dims : Dim_ThoiGian, Dim_DiaDiem, Dim_MatHang, Dim_KhachHang, Dim_CuaHang
  Facts: Fact_DonDatHang, Fact_TonKho
"""
import pandas as pd
from datetime import datetime, timedelta


def _make_time_key(date_series):
    """Chuyển date column sang integer key dạng YYYYMMDD"""
    return pd.to_datetime(date_series).dt.strftime('%Y%m%d').astype(int)


def transform_data(source_data):
    """Transform dữ liệu từ OLTP sang Data Warehouse schema"""

    print("\n=== TRANSFORMING DATA ===")

    dw_data = {}

    # ============================================
    # DIMENSION: Thời gian
    # Sinh toàn bộ ngày trong khoảng dữ liệu thực tế
    # ============================================
    print("\n[1/7] Transforming Dim_ThoiGian...")

    dates_ddh = pd.to_datetime(source_data['dondathang']['ngaydathang'], errors='coerce').dropna()
    dates_mh  = pd.to_datetime(source_data['mathang']['ngaycapnhat'],       errors='coerce').dropna()
    dates_ch  = pd.to_datetime(source_data['cuahang']['ngaythanhlapch'], errors='coerce').dropna()
    dates_tk  = pd.to_datetime(source_data['mathang_duoctru']['thoigiannhap'], errors='coerce').dropna()
    all_dates = pd.concat([dates_ddh, dates_mh, dates_ch, dates_tk])

    min_date = all_dates.min().date()
    max_date = all_dates.max().date()
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')

    dim_thoigian = pd.DataFrame({'ngay': date_range})
    dim_thoigian['sk_thoiGian'] = dim_thoigian['ngay'].dt.strftime('%Y%m%d').astype(int)
    dim_thoigian['thang']       = dim_thoigian['ngay'].dt.month
    dim_thoigian['quy']         = dim_thoigian['ngay'].dt.quarter
    dim_thoigian['nam']         = dim_thoigian['ngay'].dt.year
    dim_thoigian['ngay']        = dim_thoigian['ngay'].dt.date

    dw_data['Dim_ThoiGian'] = dim_thoigian[['sk_thoiGian', 'ngay', 'thang', 'quy', 'nam']]

    # ============================================
    # DIMENSION: Mặt hàng
    # ============================================
    print("[2/7] Transforming Dim_MatHang...")

    mh = source_data['mathang'].copy()
    mh.columns = [c.lower() for c in mh.columns]
    mh['sk_matHang'] = range(1, len(mh) + 1)

    dw_data['Dim_MatHang'] = mh.rename(columns={
        'mamh'     : 'maMH',
        'tenmh'    : 'tenMH',
        'mota'     : 'moTa',
        'kichco'   : 'kichCo',
        'trongluong': 'trongLuong',
        'gia'      : 'gia',
        'ngaycapnhat': 'ngayMoBan',
    })[['sk_matHang', 'maMH', 'tenMH', 'moTa', 'kichCo', 'trongLuong', 'gia', 'ngayMoBan']]

    # ============================================
    # DIMENSION: Địa điểm (Từ VanPhongDaiDien)
    # ============================================
    print("[3/7] Transforming Dim_DiaDiem...")

    dd = source_data['vanphongdaidien'].copy()
    dd.columns = [c.lower() for c in dd.columns]
    dd['sk_diaDiem'] = range(1, len(dd) + 1)

    col_map = {
        'matp'           : 'maTP',
        'tenthanhpho'    : 'tenThanhPho',
        'diachivp'       : 'diaChiVP',
        'mien'           : 'mien',
        'ngaythanhlapvp' : 'ngayThanhLapVP',
    }
    dd = dd.rename(columns=col_map)

    dw_data['Dim_DiaDiem'] = dd[['sk_diaDiem', 'maTP', 'mien', 'diaChiVP', 'ngayThanhLapVP', 'tenThanhPho']]

    # ============================================
    # DIMENSION: Khách hàng
    # Left Join KhachHangDuiLich  → huongDanVien, ngayDangKyDuLich
    # Left Join KhachHangBuiDien  → diaChiBuuDien, ngayDangKyBuuDien
    # Link to Dim_DiaDiem
    # Derive loaiKhachHang
    # ============================================
    print("[4/7] Transforming Dim_KhachHang...")

    kh     = source_data['khachhang'].copy()
    khdl   = source_data['khachhang_dulich'].copy()
    khbd   = source_data['khachhang_buudien'].copy()

    kh.columns   = [c.lower() for c in kh.columns]
    khdl.columns = [c.lower() for c in khdl.columns]
    khbd.columns = [c.lower() for c in khbd.columns]

    # Left join để lấy thông tin từ bảng sub-type
    kh = kh.merge(
        khdl[['khachhangmakh', 'hdvdulich', 'ngaydangky']].rename(
            columns={'khachhangmakh': 'makh', 'hdvdulich': 'huongDanVien', 'ngaydangky': 'ngayDangKyDuLich'}
        ), on='makh', how='left'
    )
    kh = kh.merge(
        khbd[['khachhangmakh', 'diachbuudien', 'ngaydangky']].rename(
            columns={'khachhangmakh': 'makh', 'diachbuudien': 'diaChiBuuDien', 'ngaydangky': 'ngayDangKyBuuDien'}
        ), on='makh', how='left'
    )

    # Derive loaiKhachHang từ dữ liệu join
    conditions = [
        kh['huongDanVien'].notna() & kh['diaChiBuuDien'].notna(),
        kh['huongDanVien'].notna(),
        kh['diaChiBuuDien'].notna(),
    ]
    choices = ['Ca hai', 'Du lich', 'Buu dien']
    kh['loaiKhachHang'] = pd.Categorical(
        pd.Series(
            [choices[next((i for i, c in enumerate(conditions) if c.iloc[j]), 3)]
             if any(c.iloc[j] for c in conditions) else 'Khong phan loai'
             for j in range(len(kh))]
        )
    )

    kh['sk_khachHang'] = range(1, len(kh) + 1)
    
    # Map sk_diaDiem
    kh = kh.rename(columns={'vanphongdaidienmatp': 'maTP'})
    kh = kh.merge(dw_data['Dim_DiaDiem'][['sk_diaDiem', 'maTP']], on='maTP', how='left')

    dw_data['Dim_KhachHang'] = kh.rename(columns={
        'makh'           : 'maKH',
        'tenkh'          : 'tenKH',
        'ngaydatdautien' : 'ngayDatHangDauTien',
    })[[
        'sk_khachHang', 'sk_diaDiem', 'maKH', 'tenKH', 'ngayDatHangDauTien',
        'huongDanVien', 'diaChiBuuDien', 'loaiKhachHang'
    ]]

    # ============================================
    # DIMENSION: Cửa hàng
    # Source: CuaHang
    # Link to Dim_DiaDiem
    # ============================================
    print("[5/7] Transforming Dim_CuaHang...")

    ch = source_data['cuahang'].copy()
    ch.columns = [c.lower() for c in ch.columns]
    
    ch['sk_cuaHang'] = range(1, len(ch) + 1)

    # Map sk_diaDiem
    ch = ch.rename(columns={'vanphongdaidienmatp': 'maTP'})
    ch = ch.merge(dw_data['Dim_DiaDiem'][['sk_diaDiem', 'maTP']], on='maTP', how='left')

    dw_data['Dim_CuaHang'] = ch.rename(columns={
        'mach'           : 'maCH',
        'sodienthoai'    : 'soDienThoai',
        'ngaythanhlapch' : 'ngayThanhLapCH',
    })[[
        'sk_cuaHang', 'sk_diaDiem', 'maCH', 'soDienThoai', 'ngayThanhLapCH'
    ]]

    # ============================================
    # FACT: Đơn đặt hàng
    # Source: MatHangDuocDat JOIN DonDatHang
    # ============================================
    print("[6/7] Transforming Fact_DonDatHang...")

    mhdd = source_data['mathang_duocdat'].copy()
    mhdd.columns = [c.lower() for c in mhdd.columns]

    fact_ddh = mhdd.rename(columns={
        'mathangmamh'      : 'maMH',
        'dondathangmadon'  : 'maDon',
        'soluongdat'       : 'soLuongDat',
        'giadat'           : 'giaDat',
        'ngaydathang'      : 'ngayDatHang',
        'khachhangmakh'    : 'maKH',
    })

    # thanhTien = soLuongDat * giaDat
    fact_ddh['thanhTien'] = fact_ddh['soLuongDat'] * fact_ddh['giaDat']

    # sk_thoiGian
    fact_ddh['sk_thoiGian'] = _make_time_key(fact_ddh['ngayDatHang'])

    # Surrogate keys từ dims
    fact_ddh = fact_ddh.merge(
        dw_data['Dim_KhachHang'][['sk_khachHang', 'maKH']], on='maKH', how='left'
    )
    fact_ddh = fact_ddh.merge(
        dw_data['Dim_MatHang'][['sk_matHang', 'maMH']], on='maMH', how='left'
    )

    dw_data['Fact_DonDatHang'] = fact_ddh[[
        'sk_khachHang', 'sk_matHang', 'sk_thoiGian',
        'maDon', 'soLuongDat', 'giaDat', 'thanhTien'
    ]]

    # ============================================
    # FACT: Tồn kho
    # Source: MatHangDuocTru
    # ============================================
    print("[7/7] Transforming Fact_TonKho...")
    
    mht = source_data['mathang_duoctru'].copy()
    mht.columns = [c.lower() for c in mht.columns]

    fact_tk = mht.rename(columns={
        'mathangmamh'       : 'maMH',
        'cuahangmach'       : 'maCH',
        'soluongtrongkho'   : 'soLuongTonKho',
        'thoigiannhap'      : 'thoiGianNhap'
    })

    # sk_thoiGian
    fact_tk['sk_thoiGian'] = _make_time_key(fact_tk['thoiGianNhap'])

    # Lấy tồn kho cuối ngày: sort theo thời gian và giữ lại bản ghi cuối cùng của mỗi mặt hàng tại 1 cửa hàng trong 1 ngày
    fact_tk = fact_tk.sort_values('thoiGianNhap')
    fact_tk = fact_tk.drop_duplicates(subset=['maMH', 'maCH', 'sk_thoiGian'], keep='last')

    # Surrogate keys từ dims
    fact_tk = fact_tk.merge(
        dw_data['Dim_MatHang'][['sk_matHang', 'maMH']], on='maMH', how='left'
    )
    fact_tk = fact_tk.merge(
        dw_data['Dim_CuaHang'][['sk_cuaHang', 'maCH']], on='maCH', how='left'
    )

    dw_data['Fact_TonKho'] = fact_tk[[
        'sk_matHang', 'sk_cuaHang', 'sk_thoiGian', 'soLuongTonKho'
    ]]

    # Print summary
    print("\n=== TRANSFORMATION SUMMARY ===")
    for table_name, df in dw_data.items():
        print(f"  {table_name:20s}: {len(df):6d} rows")

    return dw_data


if __name__ == "__main__":
    from extract import extract_from_postgres
    source_data = extract_from_postgres()
    dw_data = transform_data(source_data)
    print("\n✓ Transformation completed!")
