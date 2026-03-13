"""
Transform module - Xử lý và chuyển đổi dữ liệu sang Star Schema
Mapping từ OLTP schema idb sang Data Warehouse:
  Dims : Dim_ThoiGian, Dim_DiaDiem, Dim_MatHang, Dim_CuaHang, Dim_KhachHang
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
    dates_mh  = pd.to_datetime(source_data['mathang']['ngaymoban'],       errors='coerce').dropna()
    all_dates = pd.concat([dates_ddh, dates_mh])

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
    # DIMENSION: Địa điểm  (từ VanPhongDaiDien)
    # ============================================
    print("[2/7] Transforming Dim_DiaDiem...")

    vp = source_data['vanphongdaidien'].copy()
    vp.columns = [c.lower() for c in vp.columns]

    vp['sk_diaDiem'] = range(1, len(vp) + 1)

    dw_data['Dim_DiaDiem'] = vp.rename(columns={
        'matp'          : 'maTP',
        'tenthanhpho'   : 'tenTP',
        'diachivp'      : 'diaChiVP',
        'bang'          : 'bang',
        'ngaythanhlap'  : 'ngayThanhLapVP',
        'ngaythanhlapvp': 'ngayThanhLapVP',
    })[['sk_diaDiem', 'maTP', 'tenTP', 'diaChiVP', 'bang', 'ngayThanhLapVP']]

    # ============================================
    # DIMENSION: Mặt hàng
    # ============================================
    print("[3/7] Transforming Dim_MatHang...")

    mh = source_data['mathang'].copy()
    mh.columns = [c.lower() for c in mh.columns]
    mh['sk_matHang'] = range(1, len(mh) + 1)

    dw_data['Dim_MatHang'] = mh.rename(columns={
        'mamh'     : 'maMH',
        'mota'     : 'moTa',
        'loxuong'  : 'kichCo',   # loXuong → kichCo theo diagram
        'trongluong': 'trongLuong',
        'gia'      : 'gia',
    })[['sk_matHang', 'maMH', 'moTa', 'kichCo', 'trongLuong', 'gia']]

    # ============================================
    # DIMENSION: Cửa hàng  (join VanPhongDaiDien)
    # ============================================
    print("[4/7] Transforming Dim_CuaHang...")

    ch = source_data['cuahang'].copy()
    ch.columns = [c.lower() for c in ch.columns]
    ch['sk_cuaHang'] = range(1, len(ch) + 1)

    # tenThanhPho đã được join sẵn trong extract
    col_map = {
        'mach'           : 'maCH',
        'sodienthoai'    : 'soDienThoai',
        'ngaythanhlap'   : 'ngayThanhLapCH',
        'ngaythanhlapch' : 'ngayThanhLapCH',
        'tenthanhpho'    : 'tenThanhPho',
        'vanphongdaidienmatch': 'maThanhPho',
    }
    ch = ch.rename(columns=col_map)

    keep = ['sk_cuaHang', 'maCH', 'soDienThoai', 'ngayThanhLapCH', 'tenThanhPho']
    dw_data['Dim_CuaHang'] = ch[[c for c in keep if c in ch.columns]]

    # ============================================
    # DIMENSION: Khách hàng
    # Left Join KhachHangDuiLich  → huongDanVien
    # Left Join KhachHangBuiDien  → diaChiBuuDien
    # Derive loaiKhachHang
    # ============================================
    print("[5/7] Transforming Dim_KhachHang...")

    kh     = source_data['khachhang'].copy()
    khdl   = source_data['khachhang_dulich'].copy()
    khbd   = source_data['khachhang_buudien'].copy()

    kh.columns   = [c.lower() for c in kh.columns]
    khdl.columns = [c.lower() for c in khdl.columns]
    khbd.columns = [c.lower() for c in khbd.columns]

    # Left join để lấy thông tin từ bảng sub-type
    kh = kh.merge(
        khdl[['khachhangmakh', 'hoidulich']].rename(
            columns={'khachhangmakh': 'makh', 'hoidulich': 'huongDanVien'}
        ), on='makh', how='left'
    )
    kh = kh.merge(
        khbd[['khachhangmakh', 'hoidulich']].rename(
            columns={'khachhangmakh': 'makh', 'hoidulich': 'diaChiBuuDien'}
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

    # Join sk_diaDiem từ Dim_DiaDiem
    kh = kh.merge(
        dw_data['Dim_DiaDiem'][['sk_diaDiem', 'maTP']].rename(columns={'maTP': 'vanphongdaidienmatch'}),
        left_on='vanphongdaidienmantp' if 'vanphongdaidienmantp' in kh.columns else
                [c for c in kh.columns if 'vanphong' in c][0],
        right_on='vanphongdaidienmatch',
        how='left'
    )

    kh['sk_khachHang'] = range(1, len(kh) + 1)

    dw_data['Dim_KhachHang'] = kh.rename(columns={
        'makh'           : 'maKH',
        'tenkh'          : 'tenKH',
        'ngaydatdautien' : 'ngayDatHangDauTien',
    })[[
        'sk_khachHang', 'maKH', 'tenKH', 'ngayDatHangDauTien',
        'huongDanVien', 'diaChiBuuDien', 'loaiKhachHang', 'sk_diaDiem'
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
    # thoiGianNhap (INTEGER, số ngày trước hiện tại) → convert sang date key
    # ============================================
    print("[7/7] Transforming Fact_TonKho...")

    tk = source_data['mathang_duoctru'].copy()
    tk.columns = [c.lower() for c in tk.columns]

    tk = tk.rename(columns={
        'mathangmamh'    : 'maMH',
        'cuahangmach'    : 'maCH',
        'soluongtrongkho': 'soLuongTonKho',
        'thoigiannh'     : 'thoiGianNhap',
        'thoigiannh_ap'  : 'thoiGianNhap',
        'thoigiannh'     : 'thoiGianNhap',
    })
    # Tìm đúng tên cột thoiGianNhap
    tgn_col = next((c for c in tk.columns if 'thoigian' in c.lower()), None)
    if tgn_col:
        tk = tk.rename(columns={tgn_col: 'thoiGianNhap'})

    ref_date = datetime.today().date()
    tk['ngayNhap']    = tk['thoiGianNhap'].apply(
        lambda d: ref_date - timedelta(days=int(d)) if pd.notna(d) else ref_date
    )
    tk['sk_thoiGian'] = tk['ngayNhap'].apply(lambda d: int(d.strftime('%Y%m%d')))

    tk = tk.merge(dw_data['Dim_MatHang'][['sk_matHang', 'maMH']], on='maMH', how='left')
    tk = tk.merge(dw_data['Dim_CuaHang'][['sk_cuaHang', 'maCH']], on='maCH', how='left')

    dw_data['Fact_TonKho'] = tk[[
        'sk_cuaHang', 'sk_matHang', 'sk_thoiGian', 'soLuongTonKho'
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
