"""
Transform module - Xử lý và chuyển đổi dữ liệu sang Star Schema
"""
import pandas as pd
from datetime import datetime


def transform_data(source_data):
    """Transform dữ liệu từ OLTP sang Data Warehouse schema"""
    
    print("\n=== TRANSFORMING DATA ===")
    
    dw_data = {}
    
    # ============================================
    # DIMENSION: Khách hàng (SCD Type 2)
    # ============================================
    print("\n[1/5] Transforming dim_khach_hang...")
    
    kh = source_data['khachhang'].copy()
    kh_dulich = source_data['khachhang_dulich'].copy()
    kh_buudien = source_data['khachhang_buudien'].copy()
    
    # Xác định loại khách hàng
    kh['loai_khach_hang'] = 'Khong phan loai'
    kh.loc[kh['ma_khachhang'].isin(kh_dulich['ma_khachhang']) & 
           kh['ma_khachhang'].isin(kh_buudien['ma_khachhang']), 'loai_khach_hang'] = 'Ca hai'
    kh.loc[kh['ma_khachhang'].isin(kh_dulich['ma_khachhang']) & 
           (kh['loai_khach_hang'] != 'Ca hai'), 'loai_khach_hang'] = 'Du lich'
    kh.loc[kh['ma_khachhang'].isin(kh_buudien['ma_khachhang']) & 
           (kh['loai_khach_hang'] != 'Ca hai'), 'loai_khach_hang'] = 'Buu dien'
    
    # Merge với bảng chi tiết
    kh = kh.merge(kh_dulich[['ma_khachhang', 'passport', 'quoc_tich']], 
                  on='ma_khachhang', how='left')
    kh = kh.merge(kh_buudien[['ma_khachhang', 'ma_hop_buudien', 'loai_hop']], 
                  on='ma_khachhang', how='left')
    
    # Tạo surrogate key
    kh['khach_hang_key'] = range(1, len(kh) + 1)
    kh['ngay_hieu_luc'] = datetime.now().date()
    kh['ngay_het_han'] = None
    kh['la_hien_tai'] = 1
    
    dw_data['dim_khach_hang'] = kh[[
        'khach_hang_key', 'ma_khachhang', 'ten_khachhang', 'dia_chi',
        'thanh_pho', 'bang', 'ma_buudien', 'so_dien_thoai', 'email',
        'loai_khach_hang', 'passport', 'quoc_tich', 'ma_hop_buudien',
        'loai_hop', 'ngay_hieu_luc', 'ngay_het_han', 'la_hien_tai'
    ]]
    
    # ============================================
    # DIMENSION: Mặt hàng
    # ============================================
    print("[2/5] Transforming dim_mat_hang...")
    
    mh = source_data['mathang'].copy()
    mh['mat_hang_key'] = range(1, len(mh) + 1)
    
    dw_data['dim_mat_hang'] = mh[[
        'mat_hang_key', 'ma_mathang', 'ten_mathang', 'danh_muc',
        'don_gia', 'don_vi_tinh', 'mo_ta'
    ]]
    
    # ============================================
    # DIMENSION: Cửa hàng
    # ============================================
    print("[3/5] Transforming dim_cua_hang...")
    
    ch = source_data['cuahang'].copy()
    ch['cua_hang_key'] = range(1, len(ch) + 1)
    ch = ch.rename(columns={
        'dia_chi': 'dia_chi_cuahang',
        'thanh_pho': 'thanh_pho_cuahang',
        'bang': 'bang_cuahang',
        'so_dien_thoai': 'so_dien_thoai_cuahang'
    })
    
    dw_data['dim_cua_hang'] = ch[[
        'cua_hang_key', 'ma_cuahang', 'ten_cuahang', 'dia_chi_cuahang',
        'thanh_pho_cuahang', 'bang_cuahang', 'so_dien_thoai_cuahang',
        'dien_tich_m2', 'ma_vanphong', 'ten_vanphong', 'dia_chi_vanphong',
        'thanh_pho_vanphong', 'bang_vanphong', 'so_dien_thoai_vanphong'
    ]]
    
    # ============================================
    # FACT: Đơn đặt hàng
    # ============================================
    print("[4/5] Transforming fact_don_dat_hang...")
    
    ddh = source_data['dondathang'].copy()
    
    # Join với dimensions để lấy surrogate keys
    ddh = ddh.merge(
        dw_data['dim_khach_hang'][['khach_hang_key', 'ma_khachhang']], 
        on='ma_khachhang', how='left'
    )
    ddh = ddh.merge(
        dw_data['dim_mat_hang'][['mat_hang_key', 'ma_mathang']], 
        on='ma_mathang', how='left'
    )
    ddh = ddh.merge(
        dw_data['dim_cua_hang'][['cua_hang_key', 'ma_cuahang']], 
        on='ma_cuahang', how='left'
    )
    
    # Tạo time keys
    ddh['thoi_gian_dat_hang_key'] = pd.to_datetime(ddh['ngay_dat_hang']).dt.strftime('%Y%m%d').astype(int)
    ddh['thoi_gian_giao_hang_key'] = pd.to_datetime(ddh['ngay_giao_hang'], errors='coerce').dt.strftime('%Y%m%d')
    ddh['thoi_gian_giao_hang_key'] = pd.to_numeric(ddh['thoi_gian_giao_hang_key'], errors='coerce')
    
    ddh['don_hang_key'] = range(1, len(ddh) + 1)
    
    dw_data['fact_don_dat_hang'] = ddh[[
        'don_hang_key', 'ma_donhang', 'khach_hang_key', 'mat_hang_key',
        'cua_hang_key', 'thoi_gian_dat_hang_key', 'thoi_gian_giao_hang_key',
        'so_luong', 'don_gia', 'thanh_tien', 'trang_thai'
    ]]
    
    # ============================================
    # FACT: Tồn kho
    # ============================================
    print("[5/5] Transforming fact_ton_kho...")
    
    tk = source_data['mathang_luutru'].copy()
    
    tk = tk.merge(
        dw_data['dim_mat_hang'][['mat_hang_key', 'ma_mathang']], 
        on='ma_mathang', how='left'
    )
    tk = tk.merge(
        dw_data['dim_cua_hang'][['cua_hang_key', 'ma_cuahang']], 
        on='ma_cuahang', how='left'
    )
    
    tk['thoi_gian_key'] = pd.to_datetime(tk['ngay_nhap_kho']).dt.strftime('%Y%m%d').astype(int)
    tk['ton_kho_key'] = range(1, len(tk) + 1)
    
    dw_data['fact_ton_kho'] = tk[[
        'ton_kho_key', 'mat_hang_key', 'cua_hang_key', 'thoi_gian_key',
        'so_luong_ton', 'ngay_nhap_kho'
    ]]
    
    # Print summary
    print("\n=== TRANSFORMATION SUMMARY ===")
    for table_name, df in dw_data.items():
        print(f"  {table_name:25s}: {len(df):6d} rows")
    
    return dw_data


if __name__ == "__main__":
    from extract import extract_from_postgres
    source_data = extract_from_postgres()
    dw_data = transform_data(source_data)
    print("\n✓ Transformation completed!")
