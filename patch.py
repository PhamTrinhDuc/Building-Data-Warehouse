import re

with open('create_database.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace generate_vanphong_daidien
content = re.sub(
    r"    for i in range\(1, num_records \+ 1\):\n        ma_tp = f'TP{i:03d}'\n        thanh_pho = random.choice\(cities\)\n        dia_chi = fake.street_address\(\)\n        mien = mien_map\[thanh_pho\]\n        ngay_thanh_lap = fake.date_between\(start_date='-5y', end_date='today'\)",
    r"    start_date_vp = datetime(2020, 1, 1).date()\n    for i in range(1, num_records + 1):\n        ma_tp = f'TP{i:03d}'\n        thanh_pho = cities[i % len(cities)]\n        dia_chi = fake.street_address()\n        mien = mien_map[thanh_pho]\n        ngay_thanh_lap = start_date_vp + timedelta(days=(i * 17) % 1000)",
    content
)

# Replace generate_cuahang
content = re.sub(
    r"    for i in range\(1, num_records \+ 1\):\n        ma_ch = f'CH{i:03d}'\n        so_dien_thoai = fake.phone_number\(\)\[:10\]\n        ngay_thanh_lap = fake.date_between\(start_date='-3y', end_date='today'\)\n        ma_vp = f'TP\{random.randint\(1, num_vanphong\):03d\}'",
    r"    start_date_ch = datetime(2021, 1, 1).date()\n    for i in range(1, num_records + 1):\n        ma_ch = f'CH{i:03d}'\n        so_dien_thoai = str(random.randint(1000000000, 9999999999))\n        ngay_thanh_lap = start_date_ch + timedelta(days=(i * 23) % 1000)\n        ma_vp = f'TP{(i % num_vanphong) + 1:03d}'",
    content
)

# Replace generate_mathang
content = re.sub(
    r"        gia = round\(random.uniform\(10000, 5000000\), 2\)\n        ngay_cap_nhat = fake.date_between\(start_date='-2y', end_date='today'\)",
    r"        gia = round(random.uniform(10000, 5000000), 2)\n        ngay_cap_nhat = datetime(2025, 1, 1).date() + timedelta(days=(i * 13) % 730)",
    content
)

# Replace generate_mathang_duoctru
old_mathang_duoctru = """    while len(records) < num_records:
        ma_mh = f'MH{random.randint(1, num_mathang):04d}'
        ma_ch = f'CH{random.randint(1, num_cuahang):03d}'
        
        # Tránh trùng lặp
        if (ma_mh, ma_ch) in generated:
            continue
        
        generated.add((ma_mh, ma_ch))
        
        so_luong = random.randint(0, 1000)
        thoi_gian_nhap = fake.date_between(start_date='-1y', end_date='today')
        records.append((so_luong, thoi_gian_nhap, ma_mh, ma_ch))

        if len(records) % 10000 == 0:
            print(f"  ⏳ Đang tạo {len(records)}/{num_records}...")"""

new_mathang_duoctru = """    # Phân bố đều trong năm 2025 và 2026
    start_date = datetime(2025, 1, 1).date()
    
    # Đảm bảo mỗi mặt hàng có mặt ở nhiều cửa hàng trải dài các vùng miền
    for idx_mh in range(1, num_mathang + 1):
        ma_mh = f'MH{idx_mh:04d}'
        # Chọn ngẫu nhiên 40% cửa hàng cho mặt hàng này (do cửa hàng phân bố đều nên mặt hàng cũng phân bố đều theo miền)
        selected_stores = random.sample(range(1, num_cuahang + 1), int(num_cuahang * 0.4))
        for idx_ch in selected_stores:
            ma_ch = f'CH{idx_ch:03d}'
            so_luong = random.randint(10, 1000)
            # Trải đều ngày trong 730 ngày (2 năm 2025-2026)
            thoi_gian_nhap = start_date + timedelta(days=random.randint(0, 729))
            records.append((so_luong, thoi_gian_nhap, ma_mh, ma_ch))
            
            if len(records) % 10000 == 0:
                print(f"  ⏳ Đang tạo {len(records)}/{num_records}...")"""
content = content.replace(old_mathang_duoctru, new_mathang_duoctru)

# Replace generate_khachhang
content = re.sub(
    r"        ngay_dat_dau_tien = fake.date_between\(start_date='-2y', end_date='today'\)\n        ma_vp = f'TP\{random.randint\(1, num_vanphong\):03d\}'",
    r"        ngay_dat_dau_tien = datetime(2025, 1, 1).date() + timedelta(days=i % 730)\n        ma_vp = f'TP{(i % num_vanphong) + 1:03d}'",
    content
)

content = re.sub(
    r"ngay_dang_ky = fake.date_between\(start_date='-1y', end_date='today'\)",
    r"ngay_dang_ky = datetime(2025, 1, 1).date() + timedelta(days=random.randint(0, 729))",
    content
)

# Replace generate_donhang
old_generate_donhang = """        ngay_dat_hang = fake.date_between(start_date='-1y', end_date='today')
        ma_kh = f'KH{random.randint(1, num_khachhang):05d}'"""
new_generate_donhang = """        # Đơn hàng trải đều trong 2 năm 2025 và 2026
        ngay_dat_hang = datetime(2025, 1, 1).date() + timedelta(days=i % 730)
        # Khách hàng phân bổ đều cho các đơn hàng
        ma_kh = f'KH{(i % num_khachhang) + 1:05d}'"""
content = content.replace(old_generate_donhang, new_generate_donhang)


with open('create_database.py', 'w', encoding='utf-8') as f:
    f.write(content)

