"""
Load module - Ghi dữ liệu vào ClickHouse Data Warehouse
Thứ tự load: Dims trước → Facts sau (tránh FK violation)
"""
import clickhouse_connect
import pandas as pd
import numpy as np
import time

# Thứ tự load: dims trước, facts sau
# Mỗi entry: (tên key trong dw_data, tên bảng ClickHouse)
LOAD_ORDER = [
    ('Dim_ThoiGian',    'Dim_ThoiGian'),
    ('Dim_DiaDiem',     'Dim_DiaDiem'),
    ('Dim_MatHang',     'Dim_MatHang'),
    ('Dim_CuaHang',     'Dim_CuaHang'),
    ('Dim_KhachHang',   'Dim_KhachHang'),
    ('Fact_DonDatHang', 'Fact_DonDatHang'),
    ('Fact_TonKho',     'Fact_TonKho'),
]


def get_clickhouse_client(host='localhost', port=8123):
    """Tạo ClickHouse client (host=localhost khi chạy ngoài Docker)"""
    for i in range(3):
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username='admin',
                password='admin', 
                database='default'
            )
            client.command('SELECT 1')
            print(f"✓ ClickHouse is ready! ({host}:{port})")
            return client
        except Exception as e:
            print(f"Waiting for ClickHouse... ({i+1}/3): {e}")
            time.sleep(2)
    raise Exception("ClickHouse not available")


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hóa DataFrame trước khi insert vào ClickHouse"""
    df = df.copy()
    # Chuyển numpy int/float sang Python native để clickhouse-connect xử lý đúng
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].astype(object).where(df[col].notna(), None)
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].astype(object).where(df[col].notna(), None)
    # Thay NaN/NaT bằng None
    df = df.where(pd.notnull(df), None)
    return df


def load_to_clickhouse(dw_data, host='localhost', port=8123):
    """Load toàn bộ dw_data vào ClickHouse theo đúng thứ tự"""

    print("\n=== LOADING DATA TO CLICKHOUSE ===")

    client = get_clickhouse_client(host, port)
    total = len(LOAD_ORDER)

    for i, (data_key, table_name) in enumerate(LOAD_ORDER, 1):
        if data_key not in dw_data:
            print(f"[{i}/{total}] ⚠ Skipping {table_name} (not in dw_data)")
            continue

        df = _clean_df(dw_data[data_key])

        # Xóa dữ liệu cũ
        try:
            client.command(f'TRUNCATE TABLE {table_name}')
        except Exception:
            pass  # Bảng chưa tồn tại hoặc không cần truncate

        print(f"[{i}/{total}] Loading {table_name} ({len(df):,} rows)...")

        try:
            client.insert_df(table_name, df)
            print(f"  ✓ {table_name}")
        except Exception as e:
            print(f"  ✗ Error loading {table_name}: {e}")
            raise

    # Verification
    print("\n=== VERIFICATION ===")
    for _, table_name in LOAD_ORDER:
        try:
            count = client.command(f'SELECT COUNT(*) FROM {table_name}')
            print(f"  {table_name:<20s}: {count:>8,} rows")
        except Exception as e:
            print(f"  {table_name:<20s}: error — {e}")

    print("\n✓ Load completed!")
    return True


if __name__ == "__main__":
    from extract import extract_from_postgres
    from transform import transform_data

    source_data = extract_from_postgres()
    dw_data     = transform_data(source_data)
    load_to_clickhouse(dw_data)
