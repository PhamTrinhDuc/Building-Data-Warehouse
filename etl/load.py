"""
Load module - Ghi dữ liệu vào ClickHouse Data Warehouse
"""
import clickhouse_connect
import pandas as pd
import time


def wait_for_clickhouse(max_retries=3):
    """Đợi ClickHouse sẵn sàng"""
    for i in range(max_retries):
        try:
            client = clickhouse_connect.get_client(
                host='clickhouse',
                port=8123,
                username='default',
                password=''
            )
            client.command('SELECT 1')
            print("✓ ClickHouse is ready!")
            return client
        except Exception as e:
            print(f"Waiting for ClickHouse... ({i+1}/{max_retries})")
            time.sleep(2)
    raise Exception("ClickHouse not available")


def load_to_clickhouse(dw_data):
    """Load dữ liệu vào ClickHouse DW"""
    
    print("\n=== LOADING DATA TO CLICKHOUSE ===")
    
    # Kết nối ClickHouse
    client = wait_for_clickhouse()
    
    # Load từng bảng
    tables_to_load = [
        'dim_khach_hang',
        'dim_mat_hang',
        'dim_cua_hang',
        'fact_don_dat_hang',
        'fact_ton_kho'
    ]
    
    for i, table_name in enumerate(tables_to_load, 1):
        if table_name not in dw_data:
            print(f"[{i}/{len(tables_to_load)}] Skipping {table_name} (no data)")
            continue
            
        df = dw_data[table_name].copy()
        
        # Xử lý dữ liệu trước khi load
        # Convert NaN to None for nullable columns
        df = df.where(pd.notnull(df), None)
        
        # Xóa dữ liệu cũ (nếu có)
        try:
            client.command(f'TRUNCATE TABLE {table_name}')
        except:
            pass
        
        print(f"[{i}/{len(tables_to_load)}] Loading {table_name} ({len(df)} rows)...")
        
        # Insert data
        try:
            client.insert_df(table_name, df)
            print(f"  ✓ Loaded {len(df)} rows to {table_name}")
        except Exception as e:
            print(f"  ✗ Error loading {table_name}: {str(e)}")
            raise
    
    # Verify data
    print("\n=== VERIFICATION ===")
    for table_name in tables_to_load:
        try:
            count = client.command(f'SELECT COUNT(*) FROM {table_name}')
            print(f"  {table_name:25s}: {count:6d} rows")
        except:
            print(f"  {table_name:25s}: Error reading")
    
    print("\n✓ Load completed!")
    return True


if __name__ == "__main__":
    from extract import extract_from_postgres
    from transform import transform_data
    
    source_data = extract_from_postgres()
    dw_data = transform_data(source_data)
    load_to_clickhouse(dw_data)
