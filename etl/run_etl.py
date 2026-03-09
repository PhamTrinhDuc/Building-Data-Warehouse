"""
ETL Pipeline Entry Point
Chạy toàn bộ quy trình Extract -> Transform -> Load
"""
import sys
import traceback
from datetime import datetime
from extract import extract_from_postgres
from transform import transform_data
from load import load_to_clickhouse


def run_etl_pipeline():
    """Chạy ETL pipeline hoàn chỉnh"""
    
    start_time = datetime.now()
    
    print("=" * 60)
    print("ETL PIPELINE - Data Warehouse")
    print("=" * 60)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Step 1: Extract
        print("\n[STEP 1/3] EXTRACT")
        print("-" * 60)
        source_data = extract_from_postgres()
        
        # Step 2: Transform
        print("\n[STEP 2/3] TRANSFORM")
        print("-" * 60)
        dw_data = transform_data(source_data)
        
        # Step 3: Load
        print("\n[STEP 3/3] LOAD")
        print("-" * 60)
        load_to_clickhouse(dw_data)
        
        # Success
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY! ✓")
        print("=" * 60)
        print(f"Start time:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration:    {duration:.2f} seconds")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print("\n" + "=" * 60)
        print("ETL PIPELINE FAILED! ✗")
        print("=" * 60)
        print(f"Error: {str(e)}")
        print("\nTraceback:")
        traceback.print_exc()
        print("=" * 60)
        
        return 1


if __name__ == "__main__":
    exit_code = run_etl_pipeline()
    sys.exit(exit_code)
