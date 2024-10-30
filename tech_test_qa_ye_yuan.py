import psycopg2
import pandas as pd
from datetime import datetime
import numpy as np
from typing import Optional, Dict

def connect_to_db():
    """
    Connect to the PostgreSQL database
    """
    
    try:
        conn = psycopg2.connect(
            host="technical-test-1.cncti7m4kr9f.ap-south-1.rds.amazonaws.com",
            database="technical_test",
            user="candidate",
            password="NW337AkNQH76veGc",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

    
def check_string_values(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Check for unexpected string values in the dataset
    """
    
    query = """
    SELECT DISTINCT
        symbol,
        currency,
        country_hash
    FROM trades t
    LEFT JOIN users u ON t.login_hash = u.login_hash;
    """
    results = pd.read_sql(query, conn)
    print("\n=== String Values Check ===")
    print("Unique values in categorical columns:")
    for column in results.columns:
        print(f"\n{column}:")
        print(results[column].value_counts())
    return results


def check_numerical_values(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Check for unexpected numerical values in the dataset
    """
    
    query = """
    SELECT
        'volume' as field,
        MIN(volume) as min_value,
        MAX(volume) as max_value,
        AVG(volume) as avg_value,
        COUNT(*) FILTER (WHERE volume < 0) as negative_count,
        COUNT(*) FILTER (WHERE volume = 0) as zero_count
    FROM trades
    
    UNION ALL
    
    SELECT
        'digits' as field,
        MIN(digits) as min_value,
        MAX(digits) as max_value,
        AVG(digits) as avg_value,
        COUNT(*) FILTER (WHERE digits < 0) as negative_count,
        COUNT(*) FILTER (WHERE digits = 0) as zero_count
    FROM trades
    
    UNION ALL
    
    SELECT
        'contractsize' as field,
        MIN(contractsize) as min_value,
        MAX(contractsize) as max_value,
        AVG(contractsize) as avg_value,
        COUNT(*) FILTER (WHERE contractsize < 0) as negative_count,
        COUNT(*) FILTER (WHERE contractsize = 0) as zero_count
    FROM trades;
    """
    results = pd.read_sql(query, conn)
    print("\n=== Numerical Values Check ===")
    print(results)
    return results


def check_date_values(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Check for unexpected date values and patterns in the dataset
    """
    
    query = """
    SELECT
        MIN(open_time) as min_open_time,
        MAX(open_time) as max_open_time,
        MIN(close_time) as min_close_time,
        MAX(close_time) as max_close_time,
        COUNT(*) FILTER (WHERE close_time < open_time) as invalid_time_order,
        COUNT(*) FILTER (WHERE close_time = 'epoch') as open_trades,
        COUNT(*) FILTER (WHERE open_time IS NULL) as null_open_times,
        COUNT(*) FILTER (WHERE close_time IS NULL) as null_close_times,
        COUNT(*) as total_trades
    FROM trades;
    """
    results = pd.read_sql(query, conn)
    print("\n=== Date Values Check ===")
    print(results)
    return results

def check_data_integrity(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Check data integrity between tables and within tables
    """
    
    query = """
    WITH integrity_checks AS (
        SELECT
            COUNT(*) as total_trades,
            COUNT(DISTINCT t.login_hash) as distinct_trade_logins,
            COUNT(DISTINCT u.login_hash) as distinct_user_logins,
            COUNT(*) FILTER (WHERE u.login_hash IS NULL) as orphaned_trades,
            COUNT(*) FILTER (WHERE t.server_hash IS NULL) as null_server_hash,
            COUNT(*) FILTER (WHERE t.ticket_hash IS NULL) as null_ticket_hash
        FROM trades t
        LEFT JOIN users u ON t.login_hash = u.login_hash
    )
    SELECT
        *,
        CASE 
            WHEN orphaned_trades > 0 THEN 'Warning: Found trades without matching users'
            ELSE 'OK: All trades have matching users'
        END as integrity_status
    FROM integrity_checks;
    """
    results = pd.read_sql(query, conn)
    print("\n=== Data Integrity Check ===")
    print(results)
    return results

def check_business_rules(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Check business rules and constraints
    """
    
    query = """
    SELECT
        'cmd_values' as check_type,
        COUNT(*) FILTER (WHERE cmd NOT IN (0, 1)) as invalid_cmd_count,
        COUNT(*) as total_records
    FROM trades
    UNION ALL
    SELECT
        'enable_status' as check_type,
        COUNT(*) FILTER (WHERE enable IS NULL) as null_enable_count,
        COUNT(*) as total_records
    FROM users;
    """
    results = pd.read_sql(query, conn)
    print("\n=== Business Rules Check ===")
    print(results)
    
    # Additional specific checks for cmd values
    cmd_check = pd.read_sql("SELECT DISTINCT cmd FROM trades ORDER BY cmd;", conn)
    print("\nUnique cmd values:", cmd_check['cmd'].tolist())
    
    # Check enabled accounts distribution
    enabled_check = pd.read_sql("""
        SELECT enable, COUNT(*) 
        FROM users 
        GROUP BY enable 
        ORDER BY enable;
    """, conn)
    print("\nAccount status distribution:")
    print(enabled_check)
    
    return results

def generate_summary_report(all_results: Dict[str, pd.DataFrame]):
    """
    Generate a summary report of all data quality checks
    Args:
        all_results: Dictionary containing results from all checks
    """
    
    print("\n=== SUMMARY REPORT ===")
    print("\nPotential Data Quality Issues:")
    
    # Check numerical values
    numerical_issues = []
    if 'numerical' in all_results:
        df = all_results['numerical']
        for _, row in df.iterrows():
            if row['negative_count'] > 0:
                numerical_issues.append(f"- Found {row['negative_count']} negative values in {row['field']}")
            if row['zero_count'] > 0 and row['field'] != 'digits':
                numerical_issues.append(f"- Found {row['zero_count']} zero values in {row['field']}")
    
    # Check data integrity
    integrity_issues = []
    if 'integrity' in all_results:
        df = all_results['integrity']
        if df['orphaned_trades'].iloc[0] > 0:
            integrity_issues.append(f"- Found {df['orphaned_trades'].iloc[0]} trades without matching users")
    
    # Print all issues
    if numerical_issues:
        print("\nNumerical Issues:")
        for issue in numerical_issues:
            print(issue)
    
    if integrity_issues:
        print("\nIntegrity Issues:")
        for issue in integrity_issues:
            print(issue)

    
def check_data_quality():
    """
    Main function to perform all data quality checks
    """
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        # Store results 
        results = {}
        
        # Perform all checks
        results['string'] = check_string_values(conn)
        results['numerical'] = check_numerical_values(conn)
        results['dates'] = check_date_values(conn)
        results['integrity'] = check_data_integrity(conn)
        results['business'] = check_business_rules(conn)
        
        # Generate summary report
        generate_summary_report(results)
        
    except Exception as e:
        print(f"Error during data quality check: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_data_quality()