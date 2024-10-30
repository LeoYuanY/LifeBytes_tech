# LifeBytes_tech

## Task 1
# SQL Query Results Analysis

## Dataset Overview
- **Total Rows**: 912,194
- **Time Period**: June - September 2020 (122 days)
- **Unique Users**: 100 login hashes (enabled accounts only)
- **Trading Instruments**: 42 unique symbols
- **Data Completeness**: Full combination coverage across dates, users, servers, symbols, and currencies

## Data Distribution Analysis

### Volume Statistics
| Metric | Previous 7 Days | All Previous Days | August 2020 |
|--------|----------------|-------------------|-------------|
| Mean | 9,097.65 | 58,778.08 | 58,577.02 |
| Median | 8,148.00 | 50,041.00 | 50,041.00 |
| Max | 108,710.00 | 797,100.00 | 797,100.00 |
| Min | 0.00 | 0.00 | 0.00 |

### Ranking Distribution
- **Volume Ranking (rank_volume_symbol_prev_7d)**
  - Range: 1-17
  - Mean: 1.70
  - Most common rank: 1 (median and 25th percentile both 1)

- **Trade Count Ranking (rank_count_prev_7d)**
  - Range: 1-41
  - Mean: 4.46
  - Most trades concentrated in fewer accounts (75th percentile is 3)

## Requirements Validation

### 1. Complete Combination Coverage
- 912,194 rows = 122 days × 100 users × 42 symbols × multiple servers
- Confirms complete coverage of all required combinations

### 2. Date Range Coverage
- All dates from June to September 2020 included
- 122 unique days present in the dataset
- First date in sample: 2020-06-01

### 3. User Filtering
- Only enabled accounts included (100 unique login hashes)
- All users exist in the users table
- Currency properly linked to each user

### 4. Data Quality Controls
- No negative volumes (all values ≥ 0)
- Proper handling of missing data (zeros for no trades)
- First trade dates properly tracked
- Consistent currency assignment

### 5. Sorting
- Data ordered by row_number in descending order
- Row numbers range from 1 to 912,194
- No gaps in numbering


## Key Findings
1. **Complete Coverage**: All required combinations are present in the results
2. **Volume Distribution**: Wide range of trading volumes (0 to 797,100)
3. **Trading Activity**: Some users/symbols more active than others (rank ranges show concentration)
4. **Data Quality**: No anomalies detected in core metrics
5. **Temporal Consistency**: All required dates included with proper handling of missing data

## Technical Implementation
- Uses date spine to ensure all dates are covered
- Implements CROSS JOIN for complete combinations
- Applies proper window functions for rankings
- Handles NULL values with COALESCE
- Incorporates data quality controls from Task 1

This analysis confirms that the query successfully meets all requirements while maintaining data quality and completeness.


## Task 2
# Data Quality Analysis Report

## Overview
This report presents the findings from a comprehensive data quality analysis of the trading database, examining trades and user data across multiple dimensions.

## Table of Contents
- [Data Distribution Analysis](#data-distribution-analysis)
- [Data Quality Issues](#data-quality-issues)
- [Detailed Analysis](#detailed-analysis)

## Data Distribution Analysis

### Trading Instruments
- Total unique symbols: 71
- Most traded instruments:
  - XAUUSD: 12 trades
  - GBPUSD: 10 trades
  - EURUSD: 10 trades
  - USDJPY: 9 trades
  - EURJPY: 8 trades

### Currencies
Distribution of account currencies:
- USD: 104 accounts
- AUD: 48 accounts
- NZD: 3 accounts

### Geographic Distribution
- 12 unique country hashes
- Top countries by user count:
  1. 4442E4AF0916F53A07FB8CA9A49B98ED: 71 users
  2. 551FE18EF47D4E6E9D943B9A68ADA21D: 27 users
  3. AE54A5C026F31ADA088992587D92CB3A: 16 users

## Data Quality Issues

### Critical Issues
1. **Orphaned Trades**
   - 91,953 trades (71.8%) have no matching user records
   - Total trades: 128,020
   - Trades with valid users: 36,067

2. **Account Relationships**
   - Distinct trading accounts: 5,142
   - Distinct users: 116
   - Ratio: ~44 accounts per user

### Numerical Validations

#### Volume Statistics
- Range: 0 to 10,000
- Average: 17.99
- Issues:
  - One trade with zero volume
  - No negative values (positive finding)

#### Contract Size
- Range: 1 to 100,000
- Average: 44,856.84
- No anomalies detected

#### Precision (Digits)
- Range: 0 to 5
- Average: 3.21
- 1,168 zero-digit entries

### Temporal Analysis

#### Trading Period
- Open Times: 2020-08-03 00:01:05 to 2020-08-31 23:58:39
- Close Times: 2020-08-03 00:36:19 to 2022-08-18 23:57:18
- No invalid time orders
- No null timestamps

### Rules Validation

#### Command Types
- All trades have valid cmd values (0 or 1)
- No invalid commands detected

#### Account Status
Total accounts: 1,000
- Enabled: 973 (97.3%)
- Disabled: 27 (2.7%)

## Technical Details
- Database: PostgreSQL
- Total Records Analyzed: 128,020
- Analysis Period: August 2020 to August 2022
- Script: `tech_test_qa_ye_yuan.python`
