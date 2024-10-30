WITH date_spine AS (
    -- Generate all dates from June to September 2020
    SELECT generate_series(
        '2020-06-01'::date,
        '2020-09-30'::date,
        '1 day'::interval
    )::date AS dt_report
),
enabled_users AS (
    -- Filter for enabled users only (enable = 1) and get their currency
    SELECT DISTINCT login_hash, currency
    FROM users
    WHERE enable = 1
),
valid_trades AS (
    -- Base trades data with quality control fixes
    SELECT 
        t.login_hash,
        t.server_hash,
        t.symbol,
        eu.currency,  -- Get currency from enabled_users
        t.volume,
        t.close_time,
        CASE 
            WHEN t.volume < 0 THEN ABS(t.volume)  -- Handle negative volumes
            ELSE t.volume
        END AS cleaned_volume
    FROM trades t
    INNER JOIN enabled_users eu ON t.login_hash = eu.login_hash
),
base_combinations AS (
    -- Get all unique combinations of login/server/symbol/currency
    SELECT DISTINCT
        login_hash,
        server_hash,
        symbol,
        currency
    FROM valid_trades
),
trade_counts AS (
    -- Calculate trade counts for ranking
    SELECT 
        d.dt_report,
        vt.login_hash,
        COUNT(vt.cleaned_volume) as trade_count
    FROM date_spine d
    CROSS JOIN (SELECT DISTINCT login_hash FROM valid_trades) l
    LEFT JOIN valid_trades vt ON 
        vt.login_hash = l.login_hash
        AND vt.close_time::date <= d.dt_report 
        AND vt.close_time::date > d.dt_report - INTERVAL '7 days'
    GROUP BY d.dt_report, vt.login_hash
),
daily_stats AS (
    SELECT 
        d.dt_report,
        bc.login_hash,
        bc.server_hash,
        bc.symbol,
        bc.currency,
        -- Previous 7 days volume
        COALESCE(
            SUM(vt.cleaned_volume) FILTER (
                WHERE vt.close_time::date <= d.dt_report 
                AND vt.close_time::date > d.dt_report - INTERVAL '7 days'
            ) OVER (PARTITION BY bc.login_hash, bc.server_hash, bc.symbol)
        , 0) AS sum_volume_prev_7d,
        -- All previous days volume
        COALESCE(
            SUM(vt.cleaned_volume) FILTER (
                WHERE vt.close_time::date <= d.dt_report
            ) OVER (PARTITION BY bc.login_hash, bc.server_hash, bc.symbol)
        , 0) AS sum_volume_prev_all,
        -- August 2020 volume
        COALESCE(
            SUM(vt.cleaned_volume) FILTER (
                WHERE DATE_TRUNC('month', vt.close_time::date) = '2020-08-01'::date
                AND vt.close_time::date <= d.dt_report
            ) OVER (PARTITION BY bc.login_hash, bc.server_hash, bc.symbol)
        , 0) AS sum_volume_2020_08,
        -- First trade date
        MIN(vt.close_time) OVER (
            PARTITION BY bc.login_hash, bc.server_hash, bc.symbol
        ) AS date_first_trade
    FROM date_spine d
    CROSS JOIN base_combinations bc
    LEFT JOIN valid_trades vt ON 
        vt.login_hash = bc.login_hash 
        AND vt.server_hash = bc.server_hash 
        AND vt.symbol = bc.symbol
)
SELECT 
    ds.dt_report,
    ds.login_hash,
    ds.server_hash,
    ds.symbol,
    ds.currency,
    ds.sum_volume_prev_7d,
    ds.sum_volume_prev_all,
    DENSE_RANK() OVER (
        PARTITION BY ds.dt_report, ds.login_hash
        ORDER BY ds.sum_volume_prev_7d DESC NULLS LAST
    ) as rank_volume_symbol_prev_7d,
    DENSE_RANK() OVER (
        PARTITION BY ds.dt_report
        ORDER BY tc.trade_count DESC NULLS LAST
    ) as rank_count_prev_7d,
    ds.sum_volume_2020_08,
    ds.date_first_trade,
    ROW_NUMBER() OVER (
        ORDER BY ds.dt_report DESC, ds.login_hash, ds.server_hash, ds.symbol
    ) as row_number
FROM daily_stats ds
LEFT JOIN trade_counts tc ON 
    tc.dt_report = ds.dt_report 
    AND tc.login_hash = ds.login_hash
ORDER BY row_number DESC;