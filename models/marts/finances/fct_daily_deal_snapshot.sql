{{ config(
    materialized = 'incremental',
    unique_key = 'daily_snapshot_id'
) }}

WITH date_spine AS (
    -- Generate a continuous calendar (1 row per day)
    -- Requires dbt-utils package installed in packages.yml
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="dateadd(day, 1, current_date)"
    ) }}
),

history AS (
    SELECT * FROM {{ ref('deals_snapshot') }}
),

joined AS (
    SELECT
        -- Unique surrogate key for this table (Deal + Date)
        {{ dbt_utils.generate_surrogate_key(['h.deal_id', 'd.date_day']) }} AS daily_snapshot_id,
        
        d.date_day AS snapshot_date,
        h.deal_id,
        h.sales_rep_id, -- To know who owned the deal on THAT day
        
        -- Status on THAT specific day
        h.deal_stage,
        h.estimated_value,
        h.probability_percent,

        -- CRITICAL CALCULATION FOR ML: Time-in-Stage
        -- Subtract snapshot date minus the date when that stage started
        DATEDIFF(day, h.dbt_valid_from, d.date_day) + 1 AS days_in_current_stage

    FROM date_spine d
    INNER JOIN history h
        -- Range Join: Join if the calendar day falls WITHIN the validity of the historical record
        ON d.date_day >= CAST(h.dbt_valid_from AS DATE)
        AND d.date_day < COALESCE(CAST(h.dbt_valid_to AS DATE), DATEADD(day, 1, CURRENT_DATE()))
)

SELECT * FROM joined
{% if is_incremental() %}
  -- In incremental loads, only process new days
  WHERE snapshot_date > (SELECT max(snapshot_date) FROM {{ this }})
{% endif %}