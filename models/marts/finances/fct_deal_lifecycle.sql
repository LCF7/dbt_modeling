WITH history AS (
    SELECT 
        deal_id,
        deal_stage,
        dbt_valid_from,
        -- If the deal is still active in that stage, close the calculation with TODAY
        COALESCE(dbt_valid_to, CURRENT_TIMESTAMP()) AS effective_valid_to
    FROM {{ ref('deals_snapshot') }}
),

stage_durations AS (
    SELECT
        deal_id,
        deal_stage,
        -- Calculate how long EACH micro-period lasted in that stage
        DATEDIFF(day, dbt_valid_from, effective_valid_to) AS duration_days
    FROM history
),

pivoted_lifecycle AS (
    SELECT
        deal_id,
        
        -- 1. Total Deal Age
        SUM(duration_days) AS total_deal_age_days,
        
        -- 2. Dynamic Pivoting
        SUM(CASE WHEN deal_stage = 'Discovery' THEN duration_days ELSE 0 END) AS days_in_discovery,
        SUM(CASE WHEN deal_stage = 'Negotiation' THEN duration_days ELSE 0 END) AS days_in_negotiation,
        SUM(CASE WHEN deal_stage = 'Contract' THEN duration_days ELSE 0 END) AS days_in_contract,
        
        -- Success Metric
        MAX(CASE WHEN deal_stage IN ('Closed Won', 'Closed Lost') THEN 1 ELSE 0 END) AS is_closed,
        
        -- "Stalled" Flag
        MAX(CASE WHEN deal_stage = 'Negotiation' AND duration_days > 30 THEN 1 ELSE 0 END) AS was_stalled_in_negotiation

    FROM stage_durations
    GROUP BY 1
)

SELECT 
    l.*,
    -- Bring in foreign keys to dimensions
    d.deal_key,
    
    -- CORRECTION: Use the real column name in dim_deal
    d.sales_rep_id

FROM pivoted_lifecycle l
JOIN {{ ref('dim_deal') }} d ON l.deal_id = d.deal_id