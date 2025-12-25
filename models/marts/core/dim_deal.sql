WITH current_deals AS (
    -- Take the "current" version of the snapshot for the current dimension
    -- (Note: If you haven't run the snapshot yet, dbt run will fail. 
    -- For the first time, you can point to src_deals, but snapshot is ideal)
    SELECT * FROM {{ ref('deals_snapshot') }}
    WHERE dbt_valid_to IS NULL
),

actual_revenue AS (
    -- Aggregate transactions to know how much REAL money came in
    SELECT 
        deal_id,
        SUM(revenue_amount) AS total_realized_revenue
    FROM {{ ref('src_transactions') }}
    WHERE transaction_status = 'Completed'
    GROUP BY 1
),

reps AS (
    SELECT * FROM {{ ref('src_sales_reps') }}
)

SELECT
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['d.deal_id']) }} AS deal_key,
    
    -- IDs
    d.deal_id,
    d.client_id,
    d.sales_rep_id,
    
    -- Sales Rep Context (Denormalized for BI ease)
    r.sales_rep_name,
    r.region_name AS sales_region,
    
    -- Deal Attributes
    d.deal_stage,
    d.probability_percent,
    
    -- Financial Analysis (Forecast vs Reality)
    d.estimated_value AS forecast_value,
    COALESCE(ar.total_realized_revenue, 0) AS realized_value,
    
    -- Feature: Variance (Positive = Overestimated)
    (d.estimated_value - COALESCE(ar.total_realized_revenue, 0)) AS revenue_variance,
    
    -- Dates
    d.created_at,
    d.expected_close_date,
    d.updated_at

FROM current_deals d
LEFT JOIN actual_revenue ar ON d.deal_id = ar.deal_id
LEFT JOIN reps r ON d.sales_rep_id = r.sales_rep_id