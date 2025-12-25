WITH raw_deals AS (
    SELECT
        *
    FROM
        {{ source('deal', 'deals') }}
)

SELECT
    DEAL_ID AS deal_id,
    CLIENT_ID AS client_id,
    SALES_REP_ID AS sales_rep_id,
    
    
    DEAL_STAGE AS deal_stage,
    ESTIMATED_VALUE_USD AS estimated_value,
    PROBABILITY_PERCENT AS probability_percent,
    
    
    EXPECTED_CLOSE_DATE AS expected_close_date,
    CREATION_DATE AS created_at,
    LAST_UPDATED_DATE AS updated_at,
    
    _LOADED_AT AS loaded_at
FROM
    raw_deals