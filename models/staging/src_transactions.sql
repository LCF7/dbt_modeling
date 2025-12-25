WITH raw_transactions AS (
    SELECT
        *
    FROM
        {{ source('deal', 'transactions') }}
)

SELECT
    TRANSACTION_ID AS transaction_id,
    DEAL_ID AS deal_id,
    
    
    REVENUE_AMOUNT_USD AS revenue_amount,
    
  
    STATUS AS transaction_status,
    TRANSACTION_DATE AS transaction_date,
    
    _LOADED_AT AS loaded_at
FROM
    raw_transactions