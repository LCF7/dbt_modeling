WITH raw_sales_reps AS (
    SELECT
        *
    FROM
        {{ source('deal', 'sales_reps') }}
)

SELECT
    REP_ID AS sales_rep_id,
    FULL_NAME AS sales_rep_name,
    REGION AS region_name,
    HIRE_DATE AS hire_date,
    SENIORITY_LEVEL AS seniority_level,
    QUOTA_AMOUNT AS quota_amount,
    _LOADED_AT AS loaded_at
FROM
    raw_sales_reps