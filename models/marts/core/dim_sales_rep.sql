WITH stg_reps AS (
    SELECT * FROM {{ ref('src_sales_reps') }}
)

SELECT
    -- Surrogate Key (Best Practice)
    {{ dbt_utils.generate_surrogate_key(['sales_rep_id']) }} AS sales_rep_key,
    
    -- Natural Key
    sales_rep_id,
    
    -- Atributos
    sales_rep_name,
    region_name,
    seniority_level,
    
    -- Feature Engineering: Antigüedad en años (calculada al vuelo)
    DATEDIFF(year, hire_date, CURRENT_DATE) AS tenure_years,
    
    quota_amount,
    hire_date

FROM stg_reps