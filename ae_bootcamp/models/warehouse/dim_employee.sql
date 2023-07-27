WITH 

employees AS (
    SELECT
        id AS employee_id,
        company,
        last_name,
        first_name,
        email_address,
        job_title,
        business_phone,
        home_phone,
        mobile_phone,
        fax_number,
        address,
        city,
        state_province,
        zip_postal_code,
        country_region,
        web_page,
        notes,
        attachments,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM {{ ref('stg_employees') }}
),

unique_employees AS (
    SELECT *
    FROM employees
    QUALIFY row_number() OVER (PARTITION BY employee_id) = 1
)

SELECT *
FROM unique_employees