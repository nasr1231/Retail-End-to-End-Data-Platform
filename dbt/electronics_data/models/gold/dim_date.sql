{{ config(materialized='table', schema='gold') }}

WITH date_spine AS (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('2015-01-01' as date)",
      end_date="cast('2021-12-31' as date)"
     )
  }}
),

spine AS (
  SELECT
      date_day AS date_key
  FROM date_spine
),

holidays AS (
    SELECT
        date_key,
        /* ---------------- UNITED STATES ---------------- */
        CASE 
            WHEN MONTH(date_key)=1 AND DAY(date_key)=1 THEN 'US - New Year''s Day'
            WHEN MONTH(date_key)=7 AND DAY(date_key)=4 THEN 'US - Independence Day'
            WHEN MONTH(date_key)=12 AND DAY(date_key)=25 THEN 'US - Christmas'
        END AS holiday
    FROM spine

    UNION ALL
    SELECT 
        date_key,
        /* ---------------- UNITED KINGDOM ---------------- */
        CASE
            WHEN MONTH(date_key)=1 AND DAY(date_key)=1 THEN 'UK - New Year''s Day'
            WHEN MONTH(date_key)=12 AND DAY(date_key)=25 THEN 'UK - Christmas'
        END
    FROM spine

    UNION ALL
    SELECT 
        date_key,
        /* ---------------- NETHERLANDS ---------------- */
        CASE
            WHEN MONTH(date_key)=1 AND DAY(date_key)=1 THEN 'NL - New Year''s Day'
            WHEN MONTH(date_key)=4 AND DAY(date_key)=27 THEN 'NL - King''s Day'
        END
    FROM spine

    UNION ALL
    SELECT 
        date_key,
        /* ---------------- FRANCE ---------------- */
        CASE
            WHEN MONTH(date_key)=1 AND DAY(date_key)=1 THEN 'FR - New Year''s Day'
            WHEN MONTH(date_key)=7 AND DAY(date_key)=14 THEN 'FR - Bastille Day'
        END
    FROM spine

    UNION ALL
    SELECT 
        date_key,
        /* ---------------- GERMANY ---------------- */
        CASE
            WHEN MONTH(date_key)=10 AND DAY(date_key)=3 THEN 'DE - Unity Day'
        END
    FROM spine

    UNION ALL
    SELECT 
        date_key,
        /* ---------------- ITALY ---------------- */
        CASE
            WHEN MONTH(date_key)=6 AND DAY(date_key)=2 THEN 'IT - Republic Day'
        END
    FROM spine

    UNION ALL
    SELECT 
        date_key,
        /* ---------------- CANADA ---------------- */
        CASE
            WHEN MONTH(date_key)=7 AND DAY(date_key)=1 THEN 'CA - Canada Day'
        END
    FROM spine

    UNION ALL
    SELECT 
        date_key,
        /* ---------------- AUSTRALIA ---------------- */
        CASE
            WHEN MONTH(date_key)=1 AND DAY(date_key)=26 THEN 'AU - Australia Day'
        END
    FROM spine
)

SELECT
    ROW_NUMBER() OVER (ORDER BY d.date_key) AS date_sk,
    d.date_key,
    EXTRACT(YEAR FROM d.date_key) AS year,
    EXTRACT(MONTH FROM d.date_key) AS month,
    EXTRACT(DAY FROM d.date_key) AS day,
    EXTRACT(QUARTER FROM d.date_key) AS quarter,
    CASE WHEN h.holiday IS NOT NULL THEN 1 ELSE 0 END AS is_holiday,
    h.holiday AS holiday_name
FROM spine d
LEFT JOIN holidays h
    ON d.date_key = h.date_key
