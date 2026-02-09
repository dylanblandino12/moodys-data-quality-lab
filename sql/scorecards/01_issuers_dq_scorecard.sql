-- =============================================
-- File: 01_issuers_dq_scorecard.sql
-- Purpose: Data Quality summary scorecard for issuers table
-- Platform: BigQuery
-- =============================================

WITH base AS (
  SELECT *
  FROM `moodys_dq_lab.issuers_dirty`
),

dup_codes AS (
  SELECT issuer_code, COUNT(*) cnt
  FROM base
  GROUP BY issuer_code
  HAVING COUNT(*) > 1
),

duplicate_rows AS (
  SELECT SUM(cnt) - COUNT(*) AS failed_rows
  FROM dup_codes
)

-- =============================================
-- Scorecard
-- =============================================

-- Rule 1: issuer duplicates
SELECT
  'issuer_code_duplicates' AS rule_name,
  SUM(cnt) - COUNT(*) AS failed_rows,
  (SELECT COUNT(*) FROM base) AS total_rows,
  SAFE_DIVIDE(SUM(cnt) - COUNT(*),(SELECT COUNT(*) FROM base)) AS pct_failed
FROM dup_codes
UNION ALL

-- Rule 2: invalid country
SELECT
  'country_invalid' AS rule_name,
  SUM(CASE WHEN country IS NULL OR LENGTH(country) != 2 THEN 1 ELSE 0 END) AS failed_rows,
  COUNT(*) AS total_rows,
  SAFE_DIVIDE(
    SUM(CASE WHEN country IS NULL OR LENGTH(country) != 2 THEN 1 ELSE 0 END),
    COUNT(*)
  ) AS pct_failed
FROM base

UNION ALL

-- Rule 3: invalid revenue
SELECT
  'revenue_invalid' AS rule_name,
  SUM(CASE WHEN annual_revenue <= 0 THEN 1 ELSE 0 END) AS failed_rows,
  COUNT(*) AS total_rows,
  SAFE_DIVIDE(
    SUM(CASE WHEN annual_revenue <= 0 THEN 1 ELSE 0 END),
    COUNT(*)
  ) AS pct_failed
FROM base;
