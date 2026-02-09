-- =============================================
-- File: 01_issuers_profiling.sql
-- Purpose: Initial data profiling of issuers table
-- Goal:
--   - Assess completeness
--   - Check uniqueness
--   - Identify invalid values
--   - Quantify early data quality risks
-- Dataset: moodys_dq_lab.issuers_dirty
-- Platform: BigQuery
-- Author: Dylan Blandino
-- =============================================

-- Q0 - Granularity Overview


SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT issuer_id) AS unique_issuers,
  COUNT(DISTINCT issuer_code) AS unique_issuer_codes
FROM
  `moodys_dq_lab.issuers_dirty`
;

-- Finding:
-- 15 duplicate issuer records detected (515 rows vs 500 unique issuers).
-- This breaks the 1-row-per-issuer assumption and may cause double counting when joining with ratings or instruments.
-- Uniqueness should be enforced before downstream consumption.


-- =============================================
-- Q1 - Completeness checks
-- =============================================

SELECT
  SUM(CASE WHEN issuer_id IS NULL THEN 1 ELSE 0 END) AS null_issuers,
  SUM(CASE WHEN issuer_code IS NULL THEN 1 ELSE 0 END) AS null_issuer_codes,
  SUM(CASE WHEN issuer_name IS NULL THEN 1 ELSE 0 END) AS null_issuer_names,
  SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_countries,
  SUM(CASE WHEN industry IS NULL THEN 1 ELSE 0 END) AS null_industries,
  SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS null_status,
  SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) AS null_created_dates,
  SUM(CASE WHEN annual_revenue IS NULL THEN 1 ELSE 0 END) AS null_revenue,
  SAFE_DIVIDE(SUM(CASE WHEN issuer_id IS NULL THEN 1 ELSE 0 END),COUNT(*)) AS pct_null_issuers,
  SAFE_DIVIDE(SUM(CASE WHEN issuer_code IS NULL THEN 1 ELSE 0 END),COUNT(*)) AS pct_null_issuer_code,
  SAFE_DIVIDE(SUM(CASE WHEN issuer_name IS NULL THEN 1 ELSE 0 END),COUNT(*)) AS pct_null_issuer_name,
  SAFE_DIVIDE(SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END),COUNT(*)) AS pct_null_country,
  SAFE_DIVIDE(SUM(CASE WHEN industry IS NULL THEN 1 ELSE 0 END),COUNT(*)) AS pct_null_industry,
  SAFE_DIVIDE(SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END),COUNT(*)) AS pct_null_status,
  SAFE_DIVIDE(SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END),COUNT(*)) AS pct_null_created_date,
  SAFE_DIVIDE(SUM(CASE WHEN annual_revenue IS NULL THEN 1 ELSE 0 END),COUNT(*)) AS pct_null_annual_revenue,

FROM
  `moodys_dq_lab.issuers_dirty`;

-- Finding:
-- Country is the only column with missing values (25 records, ~4.9%).
-- Missing country impacts geographic segmentation, regulatory reporting, and regional risk models.
-- Records with NULL country should be completed or flagged before downstream analytics.


-- =============================================
-- Q2 - Uniqueness checks (issuer_code) - Metrics
-- =============================================

WITH dup_codes AS (
  SELECT
    issuer_code,
    COUNT(*) AS cnt
  FROM `moodys_dq_lab.issuers_dirty`
  GROUP BY issuer_code
  HAVING COUNT(*) > 1
)

SELECT
  COUNT(*) AS duplicated_codes,
  SUM(cnt) AS affected_rows,
  SUM(cnt) - COUNT(*) AS extra_rows,
  SAFE_DIVIDE(SUM(cnt) - COUNT(*),
              (SELECT COUNT(*) FROM `moodys_dq_lab.issuers_dirty`)) AS pct_extra_rows
FROM dup_codes;


-- Finding:
-- 15 extra issuer records detected beyond the expected 1-row-per-issuer structure.
-- All duplicated issuer_codes appear exactly twice, indicating a consistent 2-row duplication pattern.
-- These duplicates can cause double counting when joining with ratings or instruments, inflating exposure and risk metrics.
-- issuer_code should be enforced as a unique business key before downstream usage.


-- =============================================
-- Q2 - Uniqueness checks (issuer_code) - Detail
-- =============================================

WITH dup_codes AS (
  SELECT issuer_code
  FROM `moodys_dq_lab.issuers_dirty`
  GROUP BY issuer_code
  HAVING COUNT(*) > 1
)

SELECT
  d.*,
  COUNT(*) OVER (PARTITION BY issuer_code) AS duplicate_count
FROM `moodys_dq_lab.issuers_dirty` d
JOIN dup_codes USING (issuer_code)
ORDER BY issuer_code, issuer_id;


-- Purpose:
-- Returns all duplicated issuer records with their duplication count for root-cause analysis.


-- =============================================
-- Q3 - Accuracy checks (annual_revenue)
-- =============================================

SELECT
  SUM(CASE WHEN annual_revenue <= 0 THEN 1 ELSE 0 END) AS invalid_revenue_rows,
  COUNT(*) AS total_rows,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN annual_revenue <= 0 THEN 1 ELSE 0 END), COUNT(*)), 2) AS invalid_revenue_pct,
  MIN(annual_revenue) AS min_annual_revenue,
  MAX(annual_revenue) AS max_annual_revenue
FROM
  `moodys_dq_lab.issuers_dirty`;

-- Finding:
-- 15 records (~3%) contain zero or negative annual revenue values.
-- Revenue is used to segment issuers by company size and feed risk/exposure models.
-- Invalid values may misclassify large issuers as small, exclude them from analysis, or break financial ratios and models.
-- Revenue must be validated before downstream consumption.


-- =============================================
-- Q5 - Country validity checks
-- =============================================


-- ---------------------------------------------
-- Exploration: country distribution (top countries)
-- Purpose: understand geographic coverage and detect anomalies
-- ---------------------------------------------
SELECT
  country,
  COUNT(*) AS total_rows
FROM `moodys_dq_lab.issuers_dirty`
GROUP BY country
ORDER BY total_rows DESC;


-- ---------------------------------------------
-- Metrics: invalid or missing country codes
-- ---------------------------------------------
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN country IS NULL OR LENGTH(country) != 2 THEN 1 ELSE 0 END) AS invalid_rows,
  SAFE_DIVIDE(
    SUM(CASE WHEN country IS NULL OR LENGTH(country) != 2 THEN 1 ELSE 0 END),
    COUNT(*)
  ) AS pct_invalid
FROM `moodys_dq_lab.issuers_dirty`;


-- Finding:
-- Some issuer records contain missing or non-ISO country codes.
-- Country is required for geographic segmentation, regional risk analysis, and regulatory reporting.
-- Invalid or NULL values prevent correct regional comparisons and may distort country-level risk assessments.
-- Country codes should be standardized and validated before downstream analytics.



-- =============================================
-- Q5 - Summary metrics
-- =============================================


-- Duplicates detected: 15 extra rows
-- Invalid country: 25 rows (~5%)
-- Invalid revenue: 15 rows (~3%)
-- See official metrics in:
--   sql/scorecards/01_issuers_dq_scorecard.sql

