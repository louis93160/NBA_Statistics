{{ config(materialized='table') }}

SELECT
  team_full_name,
  team_conference,
  ROUND((SUM(CASE WHEN team_result = 'W' THEN 1 ELSE 0 END)/(SUM(CASE WHEN team_result = 'W' THEN 1 ELSE 0 END)+SUM(CASE WHEN team_result = 'L' THEN 1 ELSE 0 END))), 3) winning_pctg,
  rank() over(partition by team_conference order by (SUM(CASE WHEN team_result = 'W' THEN 1 ELSE 0 END)/(SUM(CASE WHEN team_result = 'W' THEN 1 ELSE 0 END)+SUM(CASE WHEN team_result = 'L' THEN 1 ELSE 0 END))) DESC) conference_rank,
FROM {{ ref('fact_team_logs') }}
GROUP BY 1,2
