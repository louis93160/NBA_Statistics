{{ config(materialized='table') }}

with source_data as (
    select
    game_date,
    team,
    opponent,
    LEFT(team_result, 1) AS team_result,
    team_points,
    ROUND(team_field_goals_pctg,2) as team_field_goals_pctg,
    round(team_2pts_pctg,2) as team_2pts_pctg,
    round(team_3pts_pctg,2) as team_3pts_pctg,
    round(team_fts_pctg,2) as team_fts_pctg,
    (team_fts/team_points) as contribution_ft,
    (team_2pts*2/team_points) as contribution_2pts,
    (team_3pts*3/team_points) as contribution_3pts,
    team_total_rebounds,
    team_blocks,
    team_steals,
    team_assists,
    opp_points
 from `wagon-bootcamp-57100.nba_stats.game_log_temp`
),
dict_team as (
  SELECT * FROM  {{ ref('dim_team_infos') }}
)

Select distinct * from source_data
left join dict_team using(team)
