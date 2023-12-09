{{ config(materialized='table') }}

with source_data as (
SELECT
  player_fullname,
  game_date,
  player_team as team,
  opponent_team,
  LEFT(game_result, 1) AS game_result,
  player_minutes,
  player_points,
  player_3pts,
  player_2pts,
  player_fts,
  player_assists,
  player_3pts_attempted,
  IFNULL(player_3pts_pctg,0) as player_3pts_pctg,
  IFNULL(player_fts_pctg,0) as player_fts_pctg,
  IFNULL(player_2pts_pctg,0) as player_2pts_pctg,
  IFNULL(player_field_goals_pctg,0) as player_field_goals_pctg,
  case when player_points is null then 0 when player_points = 0 then 0 else IFNULL(player_3pts*3/player_points,0) end as player_3pts_contribution,
  case when player_points is null then 0 when player_points = 0 then 0 else IFNULL(player_fts/player_points,0) end as player_fts_contribution,
  case when player_points is null then 0 when player_points = 0 then 0 else IFNULL(player_2pts*2/player_points,0) end as player_2pts_contribution,
  player_total_rebounds,
  player_steals,
  player_blocks
FROM
  `wagon-bootcamp-57100.nba_stats.player_log_temp`
),
dict_player as(
  select * from {{ ref('dim_player_infos') }}
)

-- dict_team as (
--   SELECT * FROM  {{ ref('dim_team_infos') }}
-- )

Select distinct * from source_data
left join dict_player using(player_fullname)
-- left join dict_team using(team)
