
{{ config(materialized='table') }}

with source_data as (
    select * from `wagon-bootcamp-57100.nba_stats.game_log_temp`
),
dict_team as (
  SELECT team_code as team, team_city, team_nickname, team_full_name, url_team_logo FROM `wagon-bootcamp-57100.nba_stats.team`
),
dict_opp as (
  SELECT team_code as opponent, team_city opponent_team_city,team_nickname opponent_team_nickname,team_full_name opponent_team_full_name,url_team_logo opponent_url_team_logo FROM `wagon-bootcamp-57100.nba_stats.team`
)

Select * from source_data
left join dict_team using(team)
left join dict_opp using(opponent)
