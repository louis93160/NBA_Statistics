
with t1 as (SELECT
  player_id,
  player_firstname,
  player_lastname,
  player_fullname,
  case when LOWER(player_fullname) = "kentavious caldwell-pope" then "kentavious caldwell pope"
  when LOWER(player_fullname) = "robert covington" then "robert covington"
  when LOWER(player_fullname) = "shai gilgeous-alexander" then "shai gilgeous-alexander" else

  LOWER(REGEXP_REPLACE(player_fullname,r'(Jr.)|(Sr.)|(\.)|(III)|(II)|(ii)|(iii)(\')|(\-)','')) end as player_fullname_headshot,
  player_position,
  player_birth_date,
  player_nba_start_year,
  player_height_feet,
  player_height_inches,
  player_height_centimeters,
  player_weight_pounds,
  player_weight_kilograms
FROM
  `wagon-bootcamp-57100.nba_stats.player`),
  t2 as (
  SELECT
  LOWER(REGEXP_REPLACE(player_fullname_headshot,r'(jr)|(sr)|(\.)|(iii)|(ii)|(\')|(\-)','')) player_fullname_headshot,
  url from
  (Select url,
  case when LOWER(player_fullname_headshot) = "luka doncic" then "luka dončić"
  when LOWER(player_fullname_headshot) = "nikola jokic" then "nikola jokić"
  when LOWER(player_fullname_headshot) = "nikola vucevic" then "nikola jović"
  when LOWER(player_fullname_headshot) = "kristaps porzingis" then "kristaps porziņģis"
  when LOWER(player_fullname_headshot) = "boban marjanovic" then "boban marjanović"
  when LOWER(player_fullname_headshot) = "theo maledon" then "théo maledon"
  when LOWER(player_fullname_headshot) = "jonas valanciunas" then "jonas valančiūnas"
  when LOWER(player_fullname_headshot) = "luka samanic" then "luka šamanić"
  when LOWER(player_fullname_headshot) = "vlatko cancar" then "vlatko čančar"
  when LOWER(player_fullname_headshot) = "dario saric" then "dario šarić"
  when LOWER(player_fullname_headshot) = "kentavious caldwell-pope" then "kentavious caldwell pope"
  when LOWER(player_fullname_headshot) = "shai gilgeous-alexander" then "shai gilgeous alexander"
  when LOWER(player_fullname_headshot) = "jusuf nurkic" then "jusuf nurkić"
  else LOWER(player_fullname_headshot) end as player_fullname_headshot
FROM
  `wagon-bootcamp-57100.nba_stats.headshots`)
  ),
  t3 as (
    select player_fullname, player_team from `wagon-bootcamp-57100.nba_stats.player_log_temp` group by 1,2
  ),
  t4 as (
    select team_code,
        team_city,
        team_nickname,
        team_full_name,
        url_team_logo
        from `wagon-bootcamp-57100.nba_stats.team`
          )

Select distinct * except(url), IFNULL(url,"https://avatar-management--avatars.us-west-2.prod.public.atl-paas.net/default-avatar.png") as url  from t1 left join t2 using(player_fullname_headshot)
left join t3 using(player_fullname)
left join t4 on t3.player_team = t4.team_code
