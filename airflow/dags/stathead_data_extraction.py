from datetime import datetime, timedelta
import time
import numpy as np
import os
import pandas as pd
import re

from airflow.decorators import dag, task

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

from google.cloud import storage
from google.cloud import bigquery


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
YESTERDAY_FULL = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
YESTERDAY_MONTH =  int((datetime.now() - timedelta(days=1)).strftime('%m'))
YESTERDAY_DAY =  int((datetime.now() - timedelta(days=1)).strftime('%d'))


@dag(
    description='Stathead extracting DAG',
    schedule_interval="0 15 * * *",  # Everyday at 15:00
    start_date=datetime(2023, 11, 7),
    catchup=False
)
def stathead_extraction():
    @task()
    def scraper():
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-dev-shm-usage")
        download = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())
        driver = webdriver.Chrome(service=download, options=chrome_options)

        # Navigate to the login page
        driver.get("https://stathead.com/users/login.cgi?redirect_uri=https%3A//stathead.com/basketball/")


        # Wait for the page to load
        time.sleep(10)


        # Your login credentials - replace 'your_username' and 'your_password' with your actual credentials
        username = 'hsravo1@gmail.com'
        password = '7Secondsorless*/!'


        # Find the username field and send the username
        username_input = driver.find_element(By.NAME, 'username')
        username_input.send_keys(username)

        # Find the password field and send the password
        password_input = driver.find_element(By.NAME, 'password')
        password_input.send_keys(password)

        # Send the enter key to log in
        password_input.send_keys(Keys.RETURN)

        print("Logged in")
        # Wait for the next page to load or for a confirmation of login
        time.sleep(7)

        #############################################################
        # Navigate to the TEAM GAME POINTS finder page
        # driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}")
        driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&year_min=2024&year_max=2024&game_month=11&game_day=17")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(30)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        try:
            game_points_content = driver.find_element(By.XPATH, stats_table_xpath).text
        except NoSuchElementException:
            game_points_content = None


        # #############################################################
        # Navigate to the TEAM GAME REBOUNDS finder page
        # driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&order_by=trb&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}")
        driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&order_by=trb&year_min=2024&year_max=2024&game_month=11&game_day=17")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(30)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        try:
            game_rebounds_content = driver.find_element(By.XPATH, stats_table_xpath).text
        except NoSuchElementException:
            game_rebounds_content = None

        # #############################################################
        # Navigate to the TEAM GAME ASSISTS finder page
        # driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&order_by=ast&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}")
        driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&order_by=ast&year_min=2024&year_max=2024&game_month=11&game_day=17")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(30)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        try:
            game_assists_content = driver.find_element(By.XPATH, stats_table_xpath).text
        except NoSuchElementException:
            game_assists_content = None

        # #############################################################
        # Navigate to the PLAYER GAME LOG FIRST 200 RESULTS finder page
        # driver.get(f"https://stathead.com/basketball/player-game-finder.cgi?request=1&order_by=date&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}")
        driver.get(f"https://stathead.com/basketball/player-game-finder.cgi?request=1&order_by=date&year_min=2024&year_max=2024&game_month=11&game_day=17")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(30)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        try:
            players_content_200 = driver.find_element(By.XPATH, stats_table_xpath).text
        except NoSuchElementException:
            players_content_200 = None

        # #############################################################
        # Navigate to the PLAYER GAME LOG NEXT 200 RESULTS finder page
        # driver.get(f"https://stathead.com/basketball/player-game-finder.cgi?request=1&order_by=date&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}&offset=200")
        driver.get(f"https://stathead.com/basketball/player-game-finder.cgi?request=1&order_by=date&year_min=2024&year_max=2024&game_month=11&game_day=17&offset=200")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(30)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        try:
            players_content_400 = driver.find_element(By.XPATH, stats_table_xpath).text
        except NoSuchElementException:
            players_content_400 = None

        # Don't forget to close the browser
        driver.quit()
        return game_points_content, game_rebounds_content, game_assists_content, players_content_200, players_content_400

    @task()
    def extract_points_content(result):
        game_points_content = result[0]
        print("Game points content")
        print(game_points_content)
        result = [line for line in game_points_content.split('\n') if line != '']

        headers = result[1].split(' ')
        games = result[2:]

        games = [game.split() for game in games]
        for game in games:
            if "@" in game:
                game.remove("@")
            if "(OT)" in game:
                game.remove("(OT)")
            game[5] += " " + game.pop(6)

        return games, headers

    @task()
    def extract_games_rebounds_content(result):
        game_rebounds_content = result[1]
        print("Game rebound content")
        print(game_rebounds_content)
        result = [line for line in game_rebounds_content.split('\n') if line != '']

        headers = result[1].split(' ')
        games = result[2:]

        games = [game.split() for game in games]
        for game in games:
            if "@" in game:
                game.remove("@")
            if "(OT)" in game:
                game.remove("(OT)")
            game[5] += " " + game.pop(6)

        return games, headers

    @task()
    def extract_games_assists_content(result):
        game_assists_content = result[2]
        print("Game assists content")
        print(game_assists_content)
        result = [line for line in game_assists_content.split('\n') if line != '']

        headers = result[1].split(' ')
        games = result[2:]

        games = [game.split() for game in games]
        for game in games:
            if "@" in game:
                game.remove("@")
            if "(OT)" in game:
                game.remove("(OT)")
            game[5] += " " + game.pop(6)

        return games, headers

    def process_players(data):
        pattern = re.compile(r'^(\d+)\s+([^\d\n]+)\s+(\d{4}-\d{2}-\d{2})')

        def process_line(line):
            match = pattern.match(line)
            if match:
                rk, player, date = match.groups()
                rest_of_line = line[match.end():].strip()
                remaining_columns = re.split(r'\s+', rest_of_line)
                for item in ["(OT)", "@", "*"]:
                    if item in remaining_columns:
                        remaining_columns.remove(item)
                if not remaining_columns[8].startswith('.') and not remaining_columns[8].startswith('1.'):
                    remaining_columns.insert(8, None)
                if not remaining_columns[11].startswith('.') and not remaining_columns[11].startswith('1.'):
                    remaining_columns.insert(11, None)
                if not remaining_columns[14].startswith('.') and not remaining_columns[14].startswith('1.'):
                    remaining_columns.insert(14, None)
                if not remaining_columns[17].startswith('.') and not remaining_columns[17].startswith('1.'):
                    remaining_columns.insert(17, None)
                if len(remaining_columns) < 32:
                    remaining_columns.insert(15, None)
                return [rk, player, date] + remaining_columns
            else:
                return None

        data = data.split('\n')
        headers = [
            "Rk",
            "Player",
            "Date",
            "Age",
            "Team",
            "Opp",
            "Result",
            "GS",
            "MP",
            "FG",
            "FGA",
            "FG%",
            "2P",
            "2PA",
            "2P%",
            "3P",
            "3PA",
            "3P%",
            "FT",
            "FTA",
            "FT%",
            "TS%",
            "ORB",
            "DRB",
            "TRB",
            "AST",
            "STL",
            "BLK",
            "TOV",
            "PF",
            "PTS",
            "GmSc",
            "BPM",
            "+/-",
            "Pos."
        ]

        processed_data = [process_line(line) for line in data]
        processed_data = [line for line in processed_data if line is not None]

        processed = pd.DataFrame(processed_data, columns=headers)
        processed["Result"] = processed["Result"] + " " + processed["GS"]
        processed.drop(columns=["GS"], inplace=True)
        processed.columns = [
            'Rk',
            'player_fullname',
            'game_date',
            'Age',
            'player_team',
            'opponent_team',
            'game_result',
            'player_minutes',
            'player_field_goals',
            'player_field_goals_attempted',
            'player_field_goals_pctg',
            'player_2pts',
            'player_2pts_attempted',
            'player_2pts_pctg',
            'player_3pts',
            'player_3pts_attempted',
            'player_3pts_pctg',
            'player_fts',
            'player_fts_attempted',
            'player_fts_pctg',
            'player_true_shooting_pctg',
            'player_off_rebounds',
            'player_def_rebounds',
            'player_total_rebounds',
            'player_assists',
            'player_steals',
            'player_blocks',
            'player_turnovers',
            'player_personal_fouls',
            'player_points',
            'GmSc',
            'BPM',
            'player_plus_minus',
            'Pos.'
        ]
        processed["player_plus_minus"] = pd.to_numeric(processed["player_plus_minus"], errors='coerce')
        processed["created_at"] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        processed.fillna(
            {
                "player_field_goals": 0,
                "player_field_goals_pctg": .000,
                "player_2pts": 0,
                "player_2pts_pctg": .000,
                "player_3pts": 0,
                "player_3pts_pctg": .000,
                "player_fts": 0,
                "player_fts_pctg": .000,
                "player_true_shooting_pctg": .000,
                "player_plus_minus": 0.0
            }, inplace=True
        )
        processed.drop(columns=["Rk", "Age", "GmSc", "BPM", "Pos."], axis=1, inplace=True)

        return processed

    @task()
    def extract_games_players_content(result):
        print(result[3])
        print(result[4])

        if not result[4]:
            processed_players = process_players(result[3])
        else:
            processed_players = pd.concat([process_players(result[3]), process_players(result[4])]).reset_index(drop=True)

        # processed_players.to_csv(f'{AIRFLOW_HOME}/data/player_log_{YESTERDAY_FULL}.csv', index=False)
        processed_players.to_csv(f'{AIRFLOW_HOME}/data/player_log_2023-11-17.csv', index=False)

    @task()
    def combine_games_content(extracted_game_points_content, extract_games_rebounds_content, extracted_game_assists_content):
        points_df = pd.DataFrame(extracted_game_points_content[0], columns=extracted_game_points_content[1])
        rebounds_df = pd.DataFrame(extract_games_rebounds_content[0], columns=extract_games_rebounds_content[1])
        assists_df = pd.DataFrame(extracted_game_assists_content[0], columns=extracted_game_assists_content[1])

        points_df.columns = [
            "Rk",
            "team",
            "game_date",
            "PTS",
            "opponent",
            "team_result",
            "MP",
            "team_field_goals",
            "team_field_goals_attempted",
            "team_field_goals_pctg",
            "team_2pts",
            "team_2pts_attempted",
            "team_2pts_pctg",
            "team_3pts",
            "team_3pts_attempted",
            "team_3pts_pctg",
            "team_fts",
            "team_fts_attempted",
            "team_fts_pctg",
            "team_points",
            "opp_field_goals",
            "opp_field_goals_attempted",
            "opp_field_goals_pctg",
            "opp_2pts",
            "opp_2pts_attempted",
            "opp_2pts_pctg",
            "opp_3pts",
            "opp_3pts_attempted",
            "opp_3pts_pctg",
            "opp_fts",
            "opp_fts_attempted",
            "opp_fts_pctg",
            "opp_points"
        ]
        points_df.drop(["Rk", "PTS", "MP"], axis=1, inplace=True)

        rebounds_df.columns = [
            "Rk",
            "team",
            "game_date",
            "TRB",
            "opponent",
            "team_result",
            "MP",
            "team_off_rebounds",
            "team_def_rebounds",
            "team_total_rebounds",
            "opp_off_rebounds",
            "opp_def_rebounds",
            "opp_total_rebounds"
        ]
        rebounds_df.drop(["Rk", "TRB", "MP"], axis=1, inplace=True)

        assists_df.columns = [
            "Rk",
            "team",
            "game_date",
            "AST",
            "opponent",
            "team_result",
            "MP",
            "team_assists",
            "team_steals",
            "team_blocks",
            "team_turnovers",
            "team_personal_fouls",
            "opp_assists",
            "opp_steals",
            "opp_blocks",
            "opp_turnovers",
            "opp_personal_fouls"
        ]
        assists_df.drop(["Rk", "AST", "MP"], axis=1, inplace=True)

        points_rbds_df = pd.merge(points_df, rebounds_df, on=['team','game_date', 'opponent', 'team_result'])
        final_df = pd.merge(points_rbds_df, assists_df, on=['team','game_date', 'opponent', 'team_result'])
        final_df["created_at"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        # final_df.to_csv(f'{AIRFLOW_HOME}/data/game_log_{YESTERDAY_FULL}.csv', index=False)
        final_df.to_csv(f'{AIRFLOW_HOME}/data/game_log_2023-11-17.csv', index=False)



    @task()
    def upload_games_to_gcs():
        # object_name = f"game_log/game_log_{YESTERDAY_FULL}.csv"
        object_name = f"game_log/game_log_2023-11-17.csv"
        # local_file = f'{AIRFLOW_HOME}/data/game_log_{YESTERDAY_FULL}.csv'
        local_file = f'{AIRFLOW_HOME}/data/game_log_2023-11-17.csv'
        bucket_name = "nba_stats_57100"
        storage_client = storage.Client.from_service_account_json('/app/airflow/.gcp_keys/le-wagon-de-bootcamp.json')
        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)

    @task()
    def gcs_games_to_bigquery():
        # object_name = f"game_log/game_log_{YESTERDAY_FULL}.csv"
        object_name = f"game_log/game_log_2023-11-17.csv"
        # Construct a BigQuery client object.
        client = bigquery.Client.from_service_account_json('/app/airflow/.gcp_keys/le-wagon-de-bootcamp.json')

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = "nba_stats.game_log_temp"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("team", "STRING"),
                bigquery.SchemaField("game_date", "DATE"),
                bigquery.SchemaField("opponent", "STRING"),
                bigquery.SchemaField("team_result", "STRING"),
                bigquery.SchemaField("team_field_goals", "INTEGER"),
                bigquery.SchemaField("team_field_goals_attempted", "INTEGER"),
                bigquery.SchemaField("team_field_goals_pctg", "FLOAT"),
                bigquery.SchemaField("team_2pts", "INTEGER"),
                bigquery.SchemaField("team_2pts_attempted", "INTEGER"),
                bigquery.SchemaField("team_2pts_pctg", "FLOAT"),
                bigquery.SchemaField("team_3pts", "INTEGER"),
                bigquery.SchemaField("team_3pts_attempted", "INTEGER"),
                bigquery.SchemaField("team_3pts_pctg", "FLOAT"),
                bigquery.SchemaField("team_fts", "INTEGER"),
                bigquery.SchemaField("team_fts_attempted", "INTEGER"),
                bigquery.SchemaField("team_fts_pctg", "FLOAT"),
                bigquery.SchemaField("team_points", "INTEGER"),
                bigquery.SchemaField("opp_field_goals", "INTEGER"),
                bigquery.SchemaField("opp_field_goals_attempted", "INTEGER"),
                bigquery.SchemaField("opp_field_goals_pctg", "FLOAT"),
                bigquery.SchemaField("opp_2pts", "INTEGER"),
                bigquery.SchemaField("opp_2pts_attempted", "INTEGER"),
                bigquery.SchemaField("opp_2pts_pctg", "FLOAT"),
                bigquery.SchemaField("opp_3pts", "INTEGER"),
                bigquery.SchemaField("opp_3pts_attempted", "INTEGER"),
                bigquery.SchemaField("opp_3pts_pctg", "FLOAT"),
                bigquery.SchemaField("opp_fts", "INTEGER"),
                bigquery.SchemaField("opp_fts_attempted", "INTEGER"),
                bigquery.SchemaField("opp_fts_pctg", "FLOAT"),
                bigquery.SchemaField("opp_points", "INTEGER"),
                bigquery.SchemaField("team_off_rebounds", "INTEGER"),
                bigquery.SchemaField("team_def_rebounds", "INTEGER"),
                bigquery.SchemaField("team_total_rebounds", "INTEGER"),
                bigquery.SchemaField("opp_off_rebounds", "INTEGER"),
                bigquery.SchemaField("opp_def_rebounds", "INTEGER"),
                bigquery.SchemaField("opp_total_rebounds", "INTEGER"),
                bigquery.SchemaField("team_assists", "INTEGER"),
                bigquery.SchemaField("team_steals", "INTEGER"),
                bigquery.SchemaField("team_blocks", "INTEGER"),
                bigquery.SchemaField("team_turnovers", "INTEGER"),
                bigquery.SchemaField("team_personal_fouls", "INTEGER"),
                bigquery.SchemaField("opp_assists", "INTEGER"),
                bigquery.SchemaField("opp_steals", "INTEGER"),
                bigquery.SchemaField("opp_blocks", "INTEGER"),
                bigquery.SchemaField("opp_turnovers", "INTEGER"),
                bigquery.SchemaField("opp_personal_fouls", "INTEGER"),
                bigquery.SchemaField("created_at", "TIMESTAMP")
            ],
            skip_leading_rows=1,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
        )
        uri = f"gs://nba_stats_57100/{object_name}"

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))

    @task()
    def upload_player_logs_to_gcs():
        # object_name = f"player_log/player_log_{YESTERDAY_FULL}.csv"
        object_name = f"player_log/player_log_2023-11-17.csv"
        # local_file = f'{AIRFLOW_HOME}/data/player_log_{YESTERDAY_FULL}.csv'
        local_file = f'{AIRFLOW_HOME}/data/player_log_2023-11-17.csv'
        bucket_name = "nba_stats_57100"
        storage_client = storage.Client.from_service_account_json('/app/airflow/.gcp_keys/le-wagon-de-bootcamp.json')
        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)

    @task()
    def gcs_player_logs_to_bigquery():
        # object_name = f"player_log/player_log_{YESTERDAY_FULL}.csv"
        object_name = f"player_log/player_log_2023-11-17.csv"
        # Construct a BigQuery client object.
        client = bigquery.Client.from_service_account_json('/app/airflow/.gcp_keys/le-wagon-de-bootcamp.json')

        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = "nba_stats.player_log_temp"

        job_config = bigquery.LoadJobConfig(
            schema=[
                    bigquery.SchemaField("player_fullname", "STRING"),
                    bigquery.SchemaField("game_date", "DATE"),
                    bigquery.SchemaField("player_team", "STRING"),
                    bigquery.SchemaField("opponent_team", "STRING"),
                    bigquery.SchemaField("game_result", "STRING"),
                    bigquery.SchemaField("player_minutes", "INTEGER"),
                    bigquery.SchemaField("player_field_goals", "INTEGER"),
                    bigquery.SchemaField("player_field_goals_attempted", "INTEGER"),
                    bigquery.SchemaField("player_field_goals_pctg", "FLOAT"),
                    bigquery.SchemaField("player_2pts", "INTEGER"),
                    bigquery.SchemaField("player_2pts_attempted", "INTEGER"),
                    bigquery.SchemaField("player_2pts_pctg", "FLOAT"),
                    bigquery.SchemaField("player_3pts", "INTEGER"),
                    bigquery.SchemaField("player_3pts_attempted", "INTEGER"),
                    bigquery.SchemaField("player_3pts_pctg", "FLOAT"),
                    bigquery.SchemaField("player_fts", "INTEGER"),
                    bigquery.SchemaField("player_fts_attempted", "INTEGER"),
                    bigquery.SchemaField("player_fts_pctg", "FLOAT"),
                    bigquery.SchemaField("player_true_shooting_pctg", "FLOAT"),
                    bigquery.SchemaField("player_off_rebounds", "INTEGER"),
                    bigquery.SchemaField("player_def_rebounds", "INTEGER"),
                    bigquery.SchemaField("player_total_rebounds", "INTEGER"),
                    bigquery.SchemaField("player_assists", "INTEGER"),
                    bigquery.SchemaField("player_steals", "INTEGER"),
                    bigquery.SchemaField("player_blocks", "INTEGER"),
                    bigquery.SchemaField("player_turnovers", "INTEGER"),
                    bigquery.SchemaField("player_personal_fouls", "INTEGER"),
                    bigquery.SchemaField("player_points", "INTEGER"),
                    bigquery.SchemaField("player_plus_minus", "FLOAT"),
                    bigquery.SchemaField("created_at", "TIMESTAMP")
            ],
            skip_leading_rows=1,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
        )
        uri = f"gs://nba_stats_57100/{object_name}"

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))


    result = scraper()
    extracted_game_points_content = extract_points_content(result)
    extract_games_rebounds_content = extract_games_rebounds_content(result)
    extracted_game_assists_content = extract_games_assists_content(result)
    extract_games_players_content = extract_games_players_content(result)
    combine_games_content = combine_games_content(extracted_game_points_content, extract_games_rebounds_content, extracted_game_assists_content)
    upload_games_to_gcs = upload_games_to_gcs()
    gcs_games_to_bigquery = gcs_games_to_bigquery()
    upload_player_logs_to_gcs = upload_player_logs_to_gcs()
    gcs_player_logs_to_bigquery = gcs_player_logs_to_bigquery()

    combine_games_content >> upload_games_to_gcs  >> gcs_games_to_bigquery
    extract_games_players_content >> upload_player_logs_to_gcs >> gcs_player_logs_to_bigquery

extraction = stathead_extraction()
