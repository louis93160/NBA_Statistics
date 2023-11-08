from datetime import datetime, timedelta
import time
import os
import pandas as pd

from airflow.decorators import dag, task

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC

from google.cloud import storage
from google.cloud import bigquery


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
YESTERDAY_FULL = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
YESTERDAY_MONTH =  int((datetime.now() - timedelta(days=1)).strftime('%m'))
YESTERDAY_DAY =  int((datetime.now() - timedelta(days=1)).strftime('%d'))


@dag(
    description='Stathead extracting DAG',
    schedule_interval=None,  # Manually triggered
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
        username = 'mariusfall0@gmail.com'
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
        driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(5)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        game_points_content = driver.find_element(By.XPATH, stats_table_xpath).text


        # #############################################################
        # Navigate to the TEAM GAME REBOUNDS finder page
        driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&order_by=trb&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(5)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        game_rebounds_content = driver.find_element(By.XPATH, stats_table_xpath).text

        # #############################################################
        # Navigate to the TEAM GAME ASSISTS finder page
        driver.get(f"https://stathead.com/basketball/team-game-finder.cgi?request=1&order_by=ast&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(5)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        game_assists_content = driver.find_element(By.XPATH, stats_table_xpath).text

        # #############################################################
        # Navigate to the PLAYER GAME LOG FIRST 200 RESULTS finder page
        driver.get(f"https://stathead.com/basketball/player-game-finder.cgi?request=1&order_by=date&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(5)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        players_content_200 = driver.find_element(By.XPATH, stats_table_xpath).text

        # #############################################################
        # Navigate to the PLAYER GAME LOG NEXT 200 RESULTS finder page
        driver.get(f"https://stathead.com/basketball/player-game-finder.cgi?request=1&order_by=date&year_min=2024&year_max=2024&game_month={YESTERDAY_MONTH}&game_day={YESTERDAY_DAY}&offset=200")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(5)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        players_content_400 = driver.find_element(By.XPATH, stats_table_xpath).text

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

    @task()
    def extract_games_players_content(result):
        print(result[3])
        print(result[4])

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

        final_df.to_csv(f'{AIRFLOW_HOME}/data/game_log_{YESTERDAY_FULL}.csv', index=False)

    @task()
    def upload_to_gcs():
        object_name = f"game_log/game_log_{YESTERDAY_FULL}.csv"
        local_file = f'{AIRFLOW_HOME}/data/game_log_{YESTERDAY_FULL}.csv'
        bucket_name = "nba_stats_57100"
        storage_client = storage.Client.from_service_account_json('/app/airflow/.gcp_keys/le-wagon-de-bootcamp.json')
        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)

    @task()
    def gcs_to_bigquery():
        object_name = f"game_log/game_log_{YESTERDAY_FULL}.csv"
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


    result = scraper()
    extracted_game_points_content = extract_points_content(result)
    extract_games_rebounds_content = extract_games_rebounds_content(result)
    extracted_game_assists_content = extract_games_assists_content(result)
    extract_games_players_content = extract_games_players_content(result)
    combine_games_content = combine_games_content(extracted_game_points_content, extract_games_rebounds_content, extracted_game_assists_content)
    upload_to_gcs = upload_to_gcs()
    gcs_to_bigquery = gcs_to_bigquery()

    combine_games_content >> upload_to_gcs  >> gcs_to_bigquery


extraction = stathead_extraction()
