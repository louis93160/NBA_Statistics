from datetime import datetime
import time

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC





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
        driver.get("https://stathead.com/basketball/team-game-finder.cgi?request=1&year_min=2024&year_max=2024&game_month=11&game_day=5")

        # Wait for the next page to load or for a confirmation of login
        time.sleep(5)
        # print("Navigated to team game finder page")
        # print(driver.page_source)

        stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        # print(stats_table_xpath)
        # print("Stats table")
        game_points_content = driver.find_element(By.XPATH, stats_table_xpath).text


        # #############################################################
        # # Navigate to the TEAM GAME REBOUNDS finder page
        # driver.get("https://stathead.com/basketball/team-game-finder.cgi?request=1&order_by=trb&year_min=2024&year_max=2024&game_month=11&game_day=5")

        # # Wait for the next page to load or for a confirmation of login
        # time.sleep(5)
        # # print("Navigated to team game finder page")
        # # print(driver.page_source)

        # stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        # # print(stats_table_xpath)
        # # print("Stats table")
        # print(driver.find_element(By.XPATH, stats_table_xpath).text)

        # #############################################################
        # # Navigate to the TEAM GAME ASSISTS finder page
        # driver.get("https://stathead.com/basketball/team-game-finder.cgi?request=1&order_by=ast&year_min=2024&year_max=2024&game_month=11&game_day=5")

        # # Wait for the next page to load or for a confirmation of login
        # time.sleep(5)
        # # print("Navigated to team game finder page")
        # # print(driver.page_source)

        # stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        # # print(stats_table_xpath)
        # # print("Stats table")
        # print(driver.find_element(By.XPATH, stats_table_xpath).text)

        # #############################################################
        # # Navigate to the PLAYER GAME LOG FIRST 200 RESULTS finder page
        # driver.get("https://stathead.com/basketball/player-game-finder.cgi?request=1&order_by=date&year_min=2024&year_max=2024&game_month=10&game_day=25")

        # # Wait for the next page to load or for a confirmation of login
        # time.sleep(5)
        # # print("Navigated to team game finder page")
        # # print(driver.page_source)

        # stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        # # print(stats_table_xpath)
        # # print("Stats table")
        # print(driver.find_element(By.XPATH, stats_table_xpath).text)

        # #############################################################
        # # Navigate to the PLAYER GAME LOG NEXT 200 RESULTS finder page
        # driver.get("https://stathead.com/basketball/player-game-finder.cgi?request=1&order_by=date&year_min=2024&year_max=2024&game_month=10&game_day=25&offset=200")

        # # Wait for the next page to load or for a confirmation of login
        # time.sleep(5)
        # # print("Navigated to team game finder page")
        # # print(driver.page_source)

        # stats_table_xpath = "//table[contains(@class, 'stats_table')]"
        # # print(stats_table_xpath)
        # # print("Stats table")
        # print(driver.find_element(By.XPATH, stats_table_xpath).text)

        # Don't forget to close the browser
        driver.quit()
        return game_points_content

    @task()
    def print_content(game_points_content):
        print(game_points_content)

    result = scraper()
    print_content(result)


extraction = stathead_extraction()
