from contextlib import contextmanager
from datetime import datetime, time
from time import sleep
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
from pyvirtualdisplay import Display


@contextmanager
def get_selenium_driver():
    # Setting up the Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # Initialize the WebDriver with the Chrome options
    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://stathead.com/users/login.cgi?redirect_uri=https%3A//stathead.com/basketball/")
    sleep(5)

    # Find the username field and send the username
    username = Variable.get("username")
    username_input = driver.find_element(By.NAME, 'username')
    username_input.send_keys(username)

    password = Variable.get("password")
    # Find the password field and send the password
    password_input = driver.find_element(By.NAME, 'password')
    password_input.send_keys(password)

    # Send the enter key to log in
    password_input.send_keys(Keys.RETURN)
    sleep(5)
    try:
        yield driver

    finally:
        driver.quit()

@dag(
    description='A simple DAG to test Selenium',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2023, 11, 7),
    catchup=False
)

def scraping():

    @task()
    def get_game_logs_points():
        with get_selenium_driver() as driver:
            print("++++LOGGED IN++++")
            # Navigate to the team game finder page
            driver.get("https://stathead.com/basketball/team-game-finder.cgi?request=1&year_min=2024&year_max=2024&game_month=11&game_day=5")
            # Wait for the next page to load or for a confirmation of login
            sleep(10)

            # Wait for the 'Export Data' span to be present and hover over it
            export_data_span_xpath = "//li[contains(@class, 'hasmore')]/span[text()='Export Data']"
            export_data_span = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, export_data_span_xpath))
            )
            ActionChains(driver).move_to_element(export_data_span).perform()

            # Wait for the 'Get table as CSV (for Excel)' button to be clickable after the dropdown appears
            csv_button_xpath = "//button[contains(@class, 'tooltip') and contains(@tip, 'Export table as')]"
            csv_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, csv_button_xpath))
            )

            # Click the 'Get table as CSV (for Excel)' button
            csv_button.click()

            # Wait for the CSV data to be displayed in the <pre> tag
            csv_data_pre = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'csv_stats'))
            )

            # Wait for the next page to load or for a confirmation of login
            sleep(2)

            # Get the CSV data from the <pre> tag
            csv_data = csv_data_pre.text

            # Print the CSV data or process it as needed
            print(csv_data)


    # def get_game_logs_rebounds():

    # def get_game_logs_assists():

    get_game_logs_points()


scraping = scraping()
