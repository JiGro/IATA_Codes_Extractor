import pandas as pd
import numpy as np
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
import random
from multiprocessing import Pool


def browser_startup_sequence():
    # start browser
    base_url = "https://www.google.com/maps/"
    path = r'Google_Maps_Scraper/chromedriver'
    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
    options.add_argument("--lang=en_US")
    driver = webdriver.Chrome(path, chrome_options=options)
    driver.maximize_window()
    return driver


def scroll_to_bottom(webdriver):
    try:
        webdriver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except:
        pass


def kiwi_cookie_consent(driver):
    try:
        driver.find_element(By.XPATH, "//button[@id='cookies_accept']").click()
        time.sleep(random.randint(5,8))
        print(" ** Cookies successfully accepted")
    except:
        print(" ** No Cookie Consent")


def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""


def get_kiwi_iata_url_identifier(dataframe):
    driver = browser_startup_sequence()
    for index, row in tqdm(dataframe.iterrows()):
        iata_code = row["iata_code"]
        input_url = f"https://www.kiwi.com/us/search/tiles/{iata_code}/anywhere/2023-01-01_2023-12-31/no-return?sortBy=price&sortAggregateBy=price"
        driver.get(input_url)
        time.sleep(random.randint(7, 10))
        kiwi_cookie_consent(driver)
        scroll_to_bottom(driver)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        sub_result = soup.find_all("a", {"data-test": "PictureCard"})
        if len(sub_result) > 0:
            first_hit = sub_result[0]["href"]
            kiwi_iata_url_identifier = find_between(first_hit, "/us/search/results/", "/")
        else:
            kiwi_iata_url_identifier = np.nan
        dataframe.loc[index, "kiwi_iata_url_identifier"] = kiwi_iata_url_identifier
        print(f"*** {iata_code} has the following identifier: {kiwi_iata_url_identifier}***")
    return dataframe


def execute_multiprocessing(python_function, list_to_iterate):
    pool = Pool(processes=4)
    results = pool.map(python_function, list_to_iterate)
    pool.close()
    pool.join()
    return results


if __name__ == "__main__":
    iata_result_df = pd.read_csv(f"kiwi_iata_codes.csv", index_col=0)
    iata_result_df["kiwi_iata_url_identifier"] = np.nan
    df_list = [iata_result_df[0:int((len(iata_result_df))/4)],
               iata_result_df[int((len(iata_result_df))/4):int((len(iata_result_df))/4)*2],
               iata_result_df[int((len(iata_result_df))/4)*2:int((len(iata_result_df))/4)*3],
               iata_result_df[int((len(iata_result_df))/4)*3:int((len(iata_result_df))/4)*4]]
    results = execute_multiprocessing(get_kiwi_iata_url_identifier, df_list)
    iata_result_df = pd.concat(results)
    iata_result_df.to_csv(f"kiwi_iata_codes_detailed.csv")
    print("### Finished Scraping ###")
