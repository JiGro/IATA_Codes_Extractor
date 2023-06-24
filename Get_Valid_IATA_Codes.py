import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
from tqdm import tqdm
import requests
from multiprocessing import Pool
import string

def extract_iata_codes_wikipedia():
    iata_lst, airport_name_lst, airport_city_lst = ([] for i in range(3))
    for letter in string.ascii_uppercase:
        url = f"https://en.wikipedia.org/wiki/List_of_airports_by_IATA_airport_code:_{letter}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        sub_soup = soup.find("tbody").find_all("tr")
        for result in sub_soup:
            try:
                iata_code = result.find_all("td")[0].text[:3]
                airport_name = result.find_all("td")[2].text
                airport_city = result.find_all("td")[3].text
                iata_lst.append(iata_code)
                airport_name_lst.append(airport_name)
                airport_city_lst.append(airport_city)
                print(f"{airport_name} in {airport_city} with IATA Code {iata_code}")
            except:
                pass
    airport_df = pd.DataFrame({"iata_code":iata_lst,
                               "airport_name":airport_name_lst,
                               "aiport_city":airport_city_lst})
    return airport_df


def extract_iata_codes_alternative():
    iata_lst, airport_country_lst, airport_city_lst = ([] for i in range(3))
    driver = browser_startup_sequence()
    INPUT_URL = "https://www.ccra.com/airport-codes/"
    driver.get(INPUT_URL)
    driver.find_element(By.XPATH, "//a[@data-cli_action='accept']").click()
    for i in range(32):
        try:
            time.sleep(2)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            table_results = soup.find("tbody").find_all("tr")
            for result in table_results:
                city = result.find_all("td")[0].text
                country = result.find_all("td")[1].text
                iata_code = result.find_all("td")[2].text
                # append lists
                iata_lst.append(iata_code)
                airport_city_lst.append(city)
                airport_country_lst.append(country)
            driver.find_element(By.XPATH, "//a[@data-dt-idx='next']").click()
        except:
            pass
    airport_df = pd.DataFrame({"iata_code":iata_lst,
                               "city":airport_city_lst,
                               "country":airport_country_lst})
    return airport_df

def create_input_url(iata, input_timeframe, anytime:bool):
    if anytime == True:
        input_url = f"https://www.kiwi.com/us/search/tiles/{iata}/anywhere/anytime/no-return?sortAggregateBy=price"
    else:
        input_url = f"https://www.kiwi.com/us/search/tiles/{iata}/anywhere/{input_timeframe}/no-return?sortAggregateBy=price"
    return input_url


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


def kiwi_cookie_consent(driver):
    try:
        driver.find_element(By.XPATH, "//button[@id='cookies_accept']").click()
        time.sleep(random.randint(5, 8))
        print(" ** Cookies successfully accepted")
    except:
        print(" ** No Cookie Consent")

def iata_parser_kiwi(dataframe):
    INPUT_TIMEFRAME = "2023-03-01_2023-03-31"
    driver = browser_startup_sequence()
    for index,row in tqdm(dataframe.iterrows()):
        iata_code = row["iata_code"]
        INPUT_URL = create_input_url(iata_code,INPUT_TIMEFRAME, False)
        print(f"### IATA Check for {iata_code} ###")
        driver.get(INPUT_URL)
        time.sleep(2)
        kiwi_cookie_consent(driver)
        current_url = driver.current_url
        if current_url != "https://www.kiwi.com/us/":
            print(" ** IATA Code usable")
        else:
            print(" ** IATA Code not on KIWI")
            dataframe = dataframe.drop(index)
    driver.quit()
    return dataframe

def execute_multiprocessing(python_function, list_to_iterate):
    pool = Pool(processes=6)
    results = pool.map(python_function, list_to_iterate)
    pool.close()
    pool.join()
    return results

def scroll_to_bottom(webdriver):
    try:
        webdriver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except:
        pass

def split_dataframe(df, n):
    # Calculate the number of rows in each dataframe
    rows_per_df = df.shape[0] // n
    # Initialize an empty list to store the dataframes
    df_list = []
    # Iterate over the number of dataframes
    for i in range(n):
        # Calculate the start and end indices of the current dataframe
        start_index = i * rows_per_df
        end_index = start_index + rows_per_df
        # Slice the dataframe and append it to the list
        df_list.append(df.iloc[start_index:end_index])
    # Return the list of dataframes
    return df_list

if __name__ == "__main__":
    iata_df = extract_iata_codes_alternative()
    df_list = split_dataframe(iata_df, 6)
    results = execute_multiprocessing(iata_parser_kiwi, df_list)
    iata_result_df = pd.concat(results)
    iata_result_df.to_csv(f"kiwi_iata_codes_alternative.csv")
    print("### Finished Scraping ###")
