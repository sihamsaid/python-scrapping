#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
As part of my training at the IA School. I have to provide a tool that allows
the web to scrap the Open Food Facts web site available at https://fr.openfoodfacts.org

This script will:

- Use Webdriver /beautifulsoup to scrap the needed information.
- Start many workers (processes) that work simultanously. To do so, I use the multiprocess package. I split the number of pages by the number of workers.
- Each worker will write into a json file once a new product is retrieved. Once the worker is complete, it will, then, store the products information as csv file too 

@author Siham Saidoun
@date 05/22/2021
@version V.2

V.2: Add ability to store the products data into Mongo Data Base

Note: This script needs a python version 3. Ensure to install the following packages (pip install package-name)

selenium
atpbar
multiprocess
bs4
pandas
numpy
json
os


To run the script in a Linux/MacOS command line execute: python3.8 Open-food-V1.py &
The parameter `&` executes the processes as background process
"""

# Import needed libraries
from sys import platform as _platform
from atpbar import atpbar
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import zipfile
from os import path
import math
import pandas as pd
import numpy as np
import json

# Multi processes
import multiprocessing
from multiprocessing import Pool

# Time execution
import time
import datetime
from datetime import datetime as dt

# Saving MongoDB
from pymongo import MongoClient

# Logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format="[PID#%(process)d]:%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scrapping-V1.log"),
        logging.StreamHandler()
    ]
)


# Constants
BASE_URL = 'https://fr.openfoodfacts.org/'
PRODUCT_URL = 'https://fr.openfoodfacts.org/produit/'
UNKNOWN_VALUE = 'XXX'

NUMBER_OF_PROCESS = 4
NUMBER_PRODUCTS_BY_PAGE = 100

WEB_DRIVER_TIME_OUT_IN_SECONDS = 20
CHROME_OPTIONS = webdriver.ChromeOptions()
CHROME_OPTIONS.add_argument('--headless')

# Use default MongoDB configuration
MONGO_DB_URL = '127.0.0.1'
MONGO_DB_PORT = 27017



class MongoDBUtils:
    """
    Mongo DB Utils class
    '''

    Attributes
    ----------

    Methods
    -------
    get_db():
        Get the scrapping Mongo DB .
    """
    CLIENT = MongoClient(MONGO_DB_URL, MONGO_DB_PORT)
    
    @staticmethod
    def get_db():
        return MongoDBUtils.CLIENT.scrapping

class TimerUtils:
    """
    Timer / Logger class helper. It allows to start and stop a timer with messages logging
    '''

    Attributes
    ----------
    begin: time
        the timer's starting
    end: time
        the timer's ending
    level: str
        the logger level

    Methods
    -------
    start(message=""):
        Starts the timer with the an optional message.
    stop(message=""):
        Stops the timer with the an optional message.
    startOnly():
        Starts the timer without any message.
    """

    def __init__(self, level=logging.DEBUG):
        """
        Constructs timer helper instance

        Parameters
        ----------
            level : str
                the logger level
        """

        self.begin = None
        self.end = None
        self.level = level

    def start(self, message='Start Computation...'):
        """
        Starts the timer with an optional message logging.

        Parameters
        ----------
        message : str, optional
            message to log

        Returns
        -------
        None
        """
        self.begin = time.time()
        if self.level == logging.INFO:
            logging.info(message)
        else:
            logging.debug(message)

    def startOnly(self):
        """
        Starts the time without any message.

        Returns
        -------
        None
        """
        self.begin = time.time()

    def stop(self, message='End Computation, it took: '):
        """
        Stops the timer with an optional message logging.

        Parameters
        ----------
        message : str, optional
            message to log

        Returns
        -------
        None
        """
        self.end = time.time()
        duration_in_sec = self.end - self.begin
        duration = datetime.timedelta(seconds=duration_in_sec)
        if self.level == logging.INFO:
            logging.info(f'{message}=>{str(duration)} (hh:mm:ss)')
        else:
            logging.debug(f'{message}=>{str(duration)} (hh:mm:ss)')


def get_webdriver_configuration():
    """
    Gets the web driver configuration. It downloads the chrome webdriver required by operating system

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    url = ''
    path = ''
    if _platform in 'linux':
        # Linux
        url = 'https://chromedriver.storage.googleapis.com/90.0.4430.24/chromedriver_linux64.zip'
        path = './chromedriver'
    elif _platform in 'darwin':
        # MacOS
        url = 'https://chromedriver.storage.googleapis.com/90.0.4430.24/chromedriver_mac64.zip'
        path = './chromedriver'
    else:
        # Windows
        url = 'https://chromedriver.storage.googleapis.com/90.0.4430.24/chromedriver_win32.zip'
        path = './chromedriver.exe'

    return url, path


def download_webdriver():
    """
    Downloads the web driver for the local operating system

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    url, local_path = get_webdriver_configuration()
    if path.exists(local_path):
        logging.info('Web driver already exists.')
        return

    timer_utils = TimerUtils()
    timer_utils.start('Start downloading web driver...')
    r = requests.get(url)
    # Download it
    with open("webdriver.zip", "wb") as code:
        code.write(r.content)

    # Unzip it
    with zipfile.ZipFile('webdriver.zip', 'r') as zip_ref:
        zip_ref.extractall('.')
    timer_utils.stop('Downloading webdriver done.')


def get_webdriver():
    """
     Gets a webdriver instance. It downloads the webdriver for operating system running this script. 
     Note: Depending on your operating system to give the executable permissions to the downloaded chrome driver

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    url, local_path = get_webdriver_configuration()
    download_webdriver()
    driver = webdriver.Chrome(
        executable_path=local_path,  options=CHROME_OPTIONS)
    #driver=webdriver.Chrome('chromedriver', options=CHROME_OPTIONS)
    # driver.set_page_load_timeout(WEB_DRIVER_TIME_OUT_IN_SECONDS)
    return driver


def get_number_of_pages():
    """
    Gets the number of pages of the Open Food Facts website

    Parameters
    ----------
    None

    Returns
    -------
        int: the number of the pages
    """
    r = requests.get(BASE_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    span = soup.find('span', style='font-weight:bold;')
    text = span.text
    # get only number of products (i.e: 801 290 produits)
    number_of_products = ''.join(filter(str.isdigit, text))
    number_of_products = int(number_of_products)
    number_of_pages = number_of_products / NUMBER_PRODUCTS_BY_PAGE
    # ceil to upper value
    number_of_pages = math.ceil(number_of_pages)
    return number_of_pages


def get_products_urls_by_page(driver, page=1):
    """
     Gets the products url for the given page.

    Parameters
    ----------
    driver: object, required
            the webdriver instance
    page: the number's page

    Returns
    -------
        list: the product urls of the given page
    """
    timer_utils = TimerUtils()
    timer_utils.start(f'Start get products of page {page}')
    url = BASE_URL + str(page)
    driver.get(url)
    products = driver.find_elements_by_xpath("//a[@class='list_product_a']")
    urls = [product.get_attribute('href') for product in products]
    timer_utils.stop(f'End get products of page {page}')
    return urls


def get_product_name(driver):
    """
     Gets the product's name.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        str: the product's name
    """
    product_name = driver.find_element_by_xpath("//h1[@itemprop='name']")
    text = product_name.text
    return text


def get_code_barres(driver):
    """
     Gets the product's code barre.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        str: the product's code barre if any, UNKNOWN_VALUE otherwise
    """
    elements = driver.find_elements_by_xpath("//span[@property='food:code']")
    if len(elements) == 1:
        return elements[0].text
    else:
        return UNKNOWN_VALUE


def get_scores(driver):
    """
     Gets the product's score: Nutri Score, NOVA and Eco Score.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        tuple: the product's score
    """
    attributes_grid = driver.find_element_by_id('attributes_grid')
    elements = attributes_grid.find_elements_by_tag_name('h4')
    nutri_score = UNKNOWN_VALUE
    nova = UNKNOWN_VALUE
    eco_score = UNKNOWN_VALUE
    for element in elements:
        text = element.text
        if 'Nutri-Score' in text:
            nutri_score = text
        elif 'NOVA' in text:
            nova = text
        elif 'Éco-Score' in text:
            eco_score = text
    return nutri_score, nova, eco_score


def get_product_caracteristics(driver):
    """
     Gets the product's caracteristics.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        dict: the product's caracteristics value if it exists, If an information is missing, it will be replaced by UNKNOWN_VALUE
        caracteristics = {
            'Dénomination générique': UNKNOWN_VALUE,
            'Quantité': UNKNOWN_VALUE,
            'Conditionnement': UNKNOWN_VALUE,
            'Marques': UNKNOWN_VALUE,
            'Catégories': UNKNOWN_VALUE,
            'Labels, certifications, récompenses': UNKNOWN_VALUE,
            'Origine des ingrédients': UNKNOWN_VALUE,
            'Lieux de fabrication ou de transformation': UNKNOWN_VALUE,
            'Code de traçabilité': UNKNOWN_VALUE,
            'Lien vers la page du produit sur le site officiel du fabricant': UNKNOWN_VALUE,
            'Magasins': UNKNOWN_VALUE,
            'Pays de vente': UNKNOWN_VALUE,
        }
    """
    caracteristics = {
        'Dénomination générique': UNKNOWN_VALUE,
        'Quantité': UNKNOWN_VALUE,
        'Conditionnement': UNKNOWN_VALUE,
        'Marques': UNKNOWN_VALUE,
        'Catégories': UNKNOWN_VALUE,
        'Labels, certifications, récompenses': UNKNOWN_VALUE,
        'Origine des ingrédients': UNKNOWN_VALUE,
        'Lieux de fabrication ou de transformation': UNKNOWN_VALUE,
        'Code de traçabilité': UNKNOWN_VALUE,
        'Lien vers la page du produit sur le site officiel du fabricant': UNKNOWN_VALUE,
        'Magasins': UNKNOWN_VALUE,
        'Pays de vente': UNKNOWN_VALUE,
    }

    container = driver.find_elements_by_xpath(
        "//div[@class='medium-12 large-8 xlarge-8 xxlarge-8 columns']")[0]
    elements = container.find_elements_by_tag_name('p')
    for element in elements:
        # label
        label = element.find_element_by_tag_name("span").text

        # for product link, we need the href attribute
        if 'site officiel du fabricant' in label:
            links = element.find_elements_by_tag_name("a")
            value = links[0].get_attribute('href') if len(
                links) == 1 else UNKNOWN_VALUE
        else:
            # example of parent.text: 'Conditionnement : Plastique, mixed plastic-packet'
            # we should keep only 'Plastique, mixed plastic-packet'
            value = element.text.replace(label, '')

        # looking for the description by key
        key = label.replace(' :', '')
        if key in caracteristics.keys():
            caracteristics[key] = value
    return caracteristics


def get_ingredients(driver):
    """
     Gets the product's ingredients.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        dict: the product's ingredients
         ingredients = {
            'Additifs': UNKNOWN_VALUE,
            "Ingrédients issus de l'huile de palme": 'No'
        }
    """
    ingredients = {
        'Additifs': UNKNOWN_VALUE,
        "Ingrédients issus de l'huile de palme": 'No'
    }
    elements = driver.find_elements_by_xpath(
        "//div[@class='medium-6 columns']")
    for element in elements:
        # label
        label = element.find_element_by_tag_name("b").text
        label = label.replace(' :', '')
        if label in ingredients.keys():
            if "l'huile de palme" in label:
                ingredients[label] = 'Yes'
            else:
                ingredients[label] = element.find_element_by_tag_name('a').text

    return ingredients


def get_100g_nutritional_info(driver):
    """
     Gets the product's nutritional information for 100 g.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        dict: the product's nutritional_info for 100 g. If an information is missing it will be replaced by UNKNOWN_VALUE
          nutritionals = {
            'Matières grasses / Lipides': UNKNOWN_VALUE,
            'Acides gras saturés': UNKNOWN_VALUE,
            'Sucres': UNKNOWN_VALUE,
            'Sel': UNKNOWN_VALUE
          }
    """
    nutritionals = {
        'Matières grasses / Lipides': UNKNOWN_VALUE,
        'Acides gras saturés': UNKNOWN_VALUE,
        'Sucres': UNKNOWN_VALUE,
        'Sel': UNKNOWN_VALUE
    }

    elements = driver.find_elements_by_xpath(
        "//div[@class='small-12 xlarge-6 columns']")
    for element in elements:
        # Filter by label
        tags = element.find_elements_by_tag_name('h4')
        if len(tags) > 0 and tags[0].text == 'Repères nutritionnels pour 100 g':
            # each nutritional value is separeted by 'br'
            values = element.text.split('\n')
            # Escape the h4 tag value. And, search by dict key
            for value in values[1:]:
                for key in nutritionals.keys():
                    if key in value:
                        nutritionals[key] = value
    return nutritionals


def get_comparison(driver):
    """
     Gets the product's comparison.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        str: the product's comparison seprated by pipe |
    """
    elements = driver.find_elements_by_xpath(
        "//input[@type='checkbox' and @checked='checked' and @class='show_comparison']")
    # No checkbox checked
    if len(elements) == 0:
        return UNKNOWN_VALUE

    # Need to navigate to its parent 'label' tag
    values = [element.find_element_by_xpath('..').text for element in elements]
    text = '|'.join(values)
    return text


def get_nutritional_info(driver):
    """
     Gets the product's nutritional information.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        dict: the product's nutritional_info for 100 g. If an information is missing it will be replaced by UNKNOWN_VALUE
          nutritionals = {
            'Energie (kcal)': UNKNOWN_VALUE,
            'Nombre de calorie (Énergie (kcal))': UNKNOWN_VALUE
        }
    """
    nutritionals = {
        'Energie (kcal)': UNKNOWN_VALUE,
        'Nombre de calorie (Énergie (kcal))': UNKNOWN_VALUE
    }

    # use xpath with to get elements and check presence/existence
    nutriments = driver.find_elements_by_xpath(
        "//tr[@id='nutriment_energy-kcal_tr']")
    if len(nutriments) > 0:
        elements = nutriments[0].find_elements_by_xpath("td")
        if len(elements) != 0:
            # first td is the label
            nutritionals['Energie (kcal)'] = elements[1].text

    nutriments = driver.find_elements_by_xpath(
        "//tr[@id='nutriment_energy_tr']")
    if len(nutriments) > 0:
        elements = nutriments[0].find_elements_by_xpath("td")
        if len(elements) != 0:
            # first td is the label
            nutritionals['Nombre de calorie (Énergie (kcal))'] = elements[1].text
    return nutritionals


def get_environment_impact(driver):
    """
     Gets the product's environment impact.

    Parameters
    ----------
    driver: object, required
            the webdriver instance

    Returns
    -------
        str: the product's environment impact description. If the information is missing it will be replaced by UNKNOWN_VALUE
    """
    attributes_grid = driver.find_element_by_id('attributes_grid')
    elements = attributes_grid.find_elements_by_tag_name('*')
    for element in elements:
        if 'Éco-Score' in element.text:
            return element.find_element_by_tag_name('span').text
    return UNKNOWN_VALUE


def get_product_info(driver, url):
    """
     Gets the product's information. 
     This method will load a given product by its url and call the different helpers methods to get the product's information.

    Parameters
    ----------
    driver: object, required
            the webdriver instance
    url: str, required
            the product's url

    Returns
    -------
        dict: the whole product's information
    """
    # get product id and name for logging
    product_identifier = url.replace(PRODUCT_URL, '')

    timer = TimerUtils()
    timer.start(f'Start scrapping product: {product_identifier}')

    # Product information
    info = {}

    timer_by_call = TimerUtils()
    # Browse to the product url
    timer_by_call.start(f'Start get driver on {product_identifier}')
    driver.get(url)
    timer_by_call.stop(f'End get driver on {product_identifier}')

    # Get product name
    timer_by_call.start(f'Start get product name on {product_identifier}')
    info['Nom du Produit'] = get_product_name(driver)
    timer_by_call.stop(f'End get product name on {product_identifier}')

    # Get code barres
    timer_by_call.start(
        f'Start get product code barre on {product_identifier}')
    info['Code_barres'] = get_code_barres(driver)
    timer_by_call.stop(f'End get product code barre on {product_identifier}')

    # Get scores
    timer_by_call.start(
        f'Start get product nutrition score on {product_identifier}')
    nutri_score, nova, eco_score = get_scores(driver)
    info['Nutri-score'] = nutri_score
    info['NOVA'] = nova
    info['Eco-Score'] = eco_score
    timer_by_call.stop(f'End get product score on {product_identifier}.')

    # Get caracteristics
    timer_by_call.start(
        f'Start get product caracteristicsore on {product_identifier}')
    caracteristics = get_product_caracteristics(driver)
    info.update(caracteristics)
    timer_by_call.stop(
        f'End get product caracteristicsore on {product_identifier}.')

    # Get ingredients
    timer_by_call.start(
        f'Start get product ingredients on {product_identifier}')
    ingredients = get_ingredients(driver)
    info.update(ingredients)
    timer_by_call.stop(f'End get product ingredients on {product_identifier}.')

    # Get nutritionals 100g
    timer_by_call.start(
        f'Start get product nutritionals 100g on {product_identifier}')
    nutritionals_100g = get_100g_nutritional_info(driver)
    info.update(nutritionals_100g)
    timer_by_call.stop(
        f'End get product nutritionals 100g on {product_identifier}.')

    # Get comparison
    timer_by_call.start(
        f'Start get product comparison on {product_identifier}')
    comparison = get_comparison(driver)
    info['Comparaison avec les valeurs moyennes des produits de même catégorie'] = comparison
    timer_by_call.stop(f'End get product comparison on {product_identifier}.')

    # Get nutritionals
    timer_by_call.start(
        f'Start get product nutritionals on {product_identifier}')
    nutritionals = get_nutritional_info(driver)
    info.update(nutritionals)
    timer_by_call.stop(
        f'End get product nutritionals on {product_identifier}.')

    # Get environment impact
    timer_by_call.start(
        f'Start get product environment impact on {product_identifier}')
    env_impact = get_environment_impact(driver)
    info['Impact environnemental'] = env_impact
    timer_by_call.stop(
        f'End get product environment impact on {product_identifier}.')

    timer.stop(f'End scrapping product: {url}.')
    return info


def get_products_df(process_id=1, pages=[]):
    """
     Gets the products dataframe. This method will load all produts pages. Stores the products as a json and csv file. 

    Parameters
    ----------
    driver: object, required
            the webdriver instance
    pages: list, required
            the pages to process

    Returns
    -------
        dataframe: the dataframe representation of the products loaded.
    """
    # Logging / Timer
    timer_utils = TimerUtils()
    timer_utils.start(
        f'Start worker with id {process_id} to get products of {len(pages)} pages.')

    # Get web driver
    driver = get_webdriver()

    # Process page description
    description_pages = f'Worker#{process_id}#Pages'
    description_products = f'Worker#{process_id}#Products'
    products = []

    timer_utils_p = TimerUtils()
    total_success = 0
    total_error = 0
    file_name = f'open_food_data#{str(process_id)}'
    for page_number in atpbar(range(len(pages)), name=description_pages):
        timer_utils_p.start(
            f'Worker {process_id} starts processing page {page_number}')
        page = pages[page_number]
        urls = get_products_urls_by_page(driver, page)
        size = len(urls)
        mongo_db= MongoDBUtils.get_db()
        for product_index in atpbar(range(size), name=description_products):
            url = urls[product_index]
            try:
                info = get_product_info(driver=driver, url=url)
                products.append(info)
                mongo_db.products.insert_one(info)
                with open(f'{file_name}.json', "w") as write_file:
                    json.dump(products, write_file)
                total_success += 1
            except Exception as e:
                # Log the error and continue
                total_error += 1
                logging.error(
                    f"Error on product {url}, page {page_number + 1}. {e}")

        now = dt.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

        logging.info(
            f"=> Worker {process_id} => Total of {total_success} products are correctly retrieved, {total_error} are in error. =>{dt_string}")

    
        timer_utils_p.stop(
            f'Worker with id {process_id} terminates processing page {page_number}. In: ')

    # Build Dataframe
    df = pd.DataFrame(data=products)

    # Store df as csv
    df.to_csv(f'{file_name}.csv')

    timer_utils.stop(
        f'Start worker with id {process_id} to get products of {len(pages)} pages. In: ')
    return df


def main():
    """
     The main entry of this script. 
     This function will retrieve and split the whole pages into NUMBER_OF_PROCESS buckets / parts and starts a worker by a given bucket.
     Once all workers/process have beed started, this function (main) will wait until their end.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    # Get number of pages
    number_of_pages = get_number_of_pages()
    pages = list(np.arange(1, number_of_pages + 1))
    timer_utils = TimerUtils()
    timer_utils.start(
        f'Start getting products information of {len(pages)} pages')

    # Split to different = number of process
    buckets = np.array_split(pages, NUMBER_OF_PROCESS)
    pool = Pool(processes=NUMBER_OF_PROCESS)
    jobs = []
    index = 0

    now = dt.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    logging.info(f'Start {NUMBER_OF_PROCESS} workers at {dt_string}')

    # Create a global variable.
    while index < len(buckets):
        process_id = index
        pages = buckets[index]
        process = pool.apply_async(get_products_df, args=(
            process_id, pages,))
        jobs.append(process)
        index += 1

    # Close the pool
    pool.close()

    # Wait until finishing all process
    results = [job.get() for job in jobs]

    timer_utils.stop(
        f'End getting products information of total {len(pages)} pages')

def check_config():
    """
    Checks if the needed resources are installed, i.e: MongoDB

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    try:
        client = MongoDBUtils.CLIENT
        logging.info(f'Mongo DB configuration: \n MongoDB:Version=>{client.server_info()["version"] }')
    except:
        logging.error('No Mongo DB found!. V2 of this script requires MongoDB.')
        raise Exception("This script version requires MongoDB.")

if __name__ == "__main__":
    logging.info('Start....')
    logging.info(f'Machine with {multiprocessing.cpu_count()} CPU.')
    main()
    logging.info('End!')
