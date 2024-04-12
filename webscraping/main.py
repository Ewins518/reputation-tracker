import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException


headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip',
    'DNT': '1',  # Do Not Track Request Header
    'Connection': 'keep-alive',
    'Referer': 'https://google.com'
}

options = webdriver.ChromeOptions()
options.headless = True  # for headless mode (without UI)

path = ("webscraping/driver/chromedriver-linux64")
s = Service(path)
driver_ = webdriver.Chrome(service=s, options=options)

def get_soup_from_url(currentUrl):
    driver = driver_
    driver.get(currentUrl)
    html = driver.page_source
    driver.quit()  # It's important to close the driver to free resources

    soup = BeautifulSoup(html, "html.parser")

    return soup

def getCategoriesNames():
    categories = []
    try:
        driver = driver_
        driver.get("https://www.amazon.co.uk/")

        # Wait for the dropdown to be visible
        wait = WebDriverWait(driver, 5)  # 10 seconds wait
        el = wait.until(EC.presence_of_element_located((By.ID, 'searchDropdownBox')))

        for option in el.find_elements(By.TAG_NAME, 'option'):
            categories.append(option.text)
    except TimeoutException:
        print("Timeout while waiting for the search dropdown box.")
    except NoSuchElementException:
        print("Some elements were not found on the page.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()

    return categories

def getSubCategorie(Categorie):
    driver = driver_
    driver.get("https://www.amazon.co.uk/")
    el = driver.find_element(By.ID, 'searchDropdownBox')
    for option in el.find_elements(By.TAG_NAME, 'option'):
        if option.text == Categorie:
            print(option.text)
            option.click()  # select() in earlier versions of webdriver
            try:
                el = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, 'nav-search-submit-button')))
                el.click()
                url = driver.current_url
                print('current url', url)
                break

            except:
                driver.quit()
    return url

def get_sub_categorie(currentUrl):
    soup = get_soup_from_url(currentUrl)
    subcategoriesList = soup.find_all("li",  {"class":"apb-browse-refinements-indent-2"})
    
    # Extract the subcategory names and URLs
    subcategories = []
    base_url = "https://www.amazon.co.uk"  
    for element in subcategoriesList:
        subcategory_name = element.find('span', {'dir': 'auto'}).text.strip()
        subcategory_url = base_url + element.find('a')['href']
        subcategories.append([subcategory_name, subcategory_url])
    
    return subcategories

def get_sub_sub_categories(currentUrl):
   # Initially, try to find further subcategories
    soup = get_soup_from_url(currentUrl)
    subcategories_elements = soup.find_all("li",  {"class":"apb-browse-refinements-indent-2"})

    
    base_url = "https://www.amazon.co.uk"
    
    if not subcategories_elements:
        # No further subcategories, assume we are at a product level
        return [], False
    
    results = []
    for element in subcategories_elements:
        subcategory_name = element.find('span', {'dir': 'auto'}).text.strip()
        subcategory_url = base_url + element.find('a')['href']
        results.append([subcategory_name, subcategory_url])
    
    return results, True

def get_products_from_page(url):
    soup = get_soup_from_url(url)
    
    # Find all span elements with the class for product names
    product_spans = soup.find_all("span", class_="a-size-base-plus a-color-base a-text-normal") 
    product_list = [span.get_text(strip=True) for span in product_spans]
    
    return product_list

def check_url(currentUrl):
    response = requests.get(currentUrl, headers=headers)
    print(response.status_code)  # Check if the request was successful (200)
   
#category = "Apps & Games"
#url  = getSubCategorie(category)
#
#print(url)
#check_url(url)
#
#print(get_sub_categorie(url))

#print(get_sub_sub_categories("https://www.amazon.co.uk/s?bbn=9314650031&rh=n%3A1661657031%2Cn%3A9314671031&dc&qid=1712879466&rnid=9314650031&ref=lp_9314650031_nr_n_4"))

#print(get_products_from_page("https://www.amazon.co.uk/s?bbn=9314650031&rh=n%3A1661657031%2Cn%3A9314671031&dc&qid=1712879466&rnid=9314650031&ref=lp_9314650031_nr_n_4"))

print(getCategoriesNames())
