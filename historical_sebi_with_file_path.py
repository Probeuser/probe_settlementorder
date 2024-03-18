import os
import time
import requests
import traceback
import pandas as pd
import mysql.connector
from bs4 import BeautifulSoup
from selenium import webdriver
from mysql.connector import errorcode
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=2&ssid=9&smid=2"



host = 'localhost'
user = 'root'
password = 'root'
database = 'sebi'
auth_plugin = 'mysql_native_password'




connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    auth_plugin=auth_plugin
)

cursor = connection.cursor()




download_folder = r"C:\Users\mohan.7482\Desktop\SEBI\downloaded_files\settlement_order_files"


chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument(f"--disable-notifications")  
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_folder,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True
})



base_path = "pdfdownload"

sub_path = "settlementorder_pdf"



def download_pdf_files(df, type_sebi_text):


    try:
        for index, row in df.iterrows():
            link = row['Link']

            browser = webdriver.Chrome(options=chrome_options)

            try:
                browser.get(link)
                time.sleep(5)
                browser.maximize_window()
                time.sleep(5)
                iframe_tag = WebDriverWait(browser, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//iframe"))
                )

                src_value = iframe_tag.get_attribute("src")
                file_name = src_value.split("/")[-1]
                time.sleep(5)

                browser.switch_to.frame(iframe_tag)
                time.sleep(5)
                download_button = browser.find_element(By.XPATH, '//*[@id="download"]')
                time.sleep(5)
                download_button.click()
                time.sleep(10)
                print(f"File {file_name} downloaded.")

                # Add the row to the DataFrame with the downloaded file information
                df.at[index, 'pdf_file_name'] = file_name
                relative_path = os.path.join(base_path, sub_path, file_name)
                relative_path = relative_path.replace('\\','/')
                df.at[index, 'pdf_path'] = '/' + relative_path
                # Save the updated DataFrame to Excel
                final_excel_file_with_pdf_name = f'final_excel_sheet_{type_sebi_text}.xlsx'
                final_excel_file_with_pdf_path = fr"C:\Users\mohan.7482\Desktop\SEBI\final_excel_sheets\{final_excel_file_with_pdf_name}"
                df.to_excel(final_excel_file_with_pdf_path, index=False)
                
            except Exception as e:
                print(f"Failed to download file {index + 1}. {str(e)}")
                traceback.print_exc()
                final_excel_file_with_pdf_name = f'final_excel_sheet_{type_sebi_text}.xlsx'
                final_excel_file_with_pdf_path = fr"C:\Users\mohan.7482\Desktop\SEBI\final_excel_sheets\{final_excel_file_with_pdf_name}"
                df.to_excel(final_excel_file_with_pdf_path, index=False)
                continue

            finally:
                browser.quit()

    except Exception as e:
        print(df)
        final_excel_file_with_pdf_name = f'final_excel_sheet_{type_sebi_text}.xlsx'
        final_excel_file_with_pdf_path = fr"C:\Users\mohan.7482\Desktop\SEBI\final_excel_sheets\{final_excel_file_with_pdf_name}"
        df.to_excel(final_excel_file_with_pdf_path, index=False)
        traceback.print_exc()

# Assuming 'chrome_options' and 'path' variables are defined elsewhere in your code.













def extract_data_website(cursor):
    browser = webdriver.Chrome()

    try:

        browser.get(url)
        browser.maximize_window()
        enforcement  = WebDriverWait(browser, 30).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="menu3"]'))
            )
        enforcement.click()
        orders = WebDriverWait(browser, 30).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="member-wrapper"]/section/div/ul/li[1]/a'))
            )
        orders.click()

        type_sebi= WebDriverWait(browser, 30).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="member-wrapper"]/section/div/ul/li[3]/a'))
            )
        type_sebi_text = type_sebi.get_attribute('innerText')
        print(type_sebi_text)
        type_sebi.click()


        # type_sebi = browser.find_element(By.XPATH,'//*[@id="subSectMenu_3"]')
        
        # type_sebi_text.replace('/',' ')
       


        
        total_records_text = browser.find_element(By.CSS_SELECTOR, ".pagination_inner p").text
        total_records = int(total_records_text.split()[-2])

       
        total_pages = (total_records + 24) // 25

        data = []

        for i in range(1, total_pages + 1):
            
            # time.sleep(5)
            page_xpath = f"//*[@class='pagination_outer']/ul/li/a[text()='{i}']"
            page_link = WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.XPATH, page_xpath))
            )
            page_link.click()

            table = WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.ID, 'sample_1'))
            )

            
            page_source = browser.page_source

            
            soup = BeautifulSoup(page_source, 'html.parser')

           
            table = soup.find('table', {'id': 'sample_1'})

            
            for row in table.find_all('tr')[1:]:
                columns = row.find_all('td')
                date = columns[0].get_text(strip=True)
                title = columns[1].find('a').get_text(strip=True)
                link = columns[1].find('a')['href']
                data.append({'Date': date, 'Title': title, 'Link': link, 'type': type_sebi_text})

        
        df = pd.DataFrame(data)

        excel_file_name = f'sebi_data_all_pages_{type_sebi_text}.xlsx'
        excel_file_path = rf"C:\Users\mohan.7482\Desktop\SEBI\first_set_excel_sheet_files\{excel_file_name}"
        df.to_excel(excel_file_path, index=False)
        print(f"Data from all pages saved to sebi_data_all_pages_{type_sebi_text}.xlsx")
        excel_data = pd.read_excel(excel_file_path)
        df = pd.DataFrame(excel_data)
        download_pdf_files(df,type_sebi_text)
        
    

    except Exception as e:
        # print(f"Failed. {str(e)}")
        traceback.print_exc()

    finally:
        
        browser.quit()






def insert_excel_data_to_mysql(excel_file_path, cursor):
    try:
        df = pd.read_excel(excel_file_path)

        table_name = "sebi_ocm_demo"
        
        # Iterate over DataFrame rows and insert into MySQL table
        for index, row in df.iterrows():
            insert_query = f"""
                INSERT INTO {table_name} (Date, Title, Link, type)
                VALUES (%s, %s, %s, %s)
            """
            # Extract values from the row
            values = (row['Date'], row['Title'], row['Link'], row['type'])

            # Execute the SQL query
            cursor.execute(insert_query, values)

        # Commit the changes
        connection.commit()
        cursor.close()
        print("Data inserted into the database table")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Access denied. Please check your MySQL username and password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Error: The specified database '{database}' does not exist.")
        else:
            print(f"Error: {err}")

    except Exception as e:
        print(f"Failed. {str(e)}")


# excel_path = fr"C:\Users\mohan.7482\Desktop\SEBI\first_set_excel_sheet_files\sebi_data_all_pages_Settlement Order.xlsx"
# excel_data = pd.read_excel(excel_path)
# df = pd.DataFrame(excel_data)
# type_sebi_text = "Settlement Order"
# download_pdf_files(df,type_sebi_text)


extract_data_website(cursor)




