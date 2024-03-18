import re
import os
import sys
import time
import shutil
import traceback
import pyautogui
import pandas as pd
import mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from config import sebi_config
from sqlalchemy import create_engine
from mysql.connector import errorcode
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchWindowException
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException,WebDriverException


url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=2&ssid=9&smid=2"



host = '4.213.77.165'
user = 'root1'
password = 'Mysql1234$'
database = 'sebi'
auth_plugin = 'mysql_native_password'

log_list = [None] * 8

no_data_avaliable = 0

no_data_scraped = 0




connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    auth_plugin=auth_plugin
)

connection1 = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    auth_plugin=auth_plugin
)

cursor = connection.cursor()

log_cursor = connection1.cursor()



download_folder = r"C:\inetpub\wwwroot\Sebi_apiproject\pdfdownload\settlementorder"


chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument(f"--disable-notifications")  
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_folder,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True
})

browser = webdriver.Chrome(options=chrome_options)


current_date = datetime.now().strftime("%Y-%m-%d")

def get_data_count(log_cursor):
    log_cursor.execute("SELECT COUNT(*) FROM sebi_orders  WHERE type_of_order = 'settlementorder';")
    return log_cursor.fetchone()[0]


data_in_database = get_data_count(cursor)

def insert_log_into_table(log_cursor, log_list):
    query = """
        INSERT INTO sebi_log (source_name, script_status, data_available, data_scraped, total_record_count, failure_reason, comments, source_status)
        VALUES (%(source_name)s, %(script_status)s, %(data_available)s, %(data_scraped)s, %(total_record_count)s, %(failure_reason)s, %(comments)s, %(source_status)s)
    """
    values = {
        'source_name': log_list[0] if log_list[0] else None,
        'script_status': log_list[1] if log_list[1] else None,
        'data_available': log_list[2] if log_list[2] else None,
        'data_scraped': log_list[3] if log_list[3] else None,
        'total_record_count': log_list[4] if log_list[4] else None,
        'failure_reason': log_list[5] if log_list[5] else None,
        'comments': log_list[6] if log_list[6] else None,
        'source_status': sebi_config.source_status
   
    }

    log_cursor.execute(query, values)
   




def insert_excel_data_to_mysql(final_excel_sheets_path, cursor):
    global log_list, log_cursor
    try:
        df = pd.read_excel(final_excel_sheets_path)

        table_name = "sebi_orders"
       
        df = df.where(pd.notnull(df), None)
       
       
     
        for index, row in df.iterrows():
            insert_query = f"""
                INSERT INTO {table_name} (date_of_order, title_of_order, type_of_order, link_to_order, pdf_file_path, pdf_file_name)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
           
            values = (row[0], row[1], row[3], row[2], row[5], row[4])

         
            cursor.execute(insert_query, values)
        connection.commit()
        cursor.close()



        log_list[0] = "sebi_settlementorder"
        log_list[1] = "Succes"
        log_list[2] = no_data_avaliable
        log_list[3] = get_data_count(log_cursor) - data_in_database
        log_list[4] = get_data_count(log_cursor)
        print(log_list)
        insert_log_into_table(log_cursor, log_list)
        connection1.commit()
        log_list = [None] * 8
        print("Data inserted into the database table")


    except Exception as e:
        log_list[0] = "sebi_settlementorder"
        log_list[1] = "Failure"
        log_list[4] = get_data_count(log_cursor)
        log_list[5] = "error in insert part"
        print(log_list)
        insert_log_into_table(log_cursor, log_list)
        connection1.commit()
        log_list = [None] * 8
        traceback.print_exc()
        sys.exit("script error")






def move_files_to_specific_folder(file_name_excel_path, type_sebi_text):

    main_folder_path = r"C:\inetpub\wwwroot\Sebi_apiproject\pdfdownload\settlementorder"
    excel_data = pd.read_excel(file_name_excel_path)

    df = pd.DataFrame(excel_data)

    years = set()

    months = set()

    final_rows = set()


    def save_to_excel():
        new_df = pd.DataFrame(final_rows)
        final_excel_sheets_name = f'final_excel_sheet{type_sebi_text}{current_date}.xlsx'
        final_excel_sheets_path = fr"C:\Users\devadmin\sebi_final_script\so\final_excel_sheets\{final_excel_sheets_name}"
        new_df.to_excel(final_excel_sheets_path, index=False)
        insert_excel_data_to_mysql(final_excel_sheets_path, cursor)




    def move_files(selected_month_rows, year_folder_path, month, year):
        for row in selected_month_rows:
            file = row[4]
            if pd.isna(file):
                final_rows.add(row + ("nan",))
            else:
                month_folder_path = os.path.join(year_folder_path, month)
                print(month_folder_path) # Moved outside the loop
                if not os.path.exists(month_folder_path):  # Check if the month directory exists
                    os.makedirs(month_folder_path)
                else:
                    print(month, "it is already exists")


                old_file_path = os.path.join(download_folder,str(file))
                new_file_path = os.path.join(month_folder_path,str(file))
                shutil.move(old_file_path, new_file_path)
                _,_,relative_path = new_file_path.partition('Sebi_apiproject')
                relative_path = relative_path.replace('\\','/')
                final_rows.add(row + (relative_path,))



    def create_year_folders(selected_year_rows,year,months):
        year_folder_path = os.path.join(main_folder_path,year)
        if not os.path.exists(year_folder_path):
            os.makedirs(year_folder_path)
        else:
            print(year,"the folder is already exists")
        for month in months:
            selected_month_rows = set()  
            for row in selected_year_rows:
                if month in row[0]:
                    selected_month_rows.add(row)
                    # print(row)
            # print(selected_month_rows)
            move_files(selected_month_rows,year_folder_path,month,year)



    def select_year_wise(months,years):
        for year in years:
            selected_year_rows = set()
            for index, row in df.iterrows():
                if year in row['Date']:
                    selected_year_rows.add(tuple(row))
            print(year)
            # print(selected_year_rows)
            create_year_folders(selected_year_rows,year,months)
        save_to_excel()



    for index, row in df.iterrows():
        month_year = row['Date']
        parts = month_year.split(", ")
        year = parts[-1]
        years.add(year)
        month = parts[0].split()[0]  
        months.add(month)
    select_year_wise(months,years)

    print(months,"months in ewxcel sheets")
    print(years,"years in the excel sheets")







def get_non_pdf_download(pdf_link,driver,name,index):
 try:    

    # Navigate to your web page
    driver.get(pdf_link)
    print("pdf_link ======",pdf_link,name,index)
    time.sleep(10)
    print_button = driver.find_element(By.XPATH, '//*[@id="member-wrapper"]/section[2]/h1/div/ul/li[7]/a')
    print_button.click()
    print("print button clicked=====")
    time.sleep(15)


    # Get handles of all currently open windows
    all_windows = driver.window_handles

    # Switch to the new window
    new_window = [window for window in all_windows if window != driver.current_window_handle][0]
    print("new window =======",new_window)
    driver.switch_to.window(new_window)

   

    try:
        WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.XPATH, "//html/body/print-preview-app")))
       
        # Find the print-preview-app element using XPath
        print_preview_app = driver.find_element(By.XPATH, "//html/body/print-preview-app")
       
        if print_preview_app :
            shadow_element = driver.execute_script('return arguments[0].shadowRoot.querySelector("print-preview-sidebar").shadowRoot.querySelector("#container").querySelector("print-preview-destination-settings").shadowRoot.querySelector("print-preview-destination-select").shadowRoot.querySelector("print-preview-settings-section > div").querySelector(".md-select")', print_preview_app)

            # Print the shadow element
            print(shadow_element)
            if shadow_element:
                # print("Shadow element found:", shadow_element.tag_name)
                # print("HTML of the located element:", shadow_element.get_attribute('outerHTML'))
                shadow_element.click()
                time.sleep(10)
                select = Select(shadow_element)
                time.sleep(10)
                select.select_by_value('Save as PDF/local/')
                shadow_element.click()
                time.sleep(10)
                # Find the print-preview-app element using XPath
                print_preview_app = driver.find_element(By.XPATH, "//html/body/print-preview-app")
                if print_preview_app :
                    shadow_element = driver.execute_script('return arguments[0].shadowRoot.querySelector("print-preview-sidebar").shadowRoot.querySelector("print-preview-button-strip").shadowRoot.querySelector(".controls").querySelector(".action-button")', print_preview_app)
                    # Print the shadow element
                    print("save button",shadow_element)
                    if shadow_element:
                        # print("Shadow element found:", shadow_element.tag_name)
                        # print("HTML of the located element:", shadow_element.get_attribute('outerHTML'))
                        shadow_element.click()
                        time.sleep(10)
                        file_name_with_path = fr"C:\Users\devadmin\sebi_final_script\so\pdfdownload\settlementorder\settlementorder_{name}_{index}.pdf"
                        print(file_name_with_path)
                        pyautogui.write(file_name_with_path)
                        time.sleep(5)
                        pyautogui.press('enter')
                        time.sleep(5)

                        return f"settlementorder_{name}_{index}.pdf"
                else:
                    print("Shadow element save not found.")
            else:
                print("Shadow element select not found.")

    except TimeoutException:
        print("Timeout waiting for print preview to load.")
        return None
    except WebDriverException as e:
        print("An error occurred:", e)
        return None
    driver.switch_to.window(driver.window_handles[0])
    driver.quit()
 except NoSuchWindowException as e:
        print("Error: NoSuchWindowException -", e)
        # browser = webdriver.Chrome(options=chrome_options)
        # browser.get(url)
        return "Nan"





def download_pdf_files(df, type_sebi_text):
    global log_list

    excel_with_final_name = f'file_name_excel_sheet_{type_sebi_text}{current_date}.xlsx'
    file_name_excel_path = fr"C:\Users\devadmin\sebi_final_script\so\file_name_excel_sheets\{excel_with_final_name}"

    try:
        for index, row in df.iterrows():
            link = row['Link']

            browser = webdriver.Chrome(options=chrome_options)

            try:
                browser.get(link)
                browser.maximize_window()
                time.sleep(5)
                iframe_tag = WebDriverWait(browser, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//iframe"))
                )

                src_value = iframe_tag.get_attribute("src")
                file_name = src_value.split("/")[-1]


                browser.switch_to.frame(iframe_tag)
                time.sleep(5)
                download_button = browser.find_element(By.XPATH, '//*[@id="download"]')
                time.sleep(5)
                download_button.click()
                time.sleep(10)
                print(f"File {file_name} downloaded.")

                df.at[index, 'pdf_file_name'] = file_name
                df.to_excel(file_name_excel_path, index=False)
               
            except Exception as e:
                date = row['Date']
                title = row['Title']
                pattern = r'(?i)in the matter of (.*)'
                date = datetime.strptime(date, '%b %d, %Y')
                formatted_date = date.strftime('%b %d, %Y').replace(",", "").lower()
                formatted_date_with_underscores = formatted_date.replace(" ", "_").lower()

                match = re.search(pattern, title)
                if match:
                    extracted_string = match.group(1).strip().replace(' ', '_').lower()
                    modified_string = extracted_string[:-1] if extracted_string.endswith('.') else extracted_string
                else:
                    modified_string = title.lower().replace(' ', '_')[:-1] if title.lower().endswith('.') else title.lower().replace(' ', '_')

                cleaned_string = re.sub(r'[^a-zA-Z0-9\s]', '', modified_string)[:30]
                cleaned_string += f"_{formatted_date_with_underscores}"
                name = cleaned_string
                print(name, "name of the file")
                non_pdf_file_name = get_non_pdf_download(link,browser,name,index)
                print(non_pdf_file_name,"non pdf file name")
                df.at[index, 'pdf_file_name'] = non_pdf_file_name
                df.to_excel(file_name_excel_path, index=False)
                print(f"Failed to download file {index + 1}. {str(e)}")
                traceback.print_exc()
                continue

            finally:
                browser.quit()


    except Exception as e:
        print(df)
        # excel_with_final_name = f'file_name_excel_sheet_{type_sebi_text}{current_date}.xlsx'
        # file_name_excel_path = fr"C:\Users\mohan.7482\Desktop\SEBI\file_name_excel_sheets\{excel_with_final_name}"
        # df.to_excel(file_name_excel_path, index=False)
        log_list[0] = "sebi_settlementorder"
        log_list[1] = "Failure"
        log_list[4] = get_data_count(log_cursor)
        log_list[5] = "script error"
        print(log_list)
        insert_log_into_table(log_cursor, log_list)
        connection1.commit()
        log_list = [None] * 8
        traceback.print_exc()
        sys.exit("script error")

    move_files_to_specific_folder(file_name_excel_path, type_sebi_text)
   
   









# def find_new_data(excel_file_path, db_table_name, type_sebi_text):
#     global log_list
#     try:
#         database_uri = f'mysql://{user}:{password}@{host}/{database}?auth_plugin={auth_plugin}'
#         engine = create_engine(database_uri)

#         columns_to_select = ['link_to_order']
#         select_query = f"SELECT {', '.join(columns_to_select)} FROM {db_table_name} WHERE type_of_order = 'settlementorder';"
#         database_table_df = pd.read_sql(select_query, con=engine)

#         excel_columns_mapping = {'Link': 'link_to_order'}
#         excel_columns_to_select = list(excel_columns_mapping.keys())

#         excel_data_df = pd.read_excel(excel_file_path, usecols=excel_columns_to_select)
#         excel_data_df.rename(columns=excel_columns_mapping, inplace=True)

#         merged_df = pd.merge(excel_data_df, database_table_df, how='left', indicator=True)
#         missing_rows = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

#         if not missing_rows.empty:
#             print("Rows from Excel Data not in Database Table")
#             print(missing_rows)
#             new_excel_file_path = rf"C:\Users\mohan.7482\Desktop\SEBI\incremental_excel_sheets\Missing_Data_{type_sebi_text}{current_date}.xlsx"
#             missing_rows.to_excel(new_excel_file_path, index=False)
#             download_pdf_files(missing_rows, type_sebi_text)
#             print(f"Missing rows saved to {new_excel_file_path}")

#     except Exception as e:
#         log_list[0] = "sebi Settlement Order"
#         log_list[1] = "Failure"
#         log_list[4] = get_data_count(log_cursor)
#         log_list[5] = "script error"
#         print(log_list)
#         insert_log_into_table(log_cursor, log_list)
#         connection1.commit()
#         log_list = [None] * 8
#         traceback.print_exc()
#         sys.exit("script error")






# def find_new_data(excel_file_path, db_table_name, type_sebi_text):
#     global log_list
#     try:

#         database_uri = f'mysql://{user}:{password}@{host}/{database}?auth_plugin={auth_plugin}'
#         engine = create_engine(database_uri)

#         excel_data = pd.read_excel(excel_file_path)
#         excel_data_df = pd.DataFrame(excel_data)

#         columns_to_select = ['link_to_order']
#         select_query = f"SELECT {', '.join(columns_to_select)} FROM {db_table_name} WHERE type_of_order = 'settlementorder';"
#         database_table_df = pd.read_sql(select_query, con=engine)

#         missing_rows = set()

#         for index, row in excel_data_df.iterrows():
#             if row['Link'] in database_table_df:
#                 pass
#             else:
#                 missing_rows.add(tuple(row))

#         print("Rows from Excel Data not in Database Table")
#         print(missing_rows)
#         new_excel_file_path = rf"C:\Users\mohan.7482\Desktop\SEBI\incremental_excel_sheets\Missing_Data_{type_sebi_text}{current_date}.xlsx"
#         missing_data_df = pd.DataFrame(missing_rows)
#         missing_data_df.to_excel(new_excel_file_path, index=False)
#         download_pdf_files(missing_data_df, type_sebi_text)
#         print(f"Missing rows saved to {new_excel_file_path}")

#     except Exception as e:
#         log_list[0] = "sebi Settlement Order"
#         log_list[1] = "Failure"
#         log_list[4] = get_data_count(log_cursor)
#         log_list[5] = "script error"
#         print(log_list)
#         insert_log_into_table(log_cursor, log_list)
#         connection1.commit()
#         log_list = [None] * 8
#         traceback.print_exc()
#         sys.exit("script error")



def find_new_data(excel_file_path, db_table_name, type_sebi_text):
    global log_list
    try:
        database_uri = f'mysql://{user}:{password}@{host}/{database}?auth_plugin={auth_plugin}'
        engine = create_engine(database_uri)

        excel_data_df = pd.read_excel(excel_file_path)

        select_query = f"SELECT link_to_order FROM {db_table_name} WHERE type_of_order = 'settlementorder';"
        database_table_df = pd.read_sql(select_query, con=engine)

        missing_rows = []

        for index, row in excel_data_df.iterrows():
            if row['Link'] not in database_table_df['link_to_order'].values:
                missing_rows.append(row)

        print("Rows from Excel Data not in Database Table:")
        print(missing_rows)

       
        new_excel_file_path = rf"C:\Users\devadmin\sebi_final_script\so\incremental_excel_sheets\Missing_Data_{type_sebi_text}_{current_date}.xlsx"
        missing_data_df = pd.DataFrame(missing_rows)
        missing_data_df.to_excel(new_excel_file_path, index=False)
        download_pdf_files(missing_data_df, type_sebi_text)  
        print(f"Missing rows saved to {new_excel_file_path}")

    except Exception as e:
        log_list[0] = "sebi_settlementorder"
        log_list[1] = "Failure"
        log_list[4] = get_data_count(log_cursor)
        log_list[5] = "script error"
        print(log_list)
        insert_log_into_table(log_cursor, log_list)
        connection1.commit()
        log_list = [None] * 8
        traceback.print_exc()
        sys.exit("script error")




def get_number_of_new_data_in_excel(excel_file_path):
    global log_list
    try:
     
        df = pd.read_excel(excel_file_path)

     
        num_rows, num_columns = df.shape

        print(f"Number of rows: {num_rows}")
        print(f"Number of columns: {num_columns}")
        return num_rows
    except Exception as e:
        log_list[0] = "sebi_settlementorder"
        log_list[1] = "Failure"
        log_list[4] = get_data_count(log_cursor)
        log_list[5] = "script error"
        print(log_list)
        insert_log_into_table(log_cursor, log_list)
        connection1.commit()
        log_list = [None] * 8
        traceback.print_exc()
        sys.exit("script error")






def check_new_data(excel_file_path, cursor, type_sebi_text):
    global log_list
    global no_data_avaliable
       
    try:


        number_new_data = get_number_of_new_data_in_excel(excel_file_path)
        print(number_new_data,"number of total record in the website")



        table_name = "sebi_orders"
        query = f"SELECT COUNT(*) FROM {table_name} WHERE type_of_order = 'settlementorder';"
        cursor.execute(query)
        row_count = cursor.fetchone()[0]
        number_old_data = row_count

        no_data_avaliable = number_new_data - number_old_data

        print(no_data_avaliable,"number of new data")

        print(f"Number of data in database '{table_name}': {row_count}")

        if number_new_data == number_old_data:
            print("no new data in the website")
            log_list[0] = "sebi_settlementorder"
            log_list[1] = "Success"
            log_list[4] = get_data_count(log_cursor)
            log_list[6] = "no new data found"
            print(log_list)
            insert_log_into_table(log_cursor, log_list)
            connection1.commit()
            log_list = [None] * 8
            sys.exit("There is no new data found")
        else:
            find_new_data(excel_file_path, table_name, type_sebi_text)
   
    except Exception as e:
        log_list[0] = "sebi_settlementorder"
        log_list[1] = "Failure"
        log_list[4] = get_data_count(log_cursor)
        log_list[5] = "script error"
        print(log_list)
        insert_log_into_table(log_cursor, log_list)
        connection1.commit()
        log_list = [None] * 8
        traceback.print_exc()
        sys.exit("script error")







def extract_data_website(cursor):
    global log_list

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
        # type_sebi_text = type_sebi.get_attribute('innerText')
        type_sebi_text = "settlementorder"
        print(type_sebi_text)
        type_sebi.click()

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

        excel_file_name = f'sebi_data_all_pages_{type_sebi_text}{current_date}.xlsx'
        excel_file_path = rf"C:\Users\devadmin\sebi_final_script\so\first_set_excel_sheet_files\{excel_file_name}"
        df.to_excel(excel_file_path, index=False)
        print(f"Data from all pages saved to sebi_data_all_pages_{type_sebi_text}.xlsx")
        check_new_data(excel_file_path, cursor,type_sebi_text)
        browser.quit()


    except Exception as e:
        log_list[0] = "sebi_settlementorder"
        log_list[1] = "Failure"
        log_list[4] = get_data_count(log_cursor)
        log_list[5] = "404 error"
        print(log_list)
        insert_log_into_table(log_cursor, log_list)
        connection1.commit()
        log_list = [None] * 8
        traceback.print_exc()
        sys.exit("script error")
       
   



if sebi_config.source_status == "Active":
    extract_data_website(cursor)
    print("started")
elif sebi_config.source_status == "Hibernated":
    log_list[0] = "sebi_settlementorder"
    log_list[1] = "not run"
    log_list[4] = get_data_count(log_cursor)
    print(log_list)
    insert_log_into_table(log_cursor, log_list)
    connection1.commit()
    log_list = [None] * 8
    traceback.print_exc()
    sys.exit("script error")
elif sebi_config.source_status == "Inactive":
    log_list[0] = "sebi_settlementorder"
    log_list[1] = "not run"
    log_list[4] = get_data_count(log_cursor)
    print(log_list)
    insert_log_into_table(log_cursor, log_list)
    connection1.commit()
    log_list = [None] * 8
    traceback.print_exc()
    sys.exit("script error")







# final_excel_sheets_path = r"C:\Users\mohan.7482\Desktop\SEBI\file_name_excel_sheets\file_name_excel_sheet_Settlement Order2024-02-27.xlsx"



# insert_excel_data_to_mysql(final_excel_sheets_path, cursor)