import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from sqlalchemy_utils import database_exists, create_database
import glob
from bs4 import BeautifulSoup


uid = "postgres"
pwd = "110620"
postgres_url = f"postgresql+psycopg2://{uid}:{pwd}@localhost:5432/EXAM1"

if not database_exists(postgres_url):
    create_database(postgres_url)
    print("Database created")
else:
    print("Database exists")

engine = create_engine(postgres_url)
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
Table_name = "Top_Banks"
csv_path = "top_banks.csv"
df = pd.DataFrame(columns = ["Rank", "Bank Name", "Market Cap (US$ Billion)"])
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, "html.parser")

tables = data.find_all("tbody")
rows = tables[0].find_all("tr")

for row in rows:
    if count < 10:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {
                "Rank": col[0].contents[0],
                "Bank Name": col[1].contents[0],
                "Market Cap (US$ Billion)": col[2].contents[0]
            }
            df1 = pd.DataFrame (data_dict, index = [0])
            df = pd.concat([df,df1], ignore_index= True)
            count += 1

    else:
        break

# print(df)
df.to_sql(Table_name, engine, if_exists= "replace", index= False)
print("Successfull!")