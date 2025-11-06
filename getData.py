from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
import os
from sqlalchemy import create_engine

#url_df = f'https://pitwall.app/seasons?view=list'
#matches = pd.read_html(url_df)[0]
#print(matches)

#table_name = 'f1_season_summary'

mysql_params = {
    'host': 'localhost',
    'user': 'domains',
    'password':'%tEBCt7pSSBo9ScAjBc!z!nL',
    'database': 'soccer_portal'
}

connection = mysql.connector.connect(**mysql_params)
if connection.is_connected():
    print('Connected to MySQL database')
else:
    print('Connection failed')

'''
engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
table_name = 'f1_season_summary'
chunksize = 1000
for i in range(0, len(matches), chunksize):
    chunk = matches.iloc[i:i+chunksize]
    chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
connection.close()
print("Season summary Data inserted successfully into MySQL table")

connection = mysql.connector.connect(host='localhost', user='domains', password='%tEBCt7pSSBo9ScAjBc!z!nL', database='soccer_portal')
cur = connection.cursor()
cur.execute(f'select Season from f1_season_summary;')
rows = cur.fetchall()
for row in rows:
    driver_standings = ""
    constructor_standings = ""
    race_schedule = ""

    url = f'https://pitwall.app/seasons/{row[0]}-formula-1-world-championship'
    print(url)

    driver_standings = pd.read_html(url)[0]
    print(driver_standings)
    engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
    table_name = f"{row[0]}_driver_standings"
    chunksize = 1000
    for i in range(0, len(driver_standings), chunksize):
        chunk = driver_standings.iloc[i:i+chunksize]
        chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    constructor_standings = pd.read_html(url)[1]
    print(constructor_standings)
    engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
    table_name = f"{row[0]}_constructor_standings"
    chunksize = 1000
    for i in range(0, len(constructor_standings), chunksize):
        chunk = constructor_standings.iloc[i:i+chunksize]
        chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    race_schedule = pd.read_html(url)[2]
    print(race_schedule)
    engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
    table_name = f"{row[0]}_race_schedule"
    chunksize = 1000
    for i in range(0, len(race_schedule), chunksize):
        chunk = race_schedule.iloc[i:i+chunksize]
        chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

'''



connection = mysql.connector.connect(host='localhost', user='domains', password='%tEBCt7pSSBo9ScAjBc!z!nL', database='soccer_portal')
cur = connection.cursor()


#cur.execute(f'select Season from f1_season_summary;')
#rows = cur.fetchall()
rows = ['2024']
for row in rows:
    if(int(row[0]) <= 2006):
        cur.execute(f'select race from {row[0]}_race_schedule;')
        rows1 = cur.fetchall()
        for r in rows1:
            r = str(r[0]).replace(" ","-")
            if(r[0] == 'SÃ£o Paulo Grand Prix'):
                r = 'sao-paulo-grand-prix'
            r = r.lower()
            url1 = f"https://pitwall.app/races/{row[0]}-{r}"
            print(url1)
            race = pd.read_html(url1)[0]
            print(race)
            engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
            r = r.replace("-","_")
            table_name = f"{row[0]}_{r}_results"
            chunksize = 1000
            for i in range(0, len(race), chunksize):
                chunk = race.iloc[i:i+chunksize]
                chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        
            if(int(row[0]) >= 2006):
                race_qual = pd.read_html(url1)[1]
                print(race_qual)
                engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
                r = r.replace("-","_")
                table_name = f"{row[0]}_{r}_qualifying_results"
                chunksize = 1000
                for i in range(0, len(race_qual), chunksize):
                    chunk = race_qual.iloc[i:i+chunksize]
                    chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

                race_prac3 = pd.read_html(url1)[2]
                print(race_prac3)
                engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
                r = r.replace("-","_")
                table_name = f"{row[0]}_{r}_practice_3_results"
                chunksize = 1000
                for i in range(0, len(race_prac3), chunksize):
                    chunk = race_prac3.iloc[i:i+chunksize]
                    chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

                race_prac2 = pd.read_html(url1)[3]
                print(race_prac2)
                engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
                r = r.replace("-","_")
                table_name = f"{row[0]}_{r}_practice_2_results"
                chunksize = 1000
                for i in range(0, len(race_prac2), chunksize):
                    chunk = race_prac2.iloc[i:i+chunksize]
                    chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

                race_prac1 = pd.read_html(url1)[4]
                print(race_prac1)
                engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
                r = r.replace("-","_")
                table_name = f"{row[0]}_{r}_practice_1_results"
                chunksize = 1000
                for i in range(0, len(race_prac1), chunksize):
                    chunk = race_prac1.iloc[i:i+chunksize]
                    chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)


            elif(int(row[0]) >= 2002):
                race_qual = pd.read_html(url1)[1]
                print(race_qual)
                engine = create_engine('mysql+mysqlconnector://domains:%tEBCt7pSSBo9ScAjBc!z!nL@localhost/soccer_portal')
                r = r.replace("-","_")
                table_name = f"{row[0]}_{r}_qualifying_results"
                chunksize = 1000
                for i in range(0, len(race_qual), chunksize):
                    chunk = race_qual.iloc[i:i+chunksize]
                    chunk.to_sql(name=table_name, con=engine, if_exists='replace', index=False)


connection.close()











#SELECT * FROM ( SELECT * FROM 2021_united_states_grand_prix_qualifying_results UNION ALL SELECT * FROM 2021_united_states_grand_prix_results UNION ALL SELECT * FROM 2022_abu_dhabi_grand_prix_practice_1_results) AS merged_tables;



