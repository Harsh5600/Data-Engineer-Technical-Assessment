#Harsh Mishra
#Rollno :19

import mysql.connector

conn = mysql.connector.connect(
   user='root', database='db'
)

# creating a dictionary with the data corresponding to the country
country = {}
with open('data.txt','r') as f:
    for line in f:
        splittedData = line.split("|")
        if splittedData[1] == 'D':
            correnspondingCountry = splittedData[8]
            if correnspondingCountry not in country:
                country[correnspondingCountry] = [splittedData[1:]]
            else:
                country[correnspondingCountry].append(splittedData[1:])

cursor = conn.cursor()

# sql query for creating the country tables
for cname in country.keys():
    sqlQuery = """
                CREATE TABLE COUNTRY_{}(
                CUSTOMER_NAME VARCHAR(255) NOT NULL PRIMARY KEY,
                CUSTOMER_ID VARCHAR(18) NOT NULL,
                CUSTOMER_OPEN_DATE DATE FORMAT 'YYYYMMDD' NOT NULL,
                LAST_CONSULTED_DATE DATE FORMAT 'YYYYMMDD',
                VACCINATION_TYPE CHAR(5),
                DOCTOR_CONSULTED CHAR(255),
                STATE CHAR(5),
                COUNTRY CHAR(5),
                POST_CODE INT,
                DATE_OF_BIRTH DATE FORMAT 'YYYYMMDD',
                ACTIVE_CUSTOMER CHAR(1)
            )
    """.format(cname)
    cursor.execute(sqlQuery)

# insert country wise data in the table
for cname in country.keys():
    for data in country[cname]:
        sqlQuery = """
                INSERT INTO COUNTRY_{}(
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
                )
        """.format(cname, data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10][0])
        cursor.execute(sqlQuery)
