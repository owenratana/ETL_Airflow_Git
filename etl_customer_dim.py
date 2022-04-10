import pandas as pd
from ast import literal_eval
import numpy as np
import mysql.connector

connectiondb = mysql.connector.connect(host='127.0.0.1',
                                    database='classicmodels',
                                    user='root',
                                    password='Owen61123427',
                                    port=3306)

connectiondw = mysql.connector.connect(host='127.0.0.1',
                                    database='classicmodels_star',
                                    user='root',
                                    password='Owen61123427',
                                    port=3306)

cursordb = connectiondb.cursor(dictionary=True)
cursordw = connectiondw.cursor(dictionary=True)

cursordb.execute("SELECT customerNumber, \
                    customerName, \
                    contactLastName, \
                    contactFirstName, \
                    phone, \
                    city, \
                    state, \
                    postalCode, \
                    country \
                    FROM customers")

customers = cursordb.fetchall()
customers_df = pd.DataFrame(customers)
customers_list = [tuple(r) for r in customers_df.to_numpy()]

for customer in customers_list:
    insert_customer_query = "INSERT INTO Customer_Dim(customerNumber,customerName,contactLastName, \
        contactFirstName,phone,city,state,postalCode,country) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursordw.execute(insert_customer_query, customer)

connectiondw.commit()


connectiondb.close()
connectiondw.close()