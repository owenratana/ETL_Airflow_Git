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

cursordw.execute('DELETE FROM CarOrderDetails_Fact')
cursordw.execute('DELETE FROM Date_Dim')
cursordw.execute('DELETE FROM Customer_Dim')
cursordw.execute('DELETE FROM Product_Dim')
connectiondw.commit()

cursordw.execute('ALTER TABLE Date_Dim AUTO_INCREMENT = 1')
cursordw.execute('ALTER TABLE Customer_Dim AUTO_INCREMENT = 1')
cursordw.execute('ALTER TABLE Product_Dim AUTO_INCREMENT = 1')
connectiondw.commit()

connectiondb.close()
connectiondw.close()