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

cursordb.execute("select DISTINCT(orderDate) as theDate, \
                    DAY(orderDate) as day, \
                    MONTH(orderDate) as month, \
                    YEAR(orderDate) as year, \
                    CASE \
                        WHEN MONTH(orderDate) <= 3 THEN 1 \
                        WHEN MONTH(orderDate) >=4 AND MONTH(orderDate) <= 6 THEN 2 \
                        WHEN MONTH(orderDate) >=7 AND MONTH(orderDate) <= 9 THEN 3 \
                        ELSE 4 \
                    END AS quarter, \
                    ELT(DAYOFWEEK(orderDate), 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday') as dayOfWeek \
                    FROM orders")

dates = cursordb.fetchall()
dates_df = pd.DataFrame(dates)

dates_list = [tuple(r) for r in dates_df.to_numpy()]
#insert dates into DW
for date in dates_list:
    insert_date_query = "INSERT INTO Date_Dim(theDate, day, month, year, quarter, dayOfWeek) \
                        VALUES(%s,%s,%s,%s,%s,%s)"
    cursordw.execute(insert_date_query, date)

connectiondw.commit()

connectiondb.close()
connectiondw.close()