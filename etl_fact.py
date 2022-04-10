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


cursordw.execute("SELECT DateKey, CustomerKey, ProductKey, \
                dbO.orderNumber, dbOD.quantityOrdered as quantity, dbO.shippedDate, dbOD.priceEach as unitPrice, \
                dbP.buyPrice, dbOD.quantityOrdered*dbOD.priceEach as totalPrice, dbOD.quantityOrdered*dbP.buyPrice as totalBuyPrice, \
                ((dbOD.quantityOrdered*dbOD.priceEach)-(dbOD.quantityOrdered*dbP.buyPrice)) as totalProfit \
                 FROM classicmodels.orderdetails AS dbOD \
                 JOIN classicmodels.orders AS dbO ON dbOD.orderNumber = dbO.orderNumber \
                 JOIN classicmodels.customers AS dbC ON dbO.customerNumber = dbC.customerNumber \
                 JOIN classicmodels.products AS dbP ON dbOD.productCode = dbP.productCode \
                 JOIN classicmodels_star.Customer_Dim as dwC ON dbC.customerNumber = dwC.customerNumber \
                 JOIN classicmodels_star.Product_Dim as dwP ON dbOD.productCode = dwP.productCode \
                 JOIN classicmodels_star.Date_Dim as dwD ON dbO.orderDate = dwD.theDate")

res = cursordw.fetchall()
res_df = pd.DataFrame(res)

res_df.drop(res_df[res_df['shippedDate'].isna()].index, inplace=True)
res_df = res_df.drop(columns=['shippedDate'])

res_list = [tuple(r) for r in res_df.to_numpy()]

for fact in res_list:
    insert_fact_query = "INSERT INTO CarOrderDetails_Fact(DateKey,CustomerKey,ProductKey,orderNumber, \
    quantity,unitPrice,buyPrice,totalPrice,totalBuyPrice,totalProfit) \
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursordw.execute(insert_fact_query, fact)

connectiondw.commit()

connectiondw.close()
connectiondb.close()
