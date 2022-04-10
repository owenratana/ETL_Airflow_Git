from pymongo import MongoClient
import certifi
import pandas as pd
import numpy as np
import mysql.connector

CONNECTION_STRING = "mongodb+srv://owenratana:Owen61123427@test.qewsh.mongodb.net/classiccars?retryWrites=true&w=majority"

client = MongoClient(CONNECTION_STRING, tlsCAFile=certifi.where())

classiccars = client['classiccars']

products = classiccars['products']

productlines = classiccars['productlines']

product_list = products.find()
product_df = pd.DataFrame(product_list)

productline_list = productlines.find()
productline_df = pd.DataFrame(productline_list)

client.close()

merged_df = product_df.merge(productline_df, how='inner', on='productLine', suffixes=('','_y'))
merged_df.rename(columns={'textDescription':'productLineDescription'},inplace=True)

productdim_df = merged_df[['productCode','productName','productLine','productLineDescription','productScale','productVendor','productDescription','buyPrice']]
productdim_list = [tuple(r) for r in productdim_df.to_numpy()]

connectiondw = mysql.connector.connect(host='127.0.0.1',
                                    database='classicmodels_star',
                                    user='root',
                                    password='Owen61123427',
                                    port=3306)

cursordw = connectiondw.cursor(dictionary=True)

for productdim in productdim_list:
    cursordw.execute('INSERT INTO Product_Dim(productCode, productName,productLine,productLineDescription, \
productScale,productVendor,productDescription,buyPrice) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)', productdim)

connectiondw.commit()
connectiondw.close()