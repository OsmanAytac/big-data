import pandas as pd
# import chardet
from pymongo import MongoClient
import pymongo

# file = 'clean_review.csv'
# with open(file, 'rb') as rawdata:
#     result = chardet.detect(rawdata.read(100000))
# print(result)

df = pd.read_csv('clean_review.csv')
client = MongoClient("mongodb://localhost:27017/",
                    username='root',
                    password='root')
db = client.test
db.Hotel_Reviews.insert_many(df.to_dict('records'))