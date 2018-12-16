import pymysql.cursors
import datetime
import time
from pymongo import MongoClient


start = datetime.datetime.now()
try:
    client = MongoClient()
    client = MongoClient('localhost',27017)

except Exception as e:
    print(e)

print('connected to MongoDB running on localhost:27017')

try:
    connection = pymysql.connect(host='localhost',user='root',password='',db='testdata')
except Exception as e:
    print(e)

print('connection established to MySQL running on localhost:3306')

cursor = connection.cursor()

sql = 'SELECT * FROM STATUS '
countsql = 'SELECT COUNT(*) FROM STATUS'

c = cursor.execute(countsql)
count = cursor.fetchone()[0]
print(count)

cursor.execute(sql)

for i in range(0,count):

    testdata = list(cursor.fetchone())

    db = client.testdata

    collection = db.status
    db.status.insert_one(
         {
            "station_id": testdata[0],
            "bikes_available": testdata[1],
            "docks_available": testdata[2],
            "time": testdata[3]

         }
    )

    # posts = db.station

    # post_id = posts.insert_one(post).inserted_id

    # print(post_id)

    print('inserted',i)
    db.commit

end = datetime.datetime.now()

print(end-start)