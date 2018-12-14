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

print('connection established to MongoDB running on localhost:27017')

try:
    connection = pymysql.connect(host='localhost',user='root',password='',db='testdata')
except Exception as e:
    print(e)

print('connection established to MySQL running on localhost:3306')

cursor = connection.cursor()

sql = 'SELECT * FROM STATION '
countsql = 'SELECT COUNT(*) FROM STATION'

c = cursor.execute(countsql)
count = cursor.fetchone()[0]
print(count)
cursor.execute(sql)
# print(len(sql))
for i in range(0,count):

    testdata = list(cursor.fetchone())

    db = client.testdata

    collection = db.station
    db.station.insert_one(
        {"id": testdata[0],
         "name": testdata[1],
         "latitude": testdata[2],
         "longitude": testdata[3],
         "dock_count": testdata[4],
         "city": testdata[5],
         "installation_date": str(testdata[6])
         } 
    )

    # posts = db.station

    # post_id = posts.insert_one(post).inserted_id

    # print(post_id)

    print(testdata,'inserted . commit success. ')

end = datetime.datetime.now()



print(end-start)