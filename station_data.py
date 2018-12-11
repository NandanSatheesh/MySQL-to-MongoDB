import csv
import pymysql.cursors
import datetime
import time



try:
    log = open('log_station.txt','w')
except Exception as e:
    print(e)
    print('unable to open the log file')

logentry = 'script started '+str(datetime.datetime.now())+'\n'
log.write(logentry)

start = time.time()

try:
    fhandle = open('station.csv')
except Exception as e:
    print(e)
    print('unable to open the CSV file')
    log.write(str(e))
    log.write('\n')
    logentry = 'unable to open the CSV file. error. \n'
    log.write(logentry)
    logentry = 'unsuccessful termination on ' + str(datetime.datetime.now()) + '\n'
    log.write(logentry)
    exit()


logentry = 'CSV file found. Reading the file any moment.\n'
log.write(logentry)


try:
    conn = pymysql.connect(host='localhost',user='root',password='',db='testdata')
    cursor = conn.cursor()
except Exception as e:
    print(e)
    print('unable to connect to the database')
    log.write(str(e))
    log.write('\n')
    logentry = 'unable to connect to the MySQL database. \n'
    log.write(logentry)
    logentry = 'unsuccessful termination on ' + str(datetime.datetime.now()) + '\n'
    log.write(logentry)
    exit()

logentry = 'connection established to MySQL database. success. \n'
log.write(logentry)

fhandle.readline()


while True:

    teststr = fhandle.readline()
    if(teststr==''):
        break

    testlist = list(teststr[:-1].split(','))
    print(testlist)
    if(len(testlist) == 0):
        break


    try:
        sql = "INSERT INTO STATION VALUES(%d,'%s',%f,%f,%d,'%s','%s')"%(int(testlist[0]) , testlist[1] , float(testlist[2]) , float(testlist[3]) , int(testlist[4]) , testlist[5] ,datetime.datetime.strptime(testlist[6], "%m/%d/%Y").strftime("%Y-%m-%d"))


        cursor.execute(sql)
        conn.commit()
        logentry = sql+' executed. commit success. \n'
        log.write(logentry)

    except Exception as e:
        logentry = sql + '  was not executed. no commits made. \n'
        log.write(logentry)
        log.write(str(e))
        log.write('\n')


cursor.close()

fhandle.close()
end = time.time()

exectime = 'script took '+str(end-start)+' to execute.\n'
end = 'script terminated on '+str(datetime.datetime.now())+'\n'
log.write(exectime)
log.write(end)


db.station.insert_one(
    {       "id" : testdata[0] ,
            "name" : testdata[1] ,
            "latitude" : testdata[2] ,
            "longitude" : testdata[3] ,
            "dock_count" : testdata[4],
            "city" : testdata[5] ,
            "installation_date" : testdata[6]
    }
)

