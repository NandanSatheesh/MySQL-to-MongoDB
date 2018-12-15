import csv
import pymysql.cursors
import datetime
import time



try:
    log = open('log_status.txt','w')
except Exception as e:
    print(e)
    print('unable to open the log file')

logentry = 'script started '+str(datetime.datetime.now())+'\n'
# log.write(logentry)
print(logentry)


start = time.time()

try:
    fhandle = open('status.csv')
except Exception as e:
    print(e)
    print('unable to open the CSV file')
    # log.write(str(e))
    # log.write('\n')
    logentry = 'unable to open the CSV file. error. \n'
    # log.write(logentry)
    print(logentry)
    logentry = 'unsuccessful termination on ' + str(datetime.datetime.now()) + '\n'
    # log.write(logentry)
    print(logentry)
    exit()


logentry = 'CSV file found. Reading the file any moment.\n'
#log.write(logentry)
print(logentry)

try:
    conn = pymysql.connect(host='localhost',user='root',password='',db='testdata')
    cursor = conn.cursor()
except Exception as e:
    print(e)
    print('unable to connect to the database')
    # log.write(str(e))
    # log.write('\n')
    logentry = 'unable to connect to the MySQL database. \n'
    # log.write(logentry)
    logentry = 'unsuccessful termination on ' + str(datetime.datetime.now()) + '\n'
    # log.write(logentry)
    print(logentry)
    exit()

logentry = 'connection established to MySQL database. success. \n'
# log.write(logentry)
print(logentry)



fhandle.readline()


k=1

while(True):
    teststr = fhandle.readline()
    if(teststr==''):
        break

    testlist = list(teststr[:-1].split(','))
    print(testlist)
    if(len(testlist) == 0):
        break

    print(k)
    k=k+1
    try:
        sql = "INSERT INTO STATUS VALUES(%d,%d,%d,'%s')"%(int(testlist[0]) , int(testlist[1]) , int(testlist[2]) , testlist[3])

        cursor.execute(sql)
        conn.commit()
        logentry = sql+' executed. commit success. \n'
        # log.write(logentry)
        print(logentry)

    except Exception as e:
        logentry = sql + '  was not executed. no commits made. \n'
        # log.write(logentry)
        print(logentry)
        # log.write(str(e))
        log.write('\n')
        print(str(e))

cursor.close()

fhandle.close()
end = time.time()

exectime = 'script took '+str(end-start)+' to execute.\n'
end = 'script terminated on '+str(datetime.datetime.now())+'\n'
# log.write(exectime)
# log.write(end)
print(exectime)
print(end)

