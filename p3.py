import sys
import os
import datetime

start = datetime.datetime.now()

fhandle = open('status.csv')



while(True):
    l1 = list((fhandle.readline())[:-1].split(','))
    if(len(l1)>1):
        print(l1)
    else:
        break

end = datetime.datetime.now()

print(end-start)

