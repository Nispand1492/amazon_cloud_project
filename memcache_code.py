__author__ = 'Nispand'
import random
import memcache
import hashlib
import time
import MySQLdb
from random_query_generator import setquery

def connect_to_database():
    cnx= {
        'host': 'nisudb.crsevmpdcmrk.us-west-2.rds.amazonaws.com',
        'username': 'nispand',
        'password': 'nispand1492',
        'db': 'assignment4'
        }
    try:
        db = MySQLdb.connect(host=cnx['host'],user=cnx['username'],passwd= cnx['password'],db= cnx['db'])
        cursr = db.cursor()
        query = "Use assignment4"
        cursr.execute(query)
        print "Connected to Schema"
        return cursr
    except Exception as e:
        return "Unable to connect::" + str(e)
try:
    s = memcache.Client(["nispand.6czzrp.cfg.usw2.cache.amazonaws.com:11211"])
    print "Connected to Elastic Cache"
except Exception as e:
    print "Cannot connect::"+e


try:
    result = connect_to_database()
    print "Connected to database"
except Exception as e:
    print "Connot connect::"+e

raw_input("Enter key to Execute 1000 queries")

start_time_1000 = time.time()
try:
    print "At first line"
    qrytoexec = setquery(1000)
    for i in range(0,len(qrytoexec)):
        print qrytoexec[i]
        hash_obj = hashlib.md5(qrytoexec[i])
        print "Hash Obj Created"
        hash_obj = hash_obj.hexdigest()

        data = result.execute(qrytoexec[i])
        data = result.fetchall()
        s.set(hash_obj,data)
        #data = result.execute(qrytoexec[i])
        """ for row in data:
        print row"""
except Exception as e:
    print "No result for this one" + str(e) 

end_time = time.time()-start_time_1000
print "time taken to execute 1000 queries::" + str(end_time)

raw_input("Enter key to Execute 5000 queries")
start_time_5000 = time.time()
try:
    qrytoexec5 = setquery(5000)
    for i in range(0,len(qrytoexec5)):
        qrytoexec5[i] = qrytoexec5[i]
        hash_obj = hashlib.md5(qrytoexec5[i].encode())
        hash_obj = hash_obj.hexdigest()
        data = result.execute(qrytoexec5[i])
        data = result.fetchall()
        s.set(hash_obj,data)
        """ for row in data:
            print row"""

except Exception as e:
        print "No result for this one"


end_time = time.time()-start_time_5000
print "time taken to execute 5000 queries::" + str(end_time)

raw_input("Enter key to Execute 20000 queries")
start_time_20000 = time.time()
try:
    qrytoexec20 = setquery(20000)
    for i in range(0,len(qrytoexec20)):
        hash_obj = hashlib.md5(qrytoexec20[i].encode())
        hash_obj = hash_obj.hexdigest()
        data = result.execute(qrytoexec20[i])
        data = result.fetchall()
        s.set(hash_obj,data)
        """ for row in data:
            print row"""
except Exception as e:
    print "No result for this one"

end_time = time.time()-start_time_20000
print "time taken to execute 20000 queries::" + str(end_time)
