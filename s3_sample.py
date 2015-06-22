# Copyright 2013. Amazon Web Services, Inc. All Rights Reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Import the SDK
import boto
import uuid
import time
import urllib
import csv
import sys
import os
import MySQLdb
import random
import memcache
from random_query_generator import setquery
# Instantiate a new client for Amazon Simple Storage Service (S3). With no
# parameters or configuration, the AWS SDK for Python (Boto) will look for
# access keys in these environment variables:
#
#    AWS_ACCESS_KEY_ID='...'
#    AWS_SECRET_ACCESS_KEY='...'
#
# For more information about this interface to Amazon S3, see:
# http://boto.readthedocs.org/en/latest/s3_tut.html

# Referenced from :: http://stackoverflow.com/questions/20749599/connecting-to-mysql-db-on-amazon-rds
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

def create_table(cursr):
        query = "CREATE TABLE IF NOT EXISTS mytable(Lang VARCHAR(7) NOT NULL, segment_id INTEGER(6) NOT NULL PRIMARY KEY" \
                ", contract_id VARCHAR(5) NOT NULL, plan_id INTEGER(3) NOT NULL, contract_year INTEGER(4) NOT NULL" \
                ", tier_level INTEGER(3) NOT NULL, tier_type_desc VARCHAR(22) NOT NULL, sentences_sort_order INTEGER(4) NOT NULL" \
                ", category_code INTEGER(2) NOT NULL)"
        cursr.execute(query)
        print "Table Created Successfully"
        #query = "DROP TABLE test"
        #cursr.execute(query)
        #print "Table dropped successfull"
        return  "Connection Successfull"

def insert_in_table(crsr):
    start_time_dbi = time.time()
    with open('nispand.csv','r') as csvfile:
        csv_data = csv.reader(csvfile,delimiter=',',quotechar = '"')
        for row in csv_data:
            print "Inserting into table"
            query = "INSERT INTO mytable(Lang,segment_id,contract_id,plan_id,contract_year,tier_level,tier_type_desc,sentences_sort_order,category_code) VALUES ('"+str(row[0])+"',"+row[1]+",'"+row[2]+"',"+row[3]+","+row[4]+","+row[5]+",'"+row[6]+"',"+row[7]+","+row[8]+");"
        print query
        try:
            crsr.execute(query)
        except Exception as e:
            print "Cannot Insert"

    time_to_ins = time.time() - start_time_dbi
    print "Time takne to insert data into Database..." + str(time_to_ins) + "seconds"
    return "Data Inserted Successfully"

def get_file():
    
    testfile = urllib.URLopener()
    testfile.retrieve("https://dl.dropboxusercontent.com/s/j9102nm9laul8to/data_assg3.csv?dl=0","nispand.csv")

    myfile = open("nispand.csv",'rt')
    try:
        reader = csv.reader(myfile)
    except Exception as e :
        print "Cannot read file" + e

    print reader

    return "file downloaded successfully"

s3 = boto.connect_s3()
s = memcache.Client(["nispand.6czzrp.cfg.usw2.cache.amazonaws.com:11211"])
# Everything uploaded to Amazon S3 must belong to a bucket. These buckets are
# in the global namespace, and must have a unique name.
#
# For more information about bucket name restrictions, see:
# http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
bucket_name = "assignment4bucket1"
print "Creating new bucket with name: " + bucket_name
bucket = s3.create_bucket(bucket_name)

# Files in Amazon S3 are called "objects" and are stored in buckets. A specific
# object is referred to by its key (i.e., name) and holds data. Here, we create
# a new object with the key "python_sample_key.txt" and content "Hello World!".
#
# For more information on keys and set_contents_from_string, see:
# http://boto.readthedocs.org/en/latest/s3_tut.html#storing-data
from boto.s3.key import Key
k = Key(bucket)
k.key = 'sample_data.csv'
contents = get_file()
start_time = time.time()
print "Uploading some data to " + bucket_name + " with key: " + k.key
k.set_contents_from_filename('nispand.csv')
time_taken = time.time()-start_time
print "time taken to upload data in bucket " + str(time_taken) +" seconds"
# Fetch the key to show that we stored something. Key.generate_url will
# construct a URL that can be used to access the object for a limited time.
# Here, we set it to expire in 30 minutes.
#
# For a more detailed overview of generate_url's options, see:
# http://boto.readthedocs.org/en/latest/ref/s3.html#boto.s3.key.Key.generate_url
expires_in_seconds = 1800


print "Generating a public URL for the object we just uploaded. This URL will be active for %d seconds" % expires_in_seconds
print
print k.generate_url(expires_in_seconds)
print

raw_input("Press enter to connect to MySQLdb...")
result = connect_to_database()
ans = create_table(result)
print ans
qry = insert_in_table(result)
print qry
raw_input("Enter a key to delete the bucket")
# Buckets cannot be deleted unless they're empty. Since we still have a
# reference to the key (object), we can just delete it.
print "Deleting the object."
k.delete()
result.execute("commit")
lim = random.randint(200,800)
raw_input("Enter key to Execute 1000 queries")
start_time_1000 = time.time()
try:
    qrytoexec = setquery(1000)
    for i in range(0,len(qrytoexec)):
        qrytoexec[i] = qrytoexec[i] + str(lim)
        data = result.execute(qrytoexec[i])
        """ for row in data:
        print row"""
except Exception as e:
    print "No result for this one"

end_time = time.time()-start_time_1000
print "time taken to execute 1000 queries::" + str(end_time)

raw_input("Enter key to Execute 5000 queries")
start_time_5000 = time.time()
try:
    qrytoexec5 = setquery(5000)
    for i in range(0,len(qrytoexec)):
        qrytoexec5[i] = qrytoexec5[i] + str(lim)
        data = result.execute(qrytoexec5[i])
        """ for row in data:
            print row"""

except Exception as e:
        print "No result for this one"


end_time = time.time()-start_time_5000
print "time taken to execute 5000 queries::" + str(end_time)

raw_input("Enter key to Execute 20000 queries")
start_time_20000 = time.time()
try:
    qrytoexec20 = setquery(1000)
    for i in range(0,len(qrytoexec20)):
        qrytoexec20[i] = qrytoexec20[i] + str(lim)
        data = result.execute(qrytoexec20[i])
        """ for row in data:
            print row"""
except Exception as e:
    print "No result for this one"

end_time = time.time()-start_time_20000
print "time taken to execute 20000 queries::" + str(end_time)

# Now that the bucket is empty, we can delete it.
print "Deleting the bucket."
s3.delete_bucket(bucket_name)



