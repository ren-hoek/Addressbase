import zipfile
import os.path
import os
import csv
import mysql.connector
import time
from mysql.connector.constants import ClientFlag

fpath = "/Users/datascientistx/Data/Addressbase/Data/"
tpath = "/Users/datascientistx/Data/Addressbase/Temp/"

table_name = raw_input("Name of Addressbase table to import: ")

codes = { 
        'Header': '10', 'BLPU': '21', 'Classification': '32',
        'Delivery_Point_Address': '28', 'LPI': '24', 'Organisation': '31',
        'Application_Cross_Reference': '23', 'Street': '11',
        'Street_Descriptor': '15', 'Successor': '30', 'Metadata': '29',
        'Trailer': '99'}

cnx = mysql.connector.connect(
        user = 'root'
        ,password = '*'
        ,host = 'localhost'
        ,database = 'Addressbase'
        ,client_flags=[ClientFlag.LOCAL_FILES])

cursor = cnx.cursor()

for lfile in os.listdir(fpath):
    
    start_time = time.time()
    path = fpath + lfile
    zfile = zipfile.ZipFile(path)
    dirname, filename = os.path.split(path)
    
    for name in zfile.namelist():

        print "Decompressing " + filename + " on " + dirname
        zfile.extract(name,dirname)
        address = csv.reader(open(fpath + name, 'rb'))
    
        i = 0 
        j = 0

        temp = []
        
        for row in address:
                
            for c in xrange(len(row)):
                if row[c].strip() == '' or row[c] == '':
                    row[c] = '\N'
            if row[0] == '30':
                i = 1

            if row[0] == codes[table_name]:
                temp.append(row)

        os.remove(fpath + name)

        if  temp != []:
            temp_path = tpath + 'temp.csv'
            temp_csv = csv.writer(open(temp_path, 'wb', buffering=0), delimiter = ',')
            temp_csv.writerows(temp)
            sql = "ALTER TABLE " + table_name + "  DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "temp.csv' " \
                    "INTO TABLE " + table_name + " FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE " + table_name + " ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(temp_path)
        
        cnx.commit()

        time_taken = time.time() - start_time
        print time_taken
        print i
