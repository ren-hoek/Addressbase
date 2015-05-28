import zipfile
import os.path
import os
import csv
import mysql.connector
import time
from mysql.connector.constants import ClientFlag

start_time = time.time()
fpath = "/Users/datascientistx/Data/Addressbase/Data/"
tpath = "/Users/datascientistx/Data/Addressbase/Temp/"

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

        head = []
        blpu = []
        clas = []
        dpa = []
        lpi = []
        org = []
        acr = []
        street = []
        strdes = []
        suc = []
        trail = [] 
        meta = []
        
        for row in address:
                
            for c in xrange(len(row)):
                if row[c].strip() == '' or row[c] == '':
                    row[c] = '\N'
            
            if row[0] == '10':
                head.append(row)
            if row[0] == '21':
                blpu.append(row)
            if row[0] == '32':
                clas.append(row)
            if row[0] == '28':
                dpa.append(row)
            if row[0] == '24':
                lpi.append(row)
            if row[0] == '31':
                org.append(row)
            if row[0] == '23':
                acr.append(row)
            if row[0] == '11':
                street.append(row)
            if row[0] == '15':
                strdes.append(row)
            if row[0] == '30':
                suc.append(row)
            if row[0] == '29':
                meta.append(row)
            if row[0] == '99':
                trail.append(row)

        os.remove(fpath + name)

        if head != []:
            hd_path = tpath + 'hd.csv'
            hd_csv = csv.writer(open(hd_path, 'wb', buffering=0), delimiter = ',')
            hd_csv.writerows(head)
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "hd.csv' " \
                    "INTO TABLE Header FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            os.remove(hd_path)
        if blpu != []:
            bu_path = tpath + 'bu.csv'
            bu_csv = csv.writer(open(bu_path, 'wb', buffering=0), delimiter = ',')
            bu_csv.writerows(blpu)
            sql = "ALTER TABLE BLPU DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "bu.csv' " \
                    "INTO TABLE BLPU FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE BLPU ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(bu_path)
        if clas != []:
            cs_path = tpath + 'cs.csv'
            cs_csv = csv.writer(open(cs_path, 'wb', buffering=0), delimiter = ',')
            cs_csv.writerows(clas)
            sql = "ALTER TABLE Classification DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "cs.csv' " \
                    "INTO TABLE Classification FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE Classification ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(cs_path)

        if dpa != []:
            da_path = tpath + 'da.csv'
            da_csv = csv.writer(open(da_path, 'wb', buffering=0), delimiter = ',')
            da_csv.writerows(dpa)
            sql = "ALTER TABLE Delivery_Point_Address DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "da.csv' " \
                    "INTO TABLE Delivery_Point_Address FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE Delivery_Point_Address ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(da_path)
        if lpi != []:
            li_path = tpath + 'li.csv'
            li_csv = csv.writer(open(li_path, 'wb', buffering=0), delimiter = ',')
            li_csv.writerows(lpi)
            sql = "ALTER TABLE LPI DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "li.csv' " \
                    "INTO TABLE LPI FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE LPI ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(li_path)
        if org != []:
            og_path = tpath + 'og.csv'
            og_csv = csv.writer(open(og_path, 'wb', buffering=0), delimiter = ',')
            og_csv.writerows(org)
            sql = "ALTER TABLE Organisation DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "og.csv' " \
                    "INTO TABLE Organisation FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE Organisation ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(og_path)
        if acr != []:
            ar_path = tpath + 'ar.csv'
            ar_csv = csv.writer(open(ar_path, 'wb', buffering=0), delimiter = ',')
            ar_csv.writerows(acr)
            sql = "ALTER TABLE Application_Cross_Reference DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "ar.csv' " \
                    "INTO TABLE Application_Cross_Reference FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE Application_Cross_Reference ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(ar_path)
        if street != []:
            st_path = tpath + 'st.csv'
            st_csv = csv.writer(open(st_path, 'wb', buffering=0), delimiter = ',')
            st_csv.writerows(street)
            sql = "ALTER TABLE Street DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "st.csv' " \
                    "INTO TABLE Street FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE Street ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(st_path)
        if strdes != []:
            ss_path = tpath + 'ss.csv'
            ss_csv = csv.writer(open(ss_path, 'wb', buffering=0), delimiter = ',')
            ss_csv.writerows(strdes)
            sql = "ALTER TABLE Street_Descriptor DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "ss.csv' " \
                    "INTO TABLE Street_Descriptor FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE Street_Descriptor ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(ss_path)
        if suc != []:
            sc_path = tpath + 'sc.csv'
            sc_csv = csv.writer(open(sc_path, 'wb', buffering=0), delimiter = ',')
            sc_csv.writerows(suc)
            sql = "ALTER TABLE Successor DISABLE KEYS;"
            cursor.execute(sql) 
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "sc.csv' " \
                    "INTO TABLE Successor FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            sql = "ALTER TABLE Successor ENABLE KEYS;"
            cursor.execute(sql) 
            os.remove(sc_path)
        if meta != []:
            ma_path = tpath + 'ma.csv'
            ma_csv = csv.writer(open(ma_path, 'wb', buffering=0), delimiter = ',')
            ma_csv.writerows(meta)
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "ma.csv' " \
                    "INTO TABLE Metadata FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            os.remove(ma_path)
        if trail != []:
            tl_path = tpath + 'tl.csv'
            tl_csv = csv.writer(open(tl_path, 'wb', buffering=0), delimiter = ',')
            tl_csv.writerows(trail)
            sql = "LOAD DATA INFILE " \
                    "'" + tpath + "tl.csv' " \
                    "INTO TABLE Trailer FIELDS TERMINATED BY ',';"
            print sql
            cursor.execute(sql)
            os.remove(tl_path)
        
        cnx.commit()


        time_taken = time.time() - start_time
        print time_taken
