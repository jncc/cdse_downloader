import json
import csv
import sqlite3
import re
import os, stat

from datetime import datetime

class ReportGenerator:

    def __init__(self, dbConnectionTimeout):
        self.dbConnectionTimeout = dbConnectionTimeout

    def getS2productData(self, product):
        pattern = re.compile("S2([AB])_MSIL1C_((20[0-9]{2})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2}))_\w+_(R[0-9]{3})_(T\w+)_")

        productId = product["productID"]
        
        m = pattern.search(productId)

        platform = "2%s" % (m.group(1))
        captureDate = "%s-%s-%s" % (m.group(3), m.group(4), m.group(5)) 
        captureTime = "%s:%s:%s" % (m.group(6), m.group(7), m.group(8)) 

        if "productPath" in product:
            path = product["productPath"]
        else:
            path = ""

        if "onlineStatus" in product:
            status = product["onlineStatus"]
        elif "downloadResult" in product and product["downloadResult"]:
            status = "DOWNLOADED"

        if "localPath" in product:
            localPath = product["localPath"]
        else:
            localPath = ""

        relOrbit = m.group(9)
        tileNumber = m.group(10)

        return [productId, platform, relOrbit, tileNumber, captureDate, captureTime, path, status, localPath]

    def writeS2ToCsv(self, outputPath, rows):
        with open(outputPath, 'w', newline='') as csvFile: 
            writer = csv.writer(csvFile)
            writer.writerow(["ProductId", "Platform", "Relative Orbit", "Tile Number", "Capture Date", "Capture Time", "Bucket Path", "Status", "Local Path"])

            for row in rows:
                writer.writerow(row)

    def writeS2ToDb(self, dbPath, rows):
        conn = sqlite3.connect(dbPath, timeout=self.dbConnectionTimeout)

        c = conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='s2RawProducts' ''')

        if c.fetchone()[0] != 1: 
            c.execute('''CREATE TABLE s2RawProducts
                        (productId text, platform text, relativeOrbit text, tileNumber text, captureDate text, captureTime text, bucketPath text, onlineStatus text, localPath text, recordTimestamp text)''')
            
            conn.commit()

        sql = "INSERT INTO s2RawProducts VALUES (?,?,?,?,?,?,?,?,?,?)"

        for row in rows:
            recordTimestamp = str(datetime.now())
            row = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], recordTimestamp)

            c.execute(sql, row)

        conn.commit()
        conn.close()

    def makeS2Report(self, data, outputPath, dbPath):
        
        rows = []
        for p in data["productList"]:
            rows.append(self.getS2productData(p))

        if dbPath:
            dbExists = os.path.exists(dbPath)

            self.writeS2ToDb(dbPath, rows)

            #If the file has just been created make it user and group writable.
            if not dbExists:
                os.chmod(dbPath, stat.S_IREAD | stat.S_IWRITE | stat.S_IRGRP | stat.S_IWGRP)

        self.writeS2ToCsv(outputPath, rows)


    def getS1productData(self, product):
        pattern = re.compile("S1([AB])_IW_\w+_1SDV_((20[0-9]{2})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2}))")

        productId = product["productID"]
        
        m = pattern.search(productId)

        platform = "1%s" % (m.group(1))
        captureDate = "%s-%s-%s" % (m.group(3), m.group(4), m.group(5)) 
        captureTime = "%s:%s:%s" % (m.group(6), m.group(7), m.group(8)) 

        if "productPath" in product:
            path = product["productPath"]
        else:
            path = ""

        if "onlineStatus" in product:
            status = product["onlineStatus"]
        elif "downloadResult" in product and product["downloadResult"]:
            status = "DOWNLOADED"

        if "localPath" in product:
            localPath = product["localPath"]
        else:
            localPath = ""

        return [productId, platform, captureDate, captureTime, path, status, localPath]

    def writeS1ToCsv(self, outputPath, rows):
        with open(outputPath, 'w', newline='') as csvFile: 
            writer = csv.writer(csvFile)
            writer.writerow(["ProductId", "Platform", "Capture Date", "Capture Time", "Bucket Path", "Status", "Local Path"])

            for row in rows:
                writer.writerow(row)

    def writeS1ToDb(self, dbPath, rows):
        conn = sqlite3.connect(dbPath, timeout=self.dbConnectionTimeout)

        c = conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='s1RawProducts' ''')

        if c.fetchone()[0] != 1: 
            c.execute('''CREATE TABLE s1RawProducts
                        (productId text, platform text, captureDate text, captureTime text, bucketPath text, onlineStatus text, localPath text, recordTimestamp text)''')
            
            conn.commit()

        sql = "INSERT INTO s1RawProducts VALUES (?,?,?,?,?,?,?,?)"

        for row in rows:
            recordTimestamp = str(datetime.now())
            row = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], recordTimestamp)

            c.execute(sql, row)

        conn.commit()
        conn.close()

    def makeS1Report(self, data, outputPath, dbPath):
        pattern = re.compile("S1([AB])_IW_GRDH_1SDV_((20[0-9]{2})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2}))")
        
        rows = []
        for p in data["productList"]:
            rows.append(self.getS1productData(p))

        if dbPath:
            dbExists = os.path.exists(dbPath)

            self.writeS1ToDb(dbPath, rows)

            #If the file has just been created make it user and group writable.
            if not dbExists:
                os.chmod(dbPath, stat.S_IREAD | stat.S_IWRITE | stat.S_IRGRP | stat.S_IWGRP)

        self.writeS1ToCsv(outputPath, rows)


    def makeReport(self, platform, data, outputPath, dbPath):
        if platform.lower() == "sentinel1":
            self.makeS1Report(data, outputPath, dbPath)
            
        elif platform.lower() == "sentinel2":
            self.makeS2Report(data, outputPath, dbPath)

        else:
            raise Exception("Unhandled platform") 


