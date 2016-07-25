fname = "C:/dev/GitHub/getmovement/national-voter-file/data/Washington/201605_VRDB_ExtractSAMPLE.txt"

import csv
import usaddress

def enrichVRDB(fname):
    with open(fname) as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            parse = usaddress.tag(createAddress(row))
            
        
        

def createAddress(row):
    addr = " ".join([row['RegStNum'],row['RegStFrac'],row['RegStPreDirection'], row['RegStName'], row['RegStType'], row['RegStPostDirection']])
    unit = row['RegUnitType']+" "+row['RegUnitNum']
    city = row['RegCity']+", "+row['RegState']+" "+row['RegZipCode'] 
    return addr+","+unit+","+city                                                          
