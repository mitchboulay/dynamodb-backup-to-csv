import json
import csv
import os

import boto3
def get_client(name):
    return boto3.client(name)

def dynamodb_backup(table):
    service = "dynamodb"
    db_client = get_client(service)

    backup_file_name = 'backup_tables/'+table+'_backup_file.csv'

    data_file = open(backup_file_name, 'w')
    csv_writer = csv.writer(data_file)
    
    write_file='data.json'
    with open(write_file, 'w') as json_file:
        json.dump(db_client.scan(TableName = table), json_file)
    
    with open(write_file) as json_file:
        data = json.load(json_file)
        data = data['Items']
    count = 0
    for i in data:
        if count == 0:
 
        # Writing headers of CSV file
            header = i.keys()
            csv_writer.writerow(header)
            count += 1
        csv_writer.writerow(i.values())
    
if __name__ == '__main__':
    tables = ['company','company_site','group','area']
    for table in tables:
        print("Backing up --",table)
        dynamodb_backup(table)