import datetime
import base64
import json
import boto3
import os
from collections import defaultdict


COUNTY_FIELD = "County"
TS_FIELD = "Timestamp"


def lambda_handler(event, context): 
    kinesis_records = event['Records'] 
    aggregated_data = defaultdict(list) 
    final_aggregated_data = {} 
    # DynamoDB prep
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])

    for record in kinesis_records: 
        payload = record['kinesis']['data']
        data_str = base64.b64decode(payload).decode("ascii").splitlines()
        # JSON data was sent by the producer (Go)
        json_data = json.loads(data_str[0])
        if json_data[COUNTY_FIELD]:
            aggregation_key = json_data[COUNTY_FIELD]
            aggregated_data[aggregation_key].append(json_data)

        jst = datetime.timezone(datetime.timedelta(hours=9))
        five_minutes_ago = get_five_minutes_ts(jst)
        # Check the timestamp of the records and filter for the last 5 minutes 
        for key, records in aggregated_data.items(): 
            # if the timestamp of the record is 5 mins old or less, it is a valid record
            recent_records = [record for record in records if parse_timestamp(record[TS_FIELD]) >= five_minutes_ago] 
            final_aggregated_data[key] = recent_records
    process_aggregated_data(final_aggregated_data, table, jst)
                
def parse_timestamp(timestamp_str):
    return datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S') 

def get_five_minutes_ts(jstTz):
    # Check the timestamp of the records and filter for the last 5 minutes 
    current_time_str = datetime.datetime.now(jstTz).strftime("%Y-%m-%d %H:%M:%S")
    current_time = parse_timestamp(current_time_str)
    return current_time - datetime.timedelta(minutes=5)
 
def process_aggregated_data(aggregated_data, table, jstTz):  
    for key, records in aggregated_data.items():
        # if no records, do not bother sending data to dynamodb
        if not records:
            continue
        print(f"Aggregation Key: {key}, Records: {records}")
        table.put_item(
            Item={
                'county': key,
                'saved_at': datetime.datetime.now(tz=jstTz).strftime("%Y-%m-%d %H:%M:%S"),
                'num_records': str(len(records)),
                'records': records,
            }
        )