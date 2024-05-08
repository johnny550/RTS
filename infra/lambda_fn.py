import datetime
import base64
from collections import defaultdict 
# from pytz import timezone
# import pytz


import json

COUNTY_FIELD = "County"
TS_FIELD = "Timestamp"


def lambda_handler(event, context):
    # Assuming the Kinesis records are in the 'Records' field of the event 
    kinesis_records = event['Records'] 
    # Use defaultdict to aggregate records based on a specific field 
    aggregated_data = defaultdict(list) 
    final_aggregated_data = {} 
    # Process each Kinesis record 
    for kinesis_record in kinesis_records: 
    # Assuming the record is a string
        payload = kinesis_record['kinesis']['data']
        arrival_ts = kinesis_record['kinesis']['approximateArrivalTimestamp']
        print(arrival_ts)
        jst = datetime.timezone(datetime.timedelta(hours=9))
        # jst = timezone('Asia/Tokyo')
        datetime_obj=datetime.datetime.utcfromtimestamp(arrival_ts)
        # jst_date_time = datetime_obj.replace(tzinfo=pytz.utc).astimezone(jst)
        jst_date_time = datetime_obj.replace(tzinfo=datetime.timezone.utc).astimezone(jst)
        print(f"UTC: {datetime_obj}")
        print(f"JST: {jst_date_time}")
        
        data_str = base64.b64decode(payload).decode("ascii").splitlines()
        # JSON data was sent by the producer (Go)
        json_data = json.loads(data_str[0])
        # if COUNTY_INDEX < len(data_list):
        #     aggregation_key = data_list[COUNTY_INDEX].strip()
        #     aggregated_data[aggregation_key].append(data_list)
        if json_data[COUNTY_FIELD]:
            aggregation_key = json_data[COUNTY_FIELD]
            aggregated_data[aggregation_key].append(json_data)
        
        five_minutes_ago = get_five_minutes_ts()
        
        for key, records in aggregated_data.items(): 
            # if the timestamp of the record is 5 mins old or less, it is a valid record
            recent_records = [kinesis_record for record in records if parse_timestamp(record[TS_FIELD]) >= five_minutes_ago] 
            final_aggregated_data[key] = recent_records
            # Process the final aggregated data as needed 
    process_aggregated_data(final_aggregated_data)
                
def parse_timestamp(timestamp_str):
    return datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S') 

def get_five_minutes_ts():
    # Check the timestamp of the records and filter for the last 5 minutes 
    # current_time_str = datetime.datetime.now(timezone('Asia/Tokyo')).strftime("%Y-%m-%d %H:%M:%S")
    current_time_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
    current_time = parse_timestamp(current_time_str)
    return current_time - datetime.timedelta(minutes=3000)

def process_aggregated_data(aggregated_data):
    for key, records in aggregated_data.items():
        print(f"Aggregation Key: {key}, Records: {len(records)}")
        if not records:
            continue
        print(not records)
    
    # Send data to S3
    # client = boto3.client('s3')
    # converted_data = json.dumps(aggregated_data, indent=2).encode('utf-8')
    # client.put_object(Body=converted_data, Bucket=os.environ['BUCKET_NAME'])
    # s3 = boto3.resource('s3')
    # s3.Object(os.environ['BUCKET_NAME'], 'aggregated-'+uuid.uuid4).put(Body=open('/tmp/hello.txt', 'rb'))


    # " future: dynamoDB. table columns: id(num), aggr_key(county), num_records, records, ts"
    # print(f"Aggregation Key: {key}, Records: {records}")
    # characters = string.ascii_lowercase + string.digits
    # result_str = ''.join(random.choice(characters) for i in range(10))
    # table.put_item(
    #     Item={
    #         'county': key,
    #         'saved_at': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    #         'num_records': str(len(records)),
    #         'records': records,
    #     }
    # )




# x =  {"Records": [{
#     "kinesis": {
#         "kinesisSchemaVersion": "1.0",
#         "partitionKey": "11pk",
#         "sequenceNumber": "49647372303744768786985359633799006011429313968808656898",
#         "data": "Q2F5dXRhIENyZWVrLEJyb3duIFRyb3V0KiAtIFN1bmZpc2gsTm9uZSxodHRwOi8vd3d3LmRlYy5ueS5nb3Yvb3V0ZG9vci8zMTQzNS5odG1sLENoZW11bmcsU2hvcmUgRmlzaGluZyAtIFB1YmxpYyBGaXNoaW5nIFJpZ2h0cyxQdWJsaWMgRWFzZW1lbnQsaHR0cDovL3d3dy5kZWMubnkuZ292L2RvY3MvZmlzaF9tYXJpbmVfcGRmL3BmcmNheXV0YWNrLnBkZiwtNzYuNjExOTYzNTY0LDQyLjIyNzMzNzYyOSwoNDIuMjI3MzM3NjI5LCAtNzYuNjExOTYzNTY0KSwyMDIzLTEyLTE1IDEyOjM5OjIzLjg1NzI1MTggKzA5MDAgSlNUIG09KzEuNDY1NDYxOTAx",
#         "approximateArrivalTimestamp": 1702619705.269
#     },
#     "eventSource": "aws:kinesis",
#     "eventVersion": "1.0",
#     "eventID": "shardId-000000000000:49647372303744768786985359633799006011429313968808656898",
#     "eventName": "aws:kinesis:record",
#     "invokeIdentityArn": "arn:aws:iam::238706903261:role/service-role/rts-stream-consumer-role-skelflps",
#     "awsRegion": "ap-northeast-1",
#     "eventSourceARN": "arn:aws:kinesis:ap-northeast-1:238706903261:stream/rts-test"
#     }]}
# 1 record
x = '{"Records":[{"kinesis":{"kinesisSchemaVersion":"1.0","partitionKey":"11pk","sequenceNumber":"49647372303744768786985359633799006011429313968808656898","data":"eyJXYXRlcmJvZHkiOiJDYXl1dGEgQ3JlZWsiLCJGaXNoU3BlY2llcyI6IkJyb3duIFRyb3V0KiAtIFN1bmZpc2giLCJDb21tZW50cyI6Ik5vbmUiLCJXYXRlckJvZHlSZWciOiJodHRwOi8vd3d3LmRlYy5ueS5nb3Yvb3V0ZG9vci8zMTQzNS5odG1sIiwiQ291bnR5IjoiQ2hlbXVuZyIsIkFjY2VzcyI6IlNob3JlIEZpc2hpbmcgLSBQdWJsaWMgRmlzaGluZyBSaWdodHMiLCJPd25lciI6IlB1YmxpYyBFYXNlbWVudCIsIldhdGVyYm9keUluZm8iOiJodHRwOi8vd3d3LmRlYy5ueS5nb3YvZG9jcy9maXNoX21hcmluZV9wZGYvcGZyY2F5dXRhY2sucGRmIiwiTG9uZ2l0dWRlIjoiLTc2LjYxMTk2MzU2NCIsIkxhdGl0dWRlIjoiNDIuMjI3MzM3NjI5IiwiTG9jYXRpb24iOiIoNDIuMjI3MzM3NjI5LCAtNzYuNjExOTYzNTY0KSIsIlRpbWVzdGFtcCI6IjIwMjMtMTItMTYgMTc6MTU6NDAifQ==","approximateArrivalTimestamp":1702619705.269},"eventSource":"aws:kinesis","eventVersion":"1.0","eventID":"shardId-000000000000:49647372303744768786985359633799006011429313968808656898","eventName":"aws:kinesis:record","invokeIdentityArn":"arn:aws:iam::238706903261:role/service-role/rts-stream-consumer-role-skelflps","awsRegion":"ap-northeast-1","eventSourceARN":"arn:aws:kinesis:ap-northeast-1:238706903261:stream/rts-test"},{"kinesis":{"kinesisSchemaVersion":"1.0","partitionKey":"11pk","sequenceNumber":"49647372303744768786985359633799006011429313968808656898","data":"eyJXYXRlcmJvZHkiOiJXZXN0IEJyYW5jaCBPd2VnbyBDcmVlayIsIkZpc2hTcGVjaWVzIjoiQnJvb2sgVHJvdXQgLSBCcm93biBUcm91dCoiLCJDb21tZW50cyI6Ik5vbmUiLCJXYXRlckJvZHlSZWciOiJodHRwOi8vd3d3LmRlYy5ueS5nb3Yvb3V0ZG9vci8zMTQ5OC5odG1sIiwiQ291bnR5IjoiVG9tcGtpbnMiLCJBY2Nlc3MiOiJTaG9yZSBGaXNoaW5nIC0gUHVibGljIEZpc2hpbmcgUmlnaHRzIiwiT3duZXIiOiJQdWJsaWMgRWFzZW1lbnQgLSBQb3RhdG8gSGlsbCBTdGF0ZSBGb3Jlc3QgKERFQykiLCJXYXRlcmJvZHlJbmZvIjoiaHR0cDovL3d3dy5kZWMubnkuZ292L291dGRvb3IvNzE4MDIuaHRtbCIsIkxvbmdpdHVkZSI6Ii03Ni4yNDQxODMwNzQiLCJMYXRpdHVkZSI6IjQyLjM0MjM2NDkyOCIsIkxvY2F0aW9uIjoiKDQyLjM0MjM2NDkyOCwgLTc2LjI0NDE4MzA3NCkiLCJUaW1lc3RhbXAiOiIyMDIzLTEyLTE2IDE3OjE1OjQwIn0=","approximateArrivalTimestamp":1702619705.269},"eventSource":"aws:kinesis","eventVersion":"1.0","eventID":"shardId-000000000000:49647372303744768786985359633799006011429313968808656898","eventName":"aws:kinesis:record","invokeIdentityArn":"arn:aws:iam::238706903261:role/service-role/rts-stream-consumer-role-skelflps","awsRegion":"ap-northeast-1","eventSourceARN":"arn:aws:kinesis:ap-northeast-1:238706903261:stream/rts-test"},{"kinesis":{"kinesisSchemaVersion":"1.0","partitionKey":"11pk","sequenceNumber":"49647372303744768786985359633799006011429313968808656898","data":"eyJXYXRlcmJvZHkiOiJXZXN0IEJyYW5jaCBPd2VnbyBDcmVlayIsIkZpc2hTcGVjaWVzIjoiQnJvb2sgVHJvdXQgLSBCcm93biBUcm91dCoiLCJDb21tZW50cyI6Ik5vbmUiLCJXYXRlckJvZHlSZWciOiJodHRwOi8vd3d3LmRlYy5ueS5nb3Yvb3V0ZG9vci8zMTQ5OC5odG1sIiwiQ291bnR5IjoiVG9tcGtpbnMiLCJBY2Nlc3MiOiJTaG9yZSBGaXNoaW5nIC0gUHVibGljIEZpc2hpbmcgUmlnaHRzIiwiT3duZXIiOiJQdWJsaWMgRWFzZW1lbnQgLSBQb3RhdG8gSGlsbCBTdGF0ZSBGb3Jlc3QgKERFQykiLCJXYXRlcmJvZHlJbmZvIjoiaHR0cDovL3d3dy5kZWMubnkuZ292L291dGRvb3IvNzE4MDIuaHRtbCIsIkxvbmdpdHVkZSI6Ii03Ni4yNDQxODMwNzQiLCJMYXRpdHVkZSI6IjQyLjM0MjM2NDkyOCIsIkxvY2F0aW9uIjoiKDQyLjM0MjM2NDkyOCwgLTc2LjI0NDE4MzA3NCkiLCJUaW1lc3RhbXAiOiIyMDIzLTEyLTE2IDE3OjE1OjQwIn0=","approximateArrivalTimestamp":1702619705.269},"eventSource":"aws:kinesis","eventVersion":"1.0","eventID":"shardId-000000000000:49647372303744768786985359633799006011429313968808656898","eventName":"aws:kinesis:record","invokeIdentityArn":"arn:aws:iam::238706903261:role/service-role/rts-stream-consumer-role-skelflps","awsRegion":"ap-northeast-1","eventSourceARN":"arn:aws:kinesis:ap-northeast-1:238706903261:stream/rts-test"}]}'

# more records
# x = '{"Records":[{"kinesis":{"kinesisSchemaVersion":"1.0","partitionKey":"11pk","sequenceNumber":"49647372303744768786985359633799006011429313968808656898","data":"V2VzdCBCcmFuY2ggT3dlZ28gQ3JlZWssQnJvb2sgVHJvdXQgLSBCcm93biBUcm91dCosTm9uZSxodHRwOi8vd3d3LmRlYy5ueS5nb3Yvb3V0ZG9vci8zMTQ5OC5odG1sLFRvbXBraW5zLFNob3JlIEZpc2hpbmcgLSBQdWJsaWMgRmlzaGluZyBSaWdodHMsUHVibGljIEVhc2VtZW50IC0gUG90YXRvIEhpbGwgU3RhdGUgRm9yZXN0IChERUMpLGh0dHA6Ly93d3cuZGVjLm55Lmdvdi9vdXRkb29yLzcxODAyLmh0bWwsLTc2LjI0NDE4MzA3NCw0Mi4zNDIzNjQ5MjgsIig0Mi4zNDIzNjQ5MjgsIC03Ni4yNDQxODMwNzQpIiwyMDIzLTEyLTE1IDE5OjA4OjU1Cg==","approximateArrivalTimestamp":1702619705.269},"eventSource":"aws:kinesis","eventVersion":"1.0","eventID":"shardId-000000000000:49647372303744768786985359633799006011429313968808656898","eventName":"aws:kinesis:record","invokeIdentityArn":"arn:aws:iam::238706903261:role/service-role/rts-stream-consumer-role-skelflps","awsRegion":"ap-northeast-1","eventSourceARN":"arn:aws:kinesis:ap-northeast-1:238706903261:stream/rts-test"},{"kinesis":{"kinesisSchemaVersion":"1.0","partitionKey":"11pk","sequenceNumber":"49647372303744768786985359633799006011429313968808656898","data":"V2VzdCBCcmFuY2ggT3dlZ28gQ3JlZWssQnJvb2sgVHJvdXQgLSBCcm93biBUcm91dCosTm9uZSxodHRwOi8vd3d3LmRlYy5ueS5nb3Yvb3V0ZG9vci8zMTQ5OC5odG1sLFRvbXBraW5zLFNob3JlIEZpc2hpbmcgLSBQdWJsaWMgRmlzaGluZyBSaWdodHMsUHVibGljIEVhc2VtZW50IC0gUG90YXRvIEhpbGwgU3RhdGUgRm9yZXN0IChERUMpLGh0dHA6Ly93d3cuZGVjLm55Lmdvdi9vdXRkb29yLzcxODAyLmh0bWwsLTc2LjI0NDE4MzA3NCw0Mi4zNDIzNjQ5MjgsIig0Mi4zNDIzNjQ5MjgsIC03Ni4yNDQxODMwNzQpIiwyMDIzLTEyLTE1IDE5OjA4OjU1Cg==","approximateArrivalTimestamp":1702619705.269},"eventSource":"aws:kinesis","eventVersion":"1.0","eventID":"shardId-000000000000:49647372303744768786985359633799006011429313968808656898","eventName":"aws:kinesis:record","invokeIdentityArn":"arn:aws:iam::238706903261:role/service-role/rts-stream-consumer-role-skelflps","awsRegion":"ap-northeast-1","eventSourceARN":"arn:aws:kinesis:ap-northeast-1:238706903261:stream/rts-test"},{"kinesis":{"kinesisSchemaVersion":"1.0","partitionKey":"11pk","sequenceNumber":"49647372303744768786985359633799006011429313968808656898","data":"U3ByaW5nd2F0ZXIgQ3JlZWssQnJvd24gVHJvdXQgLSBSYWluYm93IFRyb3V0LENpdHkgb2YgUm9jaGVzdGVyIHBlcm1pdCByZXF1aXJlZCBub3J0aCBvZiBLZWxsb2dnIFJkICg1ODUtNDI4LTY2ODApLixodHRwOi8vd3d3LmRlYy5ueS5nb3Yvb3V0ZG9vci8zMTUwNS5odG1sLExpdmluZ3N0b24sVG9ta2lucyxTcHJpbmd3YXRlciBUb3duIFBhcmsgLSBQdWJsaWMgRWFzZW1lbnQsaHR0cDovL3d3dy5kZWMubnkuZ292L2RvY3MvZmlzaF9tYXJpbmVfcGRmL3BmcnNwcm5nd3Rjay5wZGYsLTc3LjYwMjM0MTQ2Miw0Mi42MzQ2NTE5MTIsIig0Mi42MzQ2NTE5MTIsIC03Ny42MDIzNDE0NjIpIiwyMDIzLTEyLTE1IDE5OjA4OjU1Cg==","approximateArrivalTimestamp":1702619705.269},"eventSource":"aws:kinesis","eventVersion":"1.0","eventID":"shardId-000000000000:49647372303744768786985359633799006011429313968808656898","eventName":"aws:kinesis:record","invokeIdentityArn":"arn:aws:iam::238706903261:role/service-role/rts-stream-consumer-role-skelflps","awsRegion":"ap-northeast-1","eventSourceARN":"arn:aws:kinesis:ap-northeast-1:238706903261:stream/rts-test"}]}'

y = json.loads(x)
lambda_handler(y,"")