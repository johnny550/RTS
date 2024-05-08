# About
This app reads data from the given data.io dataset (https://data.world/data-ny-gov/jcxg-7gnm/workspace/file?filename=recommended-fishing-rivers-and-streams-1.csv)
In order to get the data and have it in local, it leverages the data.world API as well as the dwapi-go library. Using the latter's provided functions, the data is 
easily downloaded and saved as a CSV file.

Once the data is acquired, it is time to prepare it before sending it to AWS Kinesis (Data stream).
## Step 1: Preparing the data (can be improved)
A Timestamp column is added to the data, as well as current time info for each row.
This is done because the data being sent to AWS Kinesis should have the timestamp info, because an aggregation is done on the latter field.
Such step could have been avoided if the aggregation was to be done on the ApproximateTimestamp of each record. That ApproximateTimestamp is added automatically when a record is received by Kinesis.

## Step 2: Sending the data
Using version 2 of AWS SDK, the records are infinitely sent to the AWS Kinesis stream.
The records are concurrently streamed and no order or lack of duplication is garanteed. 

# The architecture on AWS
Sending records to the data stream triggers a lambda function hooked on it. That lambda function is in charge of aggregating the data based off the given timestamp (not the AWS-provided one called ApproximateArrivalTimestamp). Once the data is aggregated, it is then saved in a dynamo DB table with the following fields: 
- county (partition key)
- saved_at (sort key. Also the timestamp when the info was saved in the table)
- num_records (the number of records aggregated by County in a 5 minutes range)
- records (Records aggregated by county, in a 5 minutes range)

The lambda function, of course can be monitored using Cloudwatch, but no additional feature was added there. When an invocation of the function ends in an error,
data is not saved in the Dynamo DB table. Instead, an event is sent to a SQS queue, set as a dead letter queue for the lambda function. With those events, actions such as debugging, monitoring can be taken at an ulterior time.

# Commands
## Deploy the infrastructure
AWS access and secret access keys can be set in ./infra/vars.tf
```./scripts/deploy.sh```

## Build app
```./scripts/build.sh```

## Test app
Need to set a data.world authentication token as an environment variable. Otherwise, downloading the data is impossible. (#401: Unauthorized)
```export AUTHTOKEN=```
```./scripts/unit_test.sh```

## Run app
Need to set a data.world authentication token as an environment variable. Otherwise, downloading the data is impossible. (#401: Unauthorized)
```export AUTHTOKEN=```
```sh ./scripts/stream.sh```

