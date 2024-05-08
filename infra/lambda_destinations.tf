# Not a lamda destinations per se, but definitely a destination for this lamda :)
resource "aws_dynamodb_table" "rts-aggregated" {
  name           = "aggregated-data-holder-xx6575"
  billing_mode   = "PROVISIONED"
  read_capacity  = 1
  write_capacity = 2
  hash_key       = "county"
  range_key      = "saved_at"

  attribute {
    name = "county"
    type = "S"
  }

  attribute {
    name = "saved_at"
    type = "S"
  }
}

resource "aws_sqs_queue" "rts-consumer-dlq" {
  name                      = "rts-consumer-dlq"
  delay_seconds             = 0
  receive_wait_time_seconds = 10
  sqs_managed_sse_enabled = true
}