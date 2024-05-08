locals {
  function_name               = "consumer"
  function_handler            = "rts_consumer.lambda_handler"
  function_runtime            = "python3.9"

  function_source_dir = "${path.module}/${local.function_name}"
}

resource "aws_lambda_function" "function" {
  function_name = "${local.function_name}"
  handler       = local.function_handler
  runtime       = local.function_runtime

  filename         = "${local.function_source_dir}.zip"
  source_code_hash = data.archive_file.function_zip.output_base64sha256

  role = aws_iam_role.function_role.arn

  environment {
    variables = {
      TABLE_NAME = aws_dynamodb_table.rts-aggregated.name
    }
  }
}

data "archive_file" "function_zip" {
  source_dir  = local.function_source_dir
  
  type        = "zip"
  output_path = "${local.function_source_dir}.zip"
}

resource "aws_lambda_event_source_mapping" "example" {
  event_source_arn  = aws_kinesis_stream.rts_stream.arn
  function_name     = "${local.function_name}"
  starting_position = "LATEST"
  maximum_retry_attempts = 0
  # DLQ settings
  destination_config {
      on_failure {
          destination_arn = aws_sqs_queue.rts-consumer-dlq.arn
      }
    }

  depends_on = [ aws_lambda_function.function, aws_sqs_queue.rts-consumer-dlq ]
}