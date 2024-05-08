data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_policy" "policy_for_dynamodb" {
  name = "dynamodb-lambda-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
       {
        Action = [
          "dynamodb:PutItem",
          "dynamodb:DescribeTable",
          "dynamodb:ListTables",
        ]
        Effect = "Allow"
        Resource = "${aws_dynamodb_table.rts-aggregated.arn}"
      },
    ]
  })
}

resource "aws_iam_policy" "policy_for_sqs" {
  name = "sqs-lambda-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
       {
        Action = [
          "sqs:GetQueueUrl",
          "sqs:SendMessage",
          "sqs:ListTables",
        ]
        Effect = "Allow"
        Resource = "${aws_sqs_queue.rts-consumer-dlq.arn}"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_dynamodb_perm" {
  role       = aws_iam_role.function_role.name
  policy_arn = aws_iam_policy.policy_for_dynamodb.arn
}
resource "aws_iam_role_policy_attachment" "attach_sqs_perm" {
  role       = aws_iam_role.function_role.name
  policy_arn = aws_iam_policy.policy_for_sqs.arn
}

resource "aws_iam_role" "function_role" {
  name = "rts-${local.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  managed_policy_arns = var.MANAGED_POLICIES_FOR_LAMBDA
}