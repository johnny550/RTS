variable "AWS_ACCESS_KEY" {
    default = ""
}

variable "AWS_SECRET_KEY" {
    default = ""
}

variable "AWS_REGION" {
  default = "ap-northeast-1"
}

variable MANAGED_POLICIES_FOR_LAMBDA {
  default = [ "arn:aws:iam::238706903261:policy/service-role/AWSLambdaBasicExecutionRole-a9a87ddc-114e-4cde-99ff-f0fdbb711062", "arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole" ]
}
