variable "aws_region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "S3 bucket name"
  default     = "peruvianbucket"
}

variable "project_name" {
  description = "Project name"
  default     = "nyc-tlc-pipeline"
}
